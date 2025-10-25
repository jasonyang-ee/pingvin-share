import {
  BadRequestException,
  Injectable,
  InternalServerErrorException,
  NotFoundException,
  Logger,
} from "@nestjs/common";
import {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadObjectCommand,
  HeadBucketCommand,
  ListObjectsV2Command,
  S3Client,
  UploadPartCommand,
  UploadPartCommandOutput,
} from "@aws-sdk/client-s3";
import { PrismaService } from "src/prisma/prisma.service";
import { ConfigService } from "src/config/config.service";
import * as crypto from "crypto";
import * as mime from "mime-types";
import { File } from "./file.service";
import { Readable } from "stream";
import { validate as isValidUUID } from "uuid";
import * as archiver from "archiver";
import { NodeHttpHandler } from "@aws-sdk/node-http-handler";
import { Agent as HttpAgent } from "http";
import { Agent as HttpsAgent } from "https";
import { StandardRetryStrategy } from "@aws-sdk/middleware-retry";

@Injectable()
export class S3FileService {
  private readonly logger = new Logger(S3FileService.name);

  private multipartUploads: Record<
    string,
    {
      uploadId: string;
      parts: Array<{ ETag: string | undefined; PartNumber: number }>;
    }
  > = {};

  constructor(
    private prisma: PrismaService,
    private config: ConfigService,
  ) {}

  async create(
    data: string,
    chunk: { index: number; total: number },
    file: { id?: string; name: string },
    shareId: string,
  ) {
    this.logger.log(`create() called for shareId=${shareId} file=${file.name} chunk=${chunk.index+1}/${chunk.total}`);

    if (!file.id) {
      file.id = crypto.randomUUID();
    } else if (!isValidUUID(file.id)) {
      throw new BadRequestException("Invalid file ID format");
    }

    const buffer = Buffer.from(data, "base64");
    const key = `${this.getS3Path()}${shareId}/${file.name}`;
    const bucketName = this.config.get("s3.bucketName");
    const s3Instance = this.getS3Instance();
    // Quick connectivity test: attempt to list up to 1 object in the bucket
    this.logger.log(`Testing S3 bucket connectivity via ListObjectsV2: ${bucketName}`);
    try {
      const listTest = await s3Instance.send(
        new ListObjectsV2Command({ Bucket: bucketName, MaxKeys: 1 }),
      );
      const count = listTest.Contents?.length ?? 0;
      this.logger.log(`Connectivity test succeeded: found ${count} object(s)`);
    } catch (err) {
      this.logger.error(`Bucket list test failed for ${bucketName}: ${err}`);
      throw new InternalServerErrorException(
        `Cannot access or list from S3 bucket ${bucketName}`,
      );
    }

    this.logger.log(
      `Starting upload: fileId=${file.id} shareId=${shareId} chunk=${chunk.index + 1}/${chunk.total}`,
    );
    try {
      // Initialize multipart upload if it's the first chunk
      if (chunk.index === 0) {
        this.logger.log(`Initializing multipart upload: Bucket=${bucketName} Key=${key}`);
        const multipartInitResponse = await s3Instance.send(
          new CreateMultipartUploadCommand({
            Bucket: bucketName,
            Key: key,
          }),
        );
        this.logger.log(`Initialized multipart upload: uploadId=${multipartInitResponse.UploadId}`);

        const uploadId = multipartInitResponse.UploadId;
        if (!uploadId) {
          throw new Error("Failed to initialize multipart upload.");
        }

        // Store the uploadId and parts list in memory
        this.multipartUploads[file.id] = {
          uploadId,
          parts: [],
        };
      }

      // Get the ongoing multipart upload
      const multipartUpload = this.multipartUploads[file.id];
      if (!multipartUpload) {
        throw new InternalServerErrorException(
          "Multipart upload session not found.",
        );
      }

      const uploadId = multipartUpload.uploadId;

      // Upload the current chunk
      this.logger.log(`Uploading part: PartNumber=${chunk.index + 1} length=${buffer.length}`);
      const uploadPartResponse: UploadPartCommandOutput = await s3Instance.send(
        new UploadPartCommand({
          Bucket: bucketName,
          Key: key,
          PartNumber: chunk.index + 1,
          UploadId: uploadId,
          Body: buffer,
        }),
      );
      this.logger.log(`Uploaded part: ETag=${uploadPartResponse.ETag} PartNumber=${chunk.index + 1}`);

      // Store the ETag and PartNumber for later completion
      multipartUpload.parts.push({
        ETag: uploadPartResponse.ETag,
        PartNumber: chunk.index + 1,
      });

      // Complete the multipart upload if it's the last chunk
      if (chunk.index === chunk.total - 1) {
        this.logger.log(
          `Completing multipart upload: uploadId=${uploadId} parts=${JSON.stringify(multipartUpload.parts)}`,
        );
        await s3Instance.send(
          new CompleteMultipartUploadCommand({
            Bucket: bucketName,
            Key: key,
            UploadId: uploadId,
            MultipartUpload: {
              Parts: multipartUpload.parts,
            },
          }),
        );

        this.logger.log(`create(): multipart upload complete for fileId=${file.id}`);

        // Remove the completed upload from memory
        delete this.multipartUploads[file.id];
      }
    } catch (error) {
      this.logger.error(`Error in multipart upload: ${error.message}`, error.stack);
      // Abort the multipart upload if it fails
      const multipartUpload = this.multipartUploads[file.id];
      if (multipartUpload) {
        this.logger.log(`Aborting multipart upload: uploadId=${multipartUpload.uploadId}`);
        try {
          await s3Instance.send(
            new AbortMultipartUploadCommand({
              Bucket: bucketName,
              Key: key,
              UploadId: multipartUpload.uploadId,
            }),
          );
          this.logger.log(`Aborted multipart upload: uploadId=${multipartUpload.uploadId}`);
        } catch (abortError) {
          console.error("Error aborting multipart upload:", abortError);
        }
        delete this.multipartUploads[file.id];
      }
      this.logger.error(error);
      throw new Error("Multipart upload failed. The upload has been aborted.");
    }

    const isLastChunk = chunk.index == chunk.total - 1;
    if (isLastChunk) {
      const fileSize: number = await this.getFileSize(shareId, file.name);

      await this.prisma.file.create({
        data: {
          id: file.id,
          name: file.name,
          size: fileSize.toString(),
          share: { connect: { id: shareId } },
        },
      });
    }

    return file;
  }

  async get(shareId: string, fileId: string): Promise<File> {
    this.logger.log(`get() called for shareId=${shareId} fileId=${fileId}`);

    const fileName = (
      await this.prisma.file.findUnique({ where: { id: fileId } })
    ).name;

    const s3Instance = this.getS3Instance();
    const key = `${this.getS3Path()}${shareId}/${fileName}`;
    this.logger.log(`get(): sending GetObjectCommand for key=${key}`);
    const response = await s3Instance.send(
      new GetObjectCommand({
        Bucket: this.config.get("s3.bucketName"),
        Key: key,
      }),
    );

    this.logger.log(`get(): received object size=${response.ContentLength} lastModified=${response.LastModified}`);
    return {
      metaData: {
        id: fileId,
        size: response.ContentLength?.toString() || "0",
        name: fileName,
        shareId: shareId,
        createdAt: response.LastModified || new Date(),
        mimeType:
          mime.contentType(fileId.split(".").pop()) ||
          "application/octet-stream",
      },
      file: response.Body as Readable,
    } as File;
  }

  async remove(shareId: string, fileId: string) {
    this.logger.log(`remove() called for shareId=${shareId} fileId=${fileId}`);

    const fileMetaData = await this.prisma.file.findUnique({
      where: { id: fileId },
    });

    if (!fileMetaData) throw new NotFoundException("File not found");

    const key = `${this.getS3Path()}${shareId}/${fileMetaData.name}`;
    const s3Instance = this.getS3Instance();
    this.logger.log(`remove(): deleting S3 object key=${key}`);

    try {
      await s3Instance.send(
        new DeleteObjectCommand({
          Bucket: this.config.get("s3.bucketName"),
          Key: key,
        }),
      );
    } catch (error) {
      throw new Error("Could not delete file from S3");
    }

    await this.prisma.file.delete({ where: { id: fileId } });
    this.logger.log(`remove(): deleted S3 object and removed metadata for fileId=${fileId}`);
  }

  async deleteAllFiles(shareId: string) {
    this.logger.log(`deleteAllFiles() called for shareId=${shareId}`);
    const prefix = `${this.getS3Path()}${shareId}/`;
    this.logger.log(`deleteAllFiles(): listing objects under prefix=${prefix}`);
    const s3Instance = this.getS3Instance();

    try {
      // List all objects under the given prefix
      const listResponse = await s3Instance.send(
        new ListObjectsV2Command({
          Bucket: this.config.get("s3.bucketName"),
          Prefix: prefix,
        }),
      );

      if (!listResponse.Contents || listResponse.Contents.length === 0) {
        throw new Error(`No files found for share ${shareId}`);
      }

      this.logger.log(`deleteAllFiles(): found ${listResponse.Contents.length} objects`);

      // Extract the keys of the files to be deleted
      const objectsToDelete = listResponse.Contents.map((file) => ({
        Key: file.Key!,
      }));

      // Delete all files in a single request (up to 1000 objects at once)
      await s3Instance.send(
        new DeleteObjectsCommand({
          Bucket: this.config.get("s3.bucketName"),
          Delete: {
            Objects: objectsToDelete,
          },
        }),
      );
      this.logger.log(`deleteAllFiles(): deleted ${objectsToDelete.length} objects from bucket`);
    } catch (error) {
      throw new Error("Could not delete all files from S3");
    }
  }

  async getFileSize(shareId: string, fileName: string): Promise<number> {
    this.logger.log(`getFileSize() called for shareId=${shareId} fileName=${fileName}`);

    const key = `${this.getS3Path()}${shareId}/${fileName}`;
    const s3Instance = this.getS3Instance();

    try {
      // Get metadata of the file using HeadObjectCommand
      const headObjectResponse = await s3Instance.send(
        new HeadObjectCommand({
          Bucket: this.config.get("s3.bucketName"),
          Key: key,
        }),
      );

      this.logger.log(`getFileSize(): HeadObject size=${headObjectResponse.ContentLength}`);

      // Return ContentLength which is the file size in bytes
      return headObjectResponse.ContentLength ?? 0;
    } catch (error) {
      throw new Error("Could not retrieve file size");
    }
  }

  getS3Instance(): S3Client {
    // Resolve endpoint, region, and path-style usage
    const endpoint = this.config.get("s3.endpoint");
    const region = this.config.get("s3.region");
    const forcePathStyle = this.config.get("s3.forcePathStyle") === true;
    const disableHTTPS = !endpoint.startsWith("https://");
    const checksumCalculation =
      this.config.get("s3.useChecksum") === true ? null : "WHEN_REQUIRED";
    this.logger.log(
      `Creating S3 client – endpoint=${endpoint} region=${region} pathStyle=${forcePathStyle} disableHTTPS=${disableHTTPS}`,
    );
    // Set up retry strategy
    const retries = 5;
    return new S3Client({
      endpoint,
      region,
      credentials: {
        accessKeyId: this.config.get("s3.key"),
        secretAccessKey: this.config.get("s3.secret"),
      },
      forcePathStyle,
      maxAttempts: retries,
      retryStrategy: new StandardRetryStrategy(async () => retries),
      requestChecksumCalculation: checksumCalculation,
      responseChecksumValidation: checksumCalculation,
      // Use custom HTTP/HTTPS agents for keep-alive and disable HTTPS if needed
      requestHandler: new NodeHttpHandler({
        httpAgent: new HttpAgent({ keepAlive: true }),
        httpsAgent: disableHTTPS ? undefined : new HttpsAgent({ keepAlive: true }),
        connectionTimeout: 0,
        socketTimeout: 0,
      }),
    });
  }

  getZip(shareId: string) {
    this.logger.log(`getZip() called for shareId=${shareId}`);
    return new Promise<Readable>(async (resolve, reject) => {
      const s3Instance = this.getS3Instance();
      const bucketName = this.config.get("s3.bucketName");
      const compressionLevel = this.config.get("share.zipCompressionLevel");

      const prefix = `${this.getS3Path()}${shareId}/`;

      try {
        const listResponse = await s3Instance.send(
          new ListObjectsV2Command({
            Bucket: bucketName,
            Prefix: prefix,
          }),
        );

        if (!listResponse.Contents || listResponse.Contents.length === 0) {
          throw new NotFoundException(`No files found for share ${shareId}`);
        }

        this.logger.log(`getZip(): listed ${listResponse.Contents.length} items, streaming them into archive`);

        const archive = archiver("zip", {
          zlib: { level: parseInt(compressionLevel) },
        });

        archive.on("error", (err) => {
          this.logger.error("Archive error", err);
          reject(new InternalServerErrorException("Error creating ZIP file"));
        });

        const fileKeys = listResponse.Contents.filter(
          (object) => object.Key && object.Key !== prefix,
        ).map((object) => object.Key as string);

        if (fileKeys.length === 0) {
          throw new NotFoundException(
            `No valid files found for share ${shareId}`,
          );
        }

        let filesAdded = 0;

        const processNextFile = async (index: number) => {
          if (index >= fileKeys.length) {
            archive.finalize();
            return;
          }

          const key = fileKeys[index];
          const fileName = key.replace(prefix, "");

          try {
            const response = await s3Instance.send(
              new GetObjectCommand({
                Bucket: bucketName,
                Key: key,
              }),
            );

            if (response.Body instanceof Readable) {
              const fileStream = response.Body;

              fileStream.on("end", () => {
                filesAdded++;
                processNextFile(index + 1);
              });

              fileStream.on("error", (err) => {
                this.logger.error(`Error streaming file ${fileName}`, err);
                processNextFile(index + 1);
              });

              archive.append(fileStream, { name: fileName });
            } else {
              processNextFile(index + 1);
            }
          } catch (error) {
            this.logger.error(`Error processing file ${fileName}`, error);
            processNextFile(index + 1);
          }
        };

        resolve(archive);
        processNextFile(0);
      } catch (error) {
        this.logger.error("Error creating ZIP file", error);

        reject(new InternalServerErrorException("Error creating ZIP file"));
      }
    });
  }

  getS3Path(): string {
    // Normalize bucketPath: strip leading/trailing slashes, add single trailing slash
    const raw = this.config.get("s3.bucketPath") || "";
    let path = raw.trim();
    // remove leading slashes
    path = path.replace(/^\/*/, "");
    // remove trailing slashes
    path = path.replace(/\/*$/, "");
    return path ? `${path}/` : "";
  }
}
