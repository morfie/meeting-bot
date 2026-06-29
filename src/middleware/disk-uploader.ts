import { Logger } from 'winston';
import {
  createPartUploadUrl,
  fileNameTemplate,
  finalizeUpload,
  initializeMultipartUpload,
  uploadChunkToStorage
} from '../services/uploadService';
import { ContentType, extensionToContentType, FileType } from '../types';
import fs, { createWriteStream } from 'fs';
import path from 'path';
import { execFile } from 'child_process';
import { promisify } from 'util';
import { LogAggregator } from '../util/logger';
import config from '../config';
import { getStorageProvider } from '../uploader/providers/factory';
import { getTimeString } from '../lib/datetime';
import { notifyRecordingCompleted, RecordingCompletedPayload } from '../services/notificationService';
import { writeWebmDurationMetadata } from '../lib/webmDuration';

console.log(' ----- PWD OR CWD ----- ', process.cwd());

const tempFolder = path.join(process.cwd(), 'dist', '_tempvideo');
const execFileAsync = promisify(execFile);

function isNoSuchUploadError(err: any, userId: string, logger: Logger): boolean {
  /**
   * Error includes:
   * code: ERR_BAD_REQUEST
   *
   * Error response includes:
   * status: 404
   * statusText: 'Not Found'
   * data: "<?xml version='1.0' encoding='UTF-8'?><Error><Code>NoSuchUpload</Code><Message>The requested upload was not found.</Message></Error>"
   */
  const xml = err?.response?.data || err?.data || '';

  const isNoSuchUpload = typeof xml === 'string' && xml?.includes('NoSuchUpload');

  if (isNoSuchUpload) {
    const code = err?.code;
    const status = err?.response?.status;
    logger.error('Critical: NoSuchUpload error on user', { userId, status, code });
  }

  return isNoSuchUpload;
}

export interface IUploader {
  uploadRecordingToRemoteStorage(options?: { forceUpload?: boolean }): Promise<boolean>;
  saveDataToTempFile(data: Buffer): Promise<boolean>;
  setRecordingDuration(durationSeconds: number): void;
}

// Save to disk and upload in one session
// TODO Add illustrative logs to track or replay the journey
interface SegmentResult {
  success: boolean;
  key: string;
  url?: string;
  index: number;
}

class DiskUploader implements IUploader {
  private _token: string;
  private _teamId: string;
  private _timezone: string;
  private _userId: string;
  private _botId: string;
  private _namePrefix: string;
  private _tempFileId: string;
  private _originalTempFileId: string;
  private _logger: Logger;
  private _meetingLink?: string;

  private readonly UPLOAD_CHUNK_SIZE = 50 * 1024 * 1024; // 50 MiB
  private readonly SEGMENT_SIZE_THRESHOLD = 45 * 1024 * 1024; // 45 MiB — rotate segment before this

  private readonly MAX_CHUNK_UPLOAD_RETRIES = 3;
  private readonly MAX_FILE_UPLOAD_RETRIES = 3;
  private readonly RETRY_UPLOAD_DELAY_BASE_MS = 500;
  private readonly MAX_GLOBAL_FAILURES = 5;

  private folderId = 'private'; // Assume meetings belong to an individual
  private contentType: ContentType = extensionToContentType[config.uploaderFileExtension] ?? 'video/webm'; // Default video format
  private fileExtension: string = config.uploaderFileExtension;
  private fileId: string;
  private uploadId: string;
  private lastUploadedBlobUrl?: string;
  private lastRecordingId?: string;
  private lastStorageDetails?: Record<string, any>;
  private recordingDuration?: number;
  private firstChunkReceivedAt?: number;

  // Segment tracking for rolling upload
  private segmentIndex = 1;
  private currentSegmentBytes = 0;
  private isRotating = false;
  private _segmentBaseTime?: string;
  private backgroundUploads: Array<Promise<SegmentResult>> = [];
  private completedSegments: SegmentResult[] = [];

  private queue: Buffer[];
  private writing: boolean;
  private diskWriteSuccess: LogAggregator;

  private forceUpload: boolean;

  private constructor(
    token: string,
    teamId: string,
    timezone: string,
    userId: string,
    botId: string,
    namePrefix: string,
    tempFileId: string,
    logger: Logger,
    meetingLink?: string
  ) {
    this._token = token;
    this._teamId = teamId;
    this._timezone = timezone;
    this._userId = userId;
    this._botId = botId;
    this._namePrefix = namePrefix;
    this._tempFileId = tempFileId;
    this._originalTempFileId = tempFileId;
    this._logger = logger;
    this._meetingLink = meetingLink;

    this.queue = [];
    this.writing = false;
    this.diskWriteSuccess = new LogAggregator(this._logger, `Success writing temp chunk to disk ${this._userId}`);
    this.forceUpload = false;
  }

  public static async initialize(
    token: string,
    teamId: string,
    timezone: string,
    userId: string,
    botId: string,
    namePrefix: string,
    tempFileId: string,
    logger: Logger,
    meetingLink?: string
  ) {
    const folderPath = DiskUploader.getFolderPath(userId);

    await DiskUploader.setupDirectory(folderPath, userId, logger);

    const instance = new DiskUploader(
      token,
      teamId,
      timezone,
      userId,
      botId,
      namePrefix,
      tempFileId,
      logger,
      meetingLink
    );
    return instance;
  }

  private async uploadChunk(data: Buffer, partNumber: number) {
    this._logger.info('Uploader sending part...', partNumber, this._userId, this._teamId);

    const blob = new Blob([new Uint8Array(data as Buffer)], { type: 'application/octet-stream' });

    // Upload chunks to the server
    const uploadUrl = await createPartUploadUrl({
      teamId: this._teamId,
      folderId: this.folderId,
      fileId: this.fileId,
      uploadId: this.uploadId,
      partNumber: partNumber,
      contentType: this.contentType,
      token: this._token,
    });

    await uploadChunkToStorage({
      uploadUrl,
      chunk: blob,
    }, this._logger);

    this._logger.info('Uploader completed part...', partNumber, this._userId, this._teamId);
  }

  private async connect() {
    this._logger.info('Uploader connecting...', this._userId, this._teamId);
    // Initialise the file upload
    const initResponse = await initializeMultipartUpload({
      teamId: this._teamId,
      folderId: this.folderId,
      contentType: this.contentType,
      token: this._token,
    });

    this.fileId = initResponse.fileId;
    this.uploadId = initResponse.uploadId;

    this._logger.info('Uploader connected...', this._userId, this._teamId);
  }

  private async finish() {
    this._logger.info('Client finishing upload ...', this._userId, this._teamId);

    // Finalise upload
    const file: FileType = await finalizeUpload({
      teamId: this._teamId,
      folderId: this.folderId,
      fileId: this.fileId,
      uploadId: this.uploadId,
      contentType: this.contentType,
      token: this._token,
      timezone: this._timezone,
      namePrefix: this._namePrefix,
      botId: this._botId,
      duration: this.recordingDuration,
    }, this._logger);
    this._logger.info('Finish recording upload...', file.name, this._userId, this._teamId);
    try {
      // Capture URL/recordingId if available
      const fileUrl = file.url || (file.defaultProfile && file.alternativeFormats?.[file.defaultProfile]?.url) || undefined;
      this.lastUploadedBlobUrl = fileUrl;
      if (file.recordingId) this.lastRecordingId = file.recordingId;
    } catch {}
    try {
      // Capture URL/recordingId if available
      const fileUrl = file.url || (file.defaultProfile && file.alternativeFormats?.[file.defaultProfile]?.url) || undefined;
      this.lastUploadedBlobUrl = fileUrl;
      if (file.recordingId) this.lastRecordingId = file.recordingId;
      // Capture storage details for screenapp/vfs flow
      try {
        this.lastStorageDetails = {
          provider: 'screenapp',
          fileId: (file as any)?._id,
          url: fileUrl,
          defaultProfile: file.defaultProfile,
          duration: this.recordingDuration ?? file.duration,
        };
      } catch {}
    } catch {}
  }

  private writeChunkToDisk(chunk: Buffer): Promise<void> {
    const filePath = DiskUploader.getFilePath(this._userId, this._tempFileId, this.fileExtension);

    return new Promise((resolve, reject) => {
      const stream = createWriteStream(filePath, {
        flags: 'a',
        highWaterMark: 2 * 1024 * 1024,
      });
      const canWrite = stream.write(chunk);
      if (!canWrite) {
        stream.once('drain', () => {
          stream.end(() => resolve());
        });
      } else {
        stream.end(() => resolve());
      }
      stream.on('error', reject);
    });
  }

  private consecutiveWriteFailures = 0;

  private async writeWithRetries() {
    if (this.writing) return;

    this.writing = true;

    while (this.queue.length > 0) {
      const chunk = this.queue.shift();
      let success = false;
      let attempt = 0;
      const maxRetries = 3;
      const delayMs = 250;

      if (chunk) {
        while (!success && attempt <= maxRetries) {
          try {
            await this.writeChunkToDisk(chunk);
            success = true;
            this.consecutiveWriteFailures = 0; // reset on success
          } catch (err) {
            attempt++;
            if (attempt > maxRetries) {
              this.consecutiveWriteFailures++;
              this.queue.unshift(chunk); // put chunk back at front

              if (this.consecutiveWriteFailures >= this.MAX_GLOBAL_FAILURES) {
                this._logger.error(`Abandoning write after ${this.consecutiveWriteFailures} global failures`, this._userId, err);
                this.writing = false;
                return; // give up entirely
              }
              this._logger.info('Temporarily exit disk writing on error', this._userId, err);
              break; // exit inner retry loop, but keep outer loop running
            }
            this._logger.error(`Attempt to re-write chunk at attempt ${attempt}:`, this._userId, err);
            await new Promise((resolve) => setTimeout(resolve, delayMs * attempt));
          }
        }
      }
    }

    this.writing = false;
  }

  private enqueue(chunk: Buffer) {
    this.queue.push(chunk);

    if (!this.writing) {
      // Non blocking queue
      this.writeWithRetries()
        .then(() => {
          this.diskWriteSuccess.log();
        })
        .catch((err) => {
          this._logger.info('Failure during queue processing to write to disk', this._userId);
          throw err;
        });
    }
  }

  public async saveDataToTempFile(data: Buffer) {
    try {
      if (this.forceUpload) {
        // Stop disk writes when the upload or data recovery has started!
        this._logger.info('Force upload is enabled. Stopping disk writes...', this._userId, this._teamId);
        return false;
      }
      if (!this.firstChunkReceivedAt) {
        this.firstChunkReceivedAt = Date.now();
      }
      this.enqueue(data);
      this.currentSegmentBytes += data.byteLength;

      // Trigger segment rotation when threshold is reached (only for s3 uploader, not screenapp)
      if (
        config.uploaderType === 's3' &&
        !this.isRotating &&
        this.currentSegmentBytes >= this.SEGMENT_SIZE_THRESHOLD
      ) {
        this.isRotating = true;
        this.rotateSegment().catch((err) => {
          this._logger.error('Segment rotation failed', { userId: this._userId, error: err });
          this.isRotating = false;
        });
      }

      return true;
    } catch(err) {
      this._logger.info('Error: Unable to save the chunk to disk...', this._userId, this._teamId, err);
      return false;
    }
  }

  private getSegmentBaseTime(): string {
    if (!this._segmentBaseTime) {
      this._segmentBaseTime = getTimeString(this._timezone, this._logger);
    }
    return this._segmentBaseTime;
  }

  private buildSegmentKey(index: number): string {
    const time = this.getSegmentBaseTime();
    const fileName = fileNameTemplate(this._namePrefix, time);
    return `meeting-bot/${this._userId}/${fileName}_part${index}${this.fileExtension}`;
  }

  private buildFinalKey(): string {
    const time = this.getSegmentBaseTime();
    const fileName = fileNameTemplate(this._namePrefix, time);
    // No suffix for single-segment recordings (backward compat)
    const suffix = this.segmentIndex > 1 ? `_part${this.segmentIndex}` : '';
    return `meeting-bot/${this._userId}/${fileName}${suffix}${this.fileExtension}`;
  }

  private async rotateSegment(): Promise<void> {
    const oldSegmentIndex = this.segmentIndex;
    const oldTempFileId = this._tempFileId;

    // Drain the write queue so the current segment file is complete
    await this.waitForWritingFlag();
    if (this.queue.length > 0) {
      await this.writeWithRetries();
    }

    // Switch to next segment file before releasing the lock
    this.segmentIndex++;
    this._tempFileId = `${this._originalTempFileId}_part${this.segmentIndex}`;
    this.currentSegmentBytes = 0;
    this.isRotating = false;

    this._logger.info('Segment rotation complete, starting background upload', {
      userId: this._userId,
      completedSegment: oldSegmentIndex,
      nextSegment: this.segmentIndex,
    });

    const oldFilePath = DiskUploader.getFilePath(this._userId, oldTempFileId, this.fileExtension);
    const key = this.buildSegmentKey(oldSegmentIndex);

    const uploadPromise = this.uploadSegmentFile(oldFilePath, key, oldSegmentIndex);
    this.backgroundUploads.push(uploadPromise);
  }

  private async uploadSegmentFile(filePath: string, key: string, index: number): Promise<SegmentResult> {
    const provider = getStorageProvider();
    this._logger.info(`Uploading segment ${index} to object storage (${provider.name})`, { key, userId: this._userId });

    const maxAttempts = 2;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        provider.validateConfig();
        const uploadSuccess = await provider.uploadFile({
          filePath,
          key,
          contentType: this.contentType,
          metadata: this.getObjectStorageMetadata(),
          logger: this._logger,
          partSize: this.UPLOAD_CHUNK_SIZE,
          concurrency: 4,
        });

        if (!uploadSuccess) {
          throw new Error(`Segment ${index} upload returned false`);
        }

        let url: string | undefined;
        try {
          if (provider.name === 's3') {
            const s3cfg = config.s3CompatibleStorage;
            url = this.buildS3CompatibleUrl({
              endpoint: s3cfg.endpoint,
              region: s3cfg.region!,
              bucket: s3cfg.bucket!,
              forcePathStyle: !!s3cfg.forcePathStyle,
            }, key);
          } else if (provider.name === 'azure') {
            if (typeof (provider as any).getSignedUrl === 'function') {
              url = await (provider as any).getSignedUrl(key, { expiresInSeconds: config.azureBlobStorage.signedUrlTtlSeconds });
            }
          }
        } catch {}

        this._logger.info(`Segment ${index} upload complete`, { key, userId: this._userId });
        const result: SegmentResult = { success: true, key, url, index };
        this.completedSegments.push(result);

        // Clean up temp file for this segment
        try {
          await fs.promises.unlink(path.resolve(filePath));
          this._logger.info(`Temp file deleted after segment upload: ${filePath}`);
        } catch (unlinkErr) {
          this._logger.warn(`Could not delete temp file for segment ${index}`, unlinkErr);
        }

        return result;
      } catch (err) {
        if (attempt >= maxAttempts) {
          this._logger.error(`Segment ${index} upload permanently failed`, { key, err });
          return { success: false, key, index };
        }
        const delay = this.RETRY_UPLOAD_DELAY_BASE_MS * Math.pow(2, attempt);
        this._logger.warn(`Segment ${index} upload attempt ${attempt} failed, retrying in ${delay}ms`);
        await this.delayPromise(delay);
      }
    }
    return { success: false, key, index };
  }

  public setRecordingDuration(durationSeconds: number): void {
    if (!Number.isFinite(durationSeconds) || durationSeconds <= 0) {
      this._logger.warn('Ignoring invalid recording duration from recorder', { durationSeconds });
      return;
    }

    const roundedDuration = Math.round(durationSeconds);
    this.recordingDuration = roundedDuration;
    this._logger.info('Recording duration captured from recorder', {
      duration: roundedDuration,
      userId: this._userId,
      teamId: this._teamId,
    });
  }

  private static getFolderPath(userId: string) {
    const folderPath = path.join(tempFolder, userId);
    return folderPath;
  }

  private static getFilePath(userId: string, tempFileId: string, fileExtension: string) {
    const fileName = `${tempFileId}${fileExtension}`;
    const folderPath = DiskUploader.getFolderPath(userId);
    const filePath = path.join(folderPath, fileName);
    return filePath;
  }

  private async processRecordingUpload() {
    const filePath = DiskUploader.getFilePath(this._userId, this._tempFileId, this.fileExtension);
    const chunkSize = this.UPLOAD_CHUNK_SIZE;

    await this.connect();

    const stats = await fs.promises.stat(filePath);
    const totalSize = stats.size;

    let offset = 0;
    let partNumber = 1;

    while (offset < totalSize) {
      const currentChunkSize = Math.min(chunkSize, totalSize - offset);
      const buffer = Buffer.alloc(currentChunkSize);

      const fd = await fs.promises.open(filePath, 'r');
      await fd.read(buffer, 0, currentChunkSize, offset);
      await fd.close();

      this._logger.info(`Uploading part ${partNumber} (bytes ${offset}-${offset + currentChunkSize - 1})`);

      // await this.uploadChunk(buffer, partNumber);

      await this.retryUploadWithResilience(
        () => this.uploadChunk(buffer, partNumber),
        partNumber
      );

      offset += currentChunkSize;
      partNumber++;
    }

    await this.finish();

    this._logger.info(`Finished uploading ${partNumber - 1} parts.`, this._userId, this._teamId);
  }

  private delayPromise(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private async retryUploadWithResilience(fn: () => Promise<void>, partNumber: number) {
    let attempt = 0;
    while (attempt < this.MAX_CHUNK_UPLOAD_RETRIES) {
      try {
        await fn();
        return;
      } catch (err) {
        attempt++;
        if (isNoSuchUploadError(err, this._userId, this._logger)) {
          // throw this in air to restart the upload from the start
          throw err;
        }
        if (attempt < this.MAX_CHUNK_UPLOAD_RETRIES) {
          const delay = this.RETRY_UPLOAD_DELAY_BASE_MS * Math.pow(2, attempt - 1);
          this._logger.info(`Retry part ${partNumber}, attempt ${attempt} after ${delay}ms`);
          await this.delayPromise(delay);
        } else {
          this._logger.info(`Failed to upload part ${partNumber} after ${this.MAX_CHUNK_UPLOAD_RETRIES} attempts.`);
          throw err;
        }
      }
    }
  }

  private static async setupDirectory(folderPath: string, userId: string, logger: Logger) {
    try {
      if (!fs.existsSync(folderPath)) {
        logger.info('Temp Directory does not exist. Creating...', userId);
        await fs.promises.mkdir(folderPath, { recursive: true });
        logger.info('Temp Directory does not exist. Creation success...', userId);
      }
      else {
        logger.info('Found the temp directory already...', userId);
      }
    } catch (error) {
      logger.error('Failed to create directory', userId, error);
      throw error;
    }
  }

  private async deleteTempFileAsync(): Promise<void> {
    try {
      const filePath = DiskUploader.getFilePath(this._userId, this._tempFileId, this.fileExtension);
      const absPath = path.resolve(filePath);
      await fs.promises.unlink(absPath);
      this._logger.info(`Temp File deleted from disk: ${absPath}`, this._userId);
    } catch (error) {
      this._logger.warn('Could not clean up temp file:', this._userId, error);
    }
  }

  private async tempFileExists(): Promise<boolean> {
    try {
      const filePath = DiskUploader.getFilePath(this._userId, this._tempFileId, this.fileExtension);
      await fs.promises.access(filePath);
      return true;
    } catch {
      return false;
    }
  }

  private async waitForWritingFlag() {
    const userId = `${this._userId}`;

    const waitPromise = new Promise((resolve) => {
      const waitInterval = setInterval(() => {
        if (this.writing) {
          this._logger.info('Waiting on finish temp file write...', userId);
        } else {
          clearInterval(waitInterval);
          resolve(true);
        }
      }, 500);
    });

    await waitPromise;
    this._logger.info('Finish wait on temp file write...', userId);
  }

  private async isMp4File(filePath: string): Promise<boolean> {
    const file = await fs.promises.open(filePath, 'r');
    try {
      const buffer = Buffer.alloc(12);
      const { bytesRead } = await file.read(buffer, 0, buffer.length, 0);
      return bytesRead >= 12 && buffer.toString('ascii', 4, 8) === 'ftyp';
    } finally {
      await file.close();
    }
  }

  private async ensureRequestedContainer(filePath: string): Promise<void> {
    if (this.fileExtension !== '.mp4') return;

    if (await this.isMp4File(filePath)) {
      return;
    }

    const outputPath = `${filePath}.transcoded.mp4`;
    this._logger.info('Recording temp file is not an MP4 container. Transcoding before upload...', {
      inputPath: filePath,
      outputPath,
    });

    try {
      await execFileAsync('ffmpeg', [
        '-y',
        '-i', filePath,
        '-c:v', 'libx264',
        '-preset', 'veryfast',
        '-pix_fmt', 'yuv420p',
        '-c:a', 'aac',
        '-movflags', '+faststart',
        outputPath,
      ], { maxBuffer: 10 * 1024 * 1024 });

      await fs.promises.rename(outputPath, filePath);
      this._logger.info('Transcoded recording temp file to MP4 container before upload.', { filePath });
    } catch (err) {
      try {
        await fs.promises.unlink(outputPath);
      } catch {}
      throw err;
    }
  }

  private async ensureWebmDurationMetadata(filePath: string): Promise<void> {
    if (this.fileExtension !== '.webm') return;

    if (!this.recordingDuration) {
      this._logger.warn('Skipping WebM duration metadata patch because recording duration is unknown.', { filePath });
      return;
    }

    this._logger.info('Patching WebM recording duration metadata...', {
      filePath,
      duration: this.recordingDuration,
    });

    try {
      await writeWebmDurationMetadata(filePath, this.recordingDuration);
      this._logger.info('Patched WebM recording duration metadata.', { filePath });
    } catch (err) {
      this._logger.warn('Unable to patch WebM recording duration metadata; continuing with duration field only.', {
        filePath,
        error: err,
      });
    }
  }

  private async getMediaDuration(filePath: string): Promise<number | undefined> {
    try {
      const { stdout } = await execFileAsync('ffprobe', [
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        filePath,
      ], { maxBuffer: 1024 * 1024 });

      const duration = Number.parseFloat(stdout.trim());
      if (!Number.isFinite(duration) || duration <= 0) return undefined;
      return Math.round(duration);
    } catch (err) {
      this._logger.warn('Unable to determine recording duration with ffprobe', { filePath, error: err });
      return undefined;
    }
  }

  private async finalizeDiskWriting() {
    try {
      await this.waitForWritingFlag();

      // Check if the queue is empty
      if (this.queue.length > 0) {
        // Final attempt to finish the disk write
        await this.writeWithRetries();
      }

      const filePath = DiskUploader.getFilePath(this._userId, this._tempFileId, this.fileExtension);
      await this.ensureRequestedContainer(filePath);

      if (!this.recordingDuration && this.firstChunkReceivedAt) {
        const elapsedSeconds = Math.round((Date.now() - this.firstChunkReceivedAt) / 1000);
        if (elapsedSeconds > 0) {
          this.recordingDuration = elapsedSeconds;
          this._logger.info('Recording duration estimated from chunk receive time', {
            duration: this.recordingDuration,
            userId: this._userId,
            teamId: this._teamId,
          });
        }
      }

      await this.ensureWebmDurationMetadata(filePath);

      const probedDuration = await this.getMediaDuration(filePath);
      if (typeof probedDuration === 'number') {
        this.recordingDuration = probedDuration;
      }

      return true;
    } catch(err) {
      this._logger.info('Critical: Failed to finalise temp file write...', this._userId, err);
      return false;
    }
  }

  private async uploadRecordingToScreenApp(): Promise<boolean> {
    this._logger.info('Uploading recording to screenapp...');
    let attempt = 0;
    let success = false;
    do {
      try {
        this.diskWriteSuccess.flush();

        await this.processRecordingUpload();
        success = true;
      } catch (err) {
        if (isNoSuchUploadError(err, this._userId, this._logger)) {
          attempt += 1;
          this._logger.info('Processing NoSuchUpload error...', this._userId);
          if (attempt >= this.MAX_FILE_UPLOAD_RETRIES) {
            throw err;
          }
          this._logger.info('NoSuchUpload detected, restarting upload session...', this._userId);
        } else {
          throw err;
        }
      }
    } while (!success);

    return success;
  }

  private async uploadRecordingToObjectStorage(): Promise<boolean> {
    const provider = getStorageProvider();
    this._logger.info(`Uploading final segment to object storage using provider: ${provider.name}...`);

    // Wait for any in-progress segment rotation before uploading the final piece
    if (this.isRotating) {
      this._logger.info('Waiting for in-progress segment rotation to finish before final upload...');
      await new Promise<void>((resolve) => {
        const check = setInterval(() => {
          if (!this.isRotating) {
            clearInterval(check);
            resolve();
          }
        }, 100);
      });
    }

    // Wait for all background segment uploads to finish
    if (this.backgroundUploads.length > 0) {
      this._logger.info(`Waiting for ${this.backgroundUploads.length} background segment upload(s) to complete...`);
      const bgResults = await Promise.allSettled(this.backgroundUploads);
      bgResults.forEach((r, i) => {
        if (r.status === 'rejected') {
          this._logger.error(`Background segment upload ${i + 1} rejected`, r.reason);
        }
      });
    }

    const filePath = DiskUploader.getFilePath(this._userId, this._tempFileId, this.fileExtension);
    const key = this.buildFinalKey();

    const finalResult = await this.uploadSegmentFile(filePath, key, this.segmentIndex);

    if (!finalResult.success) {
      throw new Error(`Failed to upload final segment to ${provider.name}`);
    }

    // Build lastStorageDetails with all segments for the notification payload
    const allSegments = [...this.completedSegments].sort((a, b) => a.index - b.index);
    const firstUrl = allSegments[0]?.url ?? finalResult.url;

    try {
      if (provider.name === 's3') {
        const s3cfg = config.s3CompatibleStorage;
        this.lastUploadedBlobUrl = firstUrl;
        this.lastStorageDetails = {
          provider: 's3',
          bucket: s3cfg.bucket,
          region: s3cfg.region,
          endpoint: s3cfg.endpoint,
          forcePathStyle: !!s3cfg.forcePathStyle,
          url: this.lastUploadedBlobUrl,
          duration: this.recordingDuration,
          segments: allSegments.map(({ key: k, url: u, index: idx }) => ({ key: k, url: u, index: idx })),
        };
      } else if (provider.name === 'azure') {
        this.lastUploadedBlobUrl = firstUrl;
        this.lastStorageDetails = {
          provider: 'azure',
          accountName: config.azureBlobStorage.accountName,
          container: config.azureBlobStorage.container,
          url: this.lastUploadedBlobUrl,
          signedUrlTtlSeconds: config.azureBlobStorage.signedUrlTtlSeconds,
          blobPrefix: config.azureBlobStorage.blobPrefix,
          duration: this.recordingDuration,
          segments: allSegments.map(({ key: k, url: u, index: idx }) => ({ key: k, url: u, index: idx })),
        };
      }
    } catch (metaErr) {
      this._logger.warn('Unable to compute storage metadata/url for notification', metaErr as any);
    }

    this._logger.info(`All segments uploaded successfully. Total: ${allSegments.length}`, { userId: this._userId });
    return true;
  }

  private buildS3CompatibleUrl(uploadConfig: { endpoint?: string; region: string; bucket: string; forcePathStyle: boolean; }, key: string): string | undefined {
    try {
      const safeKey = encodeURI(key);
      if (uploadConfig.endpoint) {
        const ep = uploadConfig.endpoint.replace(/\/$/, '');
        if (uploadConfig.forcePathStyle) {
          return `${ep}/${uploadConfig.bucket}/${safeKey}`;
        }
        // Virtual-hosted-style with custom endpoint
        const url = new URL(ep);
        // Prepend bucket as subdomain if possible
        return `${url.protocol}//${uploadConfig.bucket}.${url.host}/${safeKey}`;
      }
      // Default AWS endpoint pattern
      return `https://${uploadConfig.bucket}.s3.${uploadConfig.region}.amazonaws.com/${safeKey}`;
    } catch {
      return undefined;
    }
  }

  private getObjectStorageMetadata(): Record<string, string> | undefined {
    const metadata: Record<string, string> = {
      contentType: this.contentType,
      uploaderType: config.uploaderType,
    };

    if (typeof this.recordingDuration === 'number' && Number.isFinite(this.recordingDuration) && this.recordingDuration > 0) {
      const duration = String(Math.round(this.recordingDuration));
      metadata.duration = duration;
      metadata.durationSeconds = duration;
      metadata.recordingDurationSeconds = duration;
    }

    return metadata;
  }

  public async uploadRecordingToRemoteStorage(options?: { forceUpload?: boolean }) {
    try {
      if (typeof options?.forceUpload === 'boolean') {
        this.forceUpload = options.forceUpload;
      }

      if (!await this.tempFileExists()) {
        throw new Error(`Unable to access the temp recording file on disk: ${this._userId} ${this._botId}`);
      }

      const goodToGo = await this.finalizeDiskWriting();

      if (this.forceUpload) {
        this._logger.info('Force upload is enabled. Ignoring disk writing check results...', { goodToGo });
      } else if (!goodToGo) {
        throw new Error(`Unable to finalise the temp recording file: ${this._userId} ${this._botId}`);
      }

      let uploadResult = false;
      // Upload recording to configured storage
      if (config.uploaderType === 'screenapp') {
        uploadResult = await this.uploadRecordingToScreenApp();
        // Screenapp path: delete temp file after upload (segment uploads clean up their own files)
        await this.deleteTempFileAsync();
      } else if (config.uploaderType === 's3') {
        // Route to selected object storage provider (S3 or Azure) based on configuration.
        // Each segment upload deletes its own temp file, so no deleteTempFileAsync needed here.
        uploadResult = await this.uploadRecordingToObjectStorage();
      } else {
        throw new Error(`Unsupported UPLOADER_TYPE configuration: ${config.uploaderType}`);
      }

      // Send optional notifications on success
      if (uploadResult) {
        try {
          const payload: RecordingCompletedPayload = {
            recordingId: this.lastRecordingId ?? this._tempFileId,
            meetingLink: this._meetingLink,
            status: 'completed',
            blobUrl: this.lastUploadedBlobUrl,
            timestamp: new Date().toISOString(),
            metadata: {
              userId: this._userId,
              teamId: this._teamId,
              botId: this._botId,
              contentType: this.contentType,
              uploaderType: config.uploaderType,
              duration: this.recordingDuration,
              storage: this.lastStorageDetails,
            },
          };
          await notifyRecordingCompleted(payload, this._logger);
        } catch (notifyErr) {
          this._logger.warn('Recording completed notification failed', notifyErr as any);
        }
      }

      return uploadResult;
    } catch (err) {
      this._logger.info('Unable to upload recording to server...', { error: err, userId: this._userId, teamId: this._teamId });
      return false;
    }
  }
}

export default DiskUploader;
