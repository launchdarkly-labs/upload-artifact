import * as core from '@actions/core'
import {S3Client} from '@aws-sdk/client-s3'
import * as mime from 'mime-types'
import fs from 'node:fs'
import {realpath} from 'node:fs/promises'
import crypto from 'node:crypto'
import path from 'node:path'
import {getUploadChunkSize, UploadOptions} from '../upload/constants'
import * as archiver from 'archiver'
import { Upload } from '@aws-sdk/lib-storage'
import {compressZstd} from './compress'

export async function uploadArtifact(
  filesToUpload: string[],
  rootDirectory: string,
  options: UploadOptions
) {
  const expiryDate = new Date()
  expiryDate.setDate(expiryDate.getDate() )

  const zipFilePath = await compressZstd(
    filesToUpload,
    rootDirectory,
    options.artifactName,
    options.compressionLevel || 6
  )

  // Get file stats for size info
  const fileStats = fs.statSync(zipFilePath)
  const fileSize = fileStats.size

  // Compute MD5 hash for the file (optional but good for integrity)
  const fileDigest = await computeMd5(zipFilePath)

  // Determine S3 key using prefix and artifact name
  const s3Key = `${options.prefix}/${options.artifactName}.tar.zst`

  // Upload to S3
  try {
    const fileStream = fs.createReadStream(zipFilePath)

    // Get optimal queue size based on file size
    const getOptimalQueueSize = (fileSize: number) => {
      if (fileSize > 1024 * 1024 * 1024) { // > 1GB
        return 20;
      } else if (fileSize > 100 * 1024 * 1024) { // > 100MB
        return 10;
      } else {
        return 4;
      }
    };

    // Get optimal part size based on file size
    const getOptimalPartSize = (fileSize: number) => {
      if (fileSize > 1024 * 1024 * 1024) { // > 1GB
        return 16 * 1024 * 1024; // 16MB
      } else {
        return getUploadChunkSize(); // Default 8MB
      }
    };


    const upload = new Upload({
      client: new S3Client({
        region: options.awsRegion,
        maxAttempts: 3
      }),
      queueSize: getOptimalQueueSize(fileSize),
      partSize: getOptimalPartSize(fileSize),
      leavePartsOnError: false,
      params: {
        Bucket: options.bucketName,
        Key: s3Key,
        ContentType: mime.lookup(zipFilePath) || 'application/zip',
        ContentLength: fileSize,
        Expires: expiryDate,
        Body: fileStream
      }
    })

    let uploadedBytes = 0;
    const startTime = Date.now();

    upload.on("httpUploadProgress", (progress) => {
      if (progress.loaded && progress.total) {
        uploadedBytes = progress.loaded;
        const elapsedSeconds = (Date.now() - startTime) / 1000;
        const uploadSpeed = uploadedBytes / elapsedSeconds / 1024 / 1024; // MB/s

        core.info(`Upload progress: ${Math.round((progress.loaded / progress.total) * 100)}% | Speed: ${uploadSpeed.toFixed(2)} MB/s`);
      }
    });

    await upload.done();

    const artifactId = crypto.createHash('sha256').update(`${options.bucketName}/${s3Key}`).digest('hex').substring(0, 8)

    core.info(
      `Artifact ${options.artifactName} has been successfully uploaded! Final size is ${fileSize} bytes. Artifact ID is ${artifactId}`
    )
    core.setOutput('artifact-id', artifactId)
    core.setOutput('artifact-digest', fileDigest)

    // Create an artifact URL (this would be your S3 URL or a signed URL)
    const artifactURL = `s3://${options.bucketName}/${s3Key}`

    core.info(`Artifact upload location: ${artifactURL}`)
    core.setOutput('artifact-url', artifactURL)

    // Clean up the temporary zip file if needed
    fs.unlinkSync(zipFilePath)

    return {
      id: artifactId,
      size: fileSize,
      digest: fileDigest
    }
  } catch (error) {
    core.error(`Failed to upload artifact to S3: ${error}`)
    throw error
  }

}

function computeMd5(filePath: string): Promise<string> {
  return new Promise((resolve, reject) => {
    // Create an MD5 hash instance
    const hash = crypto.createHash('md5');

    // Create a file read stream
    const stream = fs.createReadStream(filePath);

    // Update the hash with data as we read it from the stream
    stream.on('data', (chunk) => {
      hash.update(chunk);
    });

    // When the stream is finished, resolve the promise with the final hash
    stream.on('end', () => {
      const md5Checksum = hash.digest('hex');
      resolve(md5Checksum);
    });

    // Handle errors
    stream.on('error', (err) => {
      reject(err);
    });
  });
}

export async function zipper(
  files: string[],
  rootDirectory: string,
  artifactName: string,
  compressionLevel: number
): Promise<string> {
    core.debug(`Creating Artifact archive with compressionLevel: ${compressionLevel}`)

    // Define the zip file path using the artifactName
    const zipFilePath = `${artifactName}.zip`

    // Create a write stream for the zip file
    const output = fs.createWriteStream(zipFilePath)


  const zip = archiver.create('zip', {
      highWaterMark: getUploadChunkSize(),
      zlib: {
        level: compressionLevel
      }
    })

  // register callbacks for various events during the zip lifecycle
  zip.on('error', zipErrorCallback)
  zip.on('warning', zipWarningCallback)

  zip.pipe(output)

  for (const file of files) {
    try {
      const stats = fs.statSync(file)
      // Calculate the relative path from rootDirectory
      const relativePath = path.relative(rootDirectory, file)

      if (stats.isSymbolicLink()) {
        core.debug(`Processing ${file} as a symbolic link`)
        const realFilePath = await realpath(file)
        zip.file(realFilePath, {name: relativePath})
      } else if (stats.isFile()) {
        core.debug(`Processing ${file} as a file`)
        zip.file(file, {name: relativePath})
      }
    } catch (error) {
      core.warning(`Failed to process ${file}: ${error}`)
    }
  }


  // Create a promise to wait for the zip finalization to complete
  const finalizePromise = new Promise<void>((resolve, reject) => {
    output.on('close', () => {
      core.debug(`Zip archive created at ${zipFilePath}`)
      resolve()
    })

    output.on('error', (err) => {
      reject(err)
    })
  })

  // Finalize the zip file
  await zip.finalize()

  // Wait for the output stream to close
  await finalizePromise

  // Return the path to the created zip file
  return zipFilePath
}

// Taken from https://github.com/actions/toolkit/blob/683703c1149439530dcee7b8c5dbbfeec4104368/packages/artifact/src/internal/upload/zip.ts#L78-L107
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const zipErrorCallback = (error: any): void => {
  core.error('An error has occurred while creating the zip file for upload')
  core.info(error)

  throw new Error('An error has occurred during zip creation for the artifact')
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const zipWarningCallback = (error: any): void => {
  if (error.code === 'ENOENT') {
    core.warning(
      'ENOENT warning during artifact zip creation. No such file or directory'
    )
    core.info(error)
  } else {
    core.warning(
      `A non-blocking warning has occurred during artifact zip creation: ${error.code}`
    )
    core.info(error)
  }
}

const zipFinishCallback = (): void => {
  core.debug('Zip stream for upload has finished.')
}

const zipEndCallback = (): void => {
  core.debug('Zip stream for upload has ended.')
}
