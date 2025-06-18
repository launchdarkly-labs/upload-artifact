/* eslint-disable no-unused-vars */
export enum Inputs {
  BucketName = 'bucket-name',
  AwsRegion = 'aws-region',
  Prefix = 'prefix',
  Name = 'name',
  Path = 'path',
  IfNoFilesFound = 'if-no-files-found',
  RetentionDays = 'retention-days',
  CompressionLevel = 'compression-level',
  Overwrite = 'overwrite',
  IncludeHiddenFiles = 'include-hidden-files'
}

export enum NoFileOptions {
  /**
   * Default. Output a warning but do not fail the action
   */
  warn = 'warn',

  /**
   * Fail the action with an error message
   */
  error = 'error',

  /**
   * Do not output any warnings or errors, the action does not fail
   */
  ignore = 'ignore'
}

export interface UploadOptions {
  bucketName: string
  awsRegion: string
  prefix: string
  artifactName: string
  searchPath: string
  ifNoFilesFound: NoFileOptions
  overwrite: boolean
  includeHiddenFiles: boolean
  retentionDays: number
  compressionLevel?: number
}

// Taken from https://github.com/actions/toolkit/blob/main/packages/artifact/src/internal/shared/config.ts#L6
// Used for controlling the highWaterMark value of the zip that is being streamed
// The same value is used as the chunk size that is use during upload to blob storage
export function getUploadChunkSize(): number {
  return 8 * 1024 * 1024 // 8 MB Chunks
}
