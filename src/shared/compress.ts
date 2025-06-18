import * as core from '@actions/core'
import * as fs from 'node:fs'
import * as path from 'node:path'
import * as tar from 'tar-stream'
import { ZSTDCompress } from 'simple-zstd';
import {promisify} from 'node:util'
import {pipeline} from 'node:stream'
import { realpath } from 'node:fs/promises'

const pipelineAsync = promisify(pipeline);

export async function compressZstd(files: string[], rootDirectory: string, artifactName: string, compressionLevel: number = 3): Promise<string> {
  core.debug(`Creating artifact archive with Zstd compressionLevel: ${compressionLevel}`);

  // Define the output file path with .tar.zst extension
  const outputFilePath = `${artifactName}.tar.zst`;

  try {
    // Create a tar-stream pack instance
    const pack = tar.pack();

    const outputStream = fs.createWriteStream(outputFilePath);
    const zstdCompress = new ZSTDCompress(compressionLevel);

    const pipelinePromise = pipelineAsync(
      pack,
      zstdCompress,
      outputStream
    );

    // Process all files
    for (const file of files) {
      try {
        const stats = fs.statSync(file);
        const relativePath = path.relative(rootDirectory, file);

        if (stats.isSymbolicLink()) {
          core.debug(`Processing ${file} as a symbolic link`);
          const realFilePath = await realpath(file);
          const linkTarget = fs.readlinkSync(file);

          const entry = pack.entry({
            name: relativePath,
            type: 'symlink',
            linkname: linkTarget,
            size: 0
          });

          entry.end();
        } else if (stats.isFile()) {
          core.debug(`Processing ${file} as a file`);

          // Create entry in the tar archive
          const entry = pack.entry({
            name: relativePath,
            size: stats.size,
            mode: stats.mode,
            mtime: stats.mtime
          });

          const fileStream = fs.createReadStream(file);
          await new Promise((resolve, reject) => {
            fileStream.on('error', reject);
            fileStream.on('end', resolve);
            fileStream.pipe(entry);
          });
        }
      } catch (error) {
        core.warning(`Failed to process ${file}: ${error}`);
      }
    }

    // Finalize the tar pack
    pack.finalize();
    await pipelinePromise;

    core.debug(`Tar+Zstd archive created at ${outputFilePath}`);
    return outputFilePath;
  } catch (error) {
    core.error(`Failed to create tar+zstd archive: ${error}`);
    throw error;
  }
}
