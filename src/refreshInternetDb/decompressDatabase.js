const fs = require('fs');
const util = require('util');
const exec = util.promisify(require('child_process').exec);

const { lessStorageMoreDowntime } = require('../../config/config');
const { getFileSizeInGB } = require('../dataTransformations');
const { getLocalStorageProperty, setLocalStorageProperty } = require('./localStorage');

const {
  COMPRESSED_DB_FILEPATH,
  TEMP_DB_DECOMPRESSION_FILEPATH,
  FINAL_DB_DECOMPRESSION_FILEPATH
} = require('../constants');

const decompressDatabase = async (knex, setKnex, Logger) => {
  try {
    Logger.trace('Starting Database Decompression');

    let decompressionFilePath = TEMP_DB_DECOMPRESSION_FILEPATH;
    if (lessStorageMoreDowntime) {
      Logger.info(
        'Deleting Current Database before Decompression. Searching will be disabled during this time.'
      );

      decompressionFilePath = FINAL_DB_DECOMPRESSION_FILEPATH;

      if (knex && knex.destroy) {
        setKnex(undefined);
        await knex.destroy();
      }
      if (fs.existsSync(TEMP_DB_DECOMPRESSION_FILEPATH)) {
        fs.unlinkSync(TEMP_DB_DECOMPRESSION_FILEPATH);
      }
      if (fs.existsSync(FINAL_DB_DECOMPRESSION_FILEPATH)) {
        fs.unlinkSync(FINAL_DB_DECOMPRESSION_FILEPATH);
      }
    }

    const { stdout: databaseDecompressionMessage, stderr } = await exec(
      `bzip2 -dc1 ${COMPRESSED_DB_FILEPATH} > ${decompressionFilePath}`
    );

    if (fs.existsSync(COMPRESSED_DB_FILEPATH)) {
      fs.unlinkSync(COMPRESSED_DB_FILEPATH);
    }

    if (stderr) {
      throw new Error(`Database Decompression Failed -> ${stderr}`);
    }

    const databaseFileSize = getFileSizeInGB(decompressionFilePath);

    Logger.info(
      databaseDecompressionMessage ? { databaseDecompressionMessage } : '',
      `Database Decompression Complete. Database File Size Before Indexing: ${databaseFileSize}GB`
    );
  } catch (error) {
    Logger.error(error, 'Error when Decompressing Database File');
    if (error.message.includes('bzip2: command not found')) {
      throw new Error(
        "Must run 'npm run build' on your server then restart the integration before using this integration is possible."
      );
    }
    throw error;
  }
};

const decompressPreformattedDatabase = async (Logger) => {
  try {
    Logger.trace(
      'Starting Preformatted Database Decompression. This could take a few minutes'
    );

    const preformattedCompressedDbPath = './data/internetdb.sqlite.bz2';
    const compressedFileExists = fs.existsSync(preformattedCompressedDbPath);
    const formattedDatabaseExists = fs.existsSync(FINAL_DB_DECOMPRESSION_FILEPATH);

    if (
      formattedDatabaseExists &&
      !compressedFileExists &&
      getLocalStorageProperty('databaseReformatted')
    ) {
      Logger.info('Preformatted Database already decompressed.');
      return;
    }

    if (fs.existsSync(TEMP_DB_DECOMPRESSION_FILEPATH)) {
      fs.unlinkSync(TEMP_DB_DECOMPRESSION_FILEPATH);
    }
    if (formattedDatabaseExists) {
      fs.unlinkSync(FINAL_DB_DECOMPRESSION_FILEPATH);
    }

    if (!compressedFileExists) {
      throw Error(
        `Compress Preformatted Database File Not Found -> Make sure file is located at ${preformattedCompressedDbPath}`
      );
    }

    const { stdout: databaseDecompressionMessage, stderr } = await exec(
      `bzip2 -dc1 ${preformattedCompressedDbPath} > ${FINAL_DB_DECOMPRESSION_FILEPATH}`
    );

    if (stderr) {
      throw new Error(`Database Decompression Failed -> ${stderr}`);
    }

    if (compressedFileExists) {
      fs.unlinkSync(preformattedCompressedDbPath);
    }

    setLocalStorageProperty('databaseReformatted', true);

    const databaseFileSize = getFileSizeInGB(FINAL_DB_DECOMPRESSION_FILEPATH);

    Logger.info(
      databaseDecompressionMessage ? { databaseDecompressionMessage } : '',
      `Database Decompression Complete. Database File Size: ${databaseFileSize}GB`
    );
  } catch (error) {
    Logger.error(error, 'Error when Decompressing Preformatted Database File');
    if (error.message.includes('bzip2: command not found')) {
      throw new Error(
        "Must run 'npm run build' on your server then restart the integration before using this integration is possible."
      );
    }
    throw error;
  }
};

module.exports = { decompressDatabase, decompressPreformattedDatabase };
