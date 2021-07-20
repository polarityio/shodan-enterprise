const fs = require('fs');
const request = require('request');

const { COMPRESSED_DB_FILEPATH } = require('../constants');
const { getFileSizeInGB } = require('../dataTransformations');
const { setLocalStorageProperty } = require('./localStorage');
const { shouldSkipCompressedDatabaseDownload } = require('./fileChecks');


const downloadCompressedDatabase = async (downloadLink, requestDefaults, Logger) => {
  try {
    Logger.trace('Starting Compressed Database Download');

    setLocalStorageProperty('databaseReformatted', false);
    
    if (shouldSkipCompressedDatabaseDownload(downloadLink)) {
      Logger.info(
        'Compressed Database File Exists and is Up to Date.  Skipping Compressed Database Download.'
      );
      return;
    }

    if (fs.existsSync(COMPRESSED_DB_FILEPATH)) {
      fs.unlinkSync(COMPRESSED_DB_FILEPATH);
    }

    const file = fs.createWriteStream(COMPRESSED_DB_FILEPATH);

    await new Promise((resolve, reject) =>
      request({ ...requestDefaults, uri: downloadLink })
        .pipe(file)
        .on('finish', resolve)
        .on('error', reject)
    );

    setLocalStorageProperty('oldDownloadLink', downloadLink);

    const compressedDatabaseFileSize = getFileSizeInGB(COMPRESSED_DB_FILEPATH);

    Logger.info(
      `Compressed Database Download Complete. File Size: ${compressedDatabaseFileSize}GB`
    );
  } catch (error) {
    Logger.error(error, 'Error when Downloading Compressed Database File');
    throw error;
  }
};

module.exports = downloadCompressedDatabase;