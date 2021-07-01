const fp = require('lodash/fp');

const config = require('../../config/config');
const checkDiskSpace = require('check-disk-space').default;
const { getFileSizeInGB } = require('../dataTransformations');

const {
  FINAL_DB_DECOMPRESSION_FILEPATH,
  TEMP_DB_DECOMPRESSION_FILEPATH
} = require('../constants');

const checkForEnoughDiskSpace = async (Logger) => {
  const freeDiskSpace = fp.get('free', await checkDiskSpace('/'));

  let minimumDiskSpaceNeeded = 46000000000;
  
  let databaseFileSize =
    getFileSizeInGB(TEMP_DB_DECOMPRESSION_FILEPATH) ||
    getFileSizeInGB(FINAL_DB_DECOMPRESSION_FILEPATH);
  
  
  if (databaseFileSize) {
    databaseFileSize = Math.floor((databaseFileSize + 0.01) * 1073741824);
    minimumDiskSpaceNeeded = Math.max(minimumDiskSpaceNeeded, databaseFileSize * 2.1);
  }

  if (config.minimizeEndDatabaseSize) minimumDiskSpaceNeeded *= 2;

  minimumDiskSpaceNeeded -= databaseFileSize;

  const thereIsNotEnoughDiskSpace = freeDiskSpace < minimumDiskSpaceNeeded;

  if (thereIsNotEnoughDiskSpace) {
    throw Error(
      `Not Enough Disk Space For Database Refresh ->  Need ~${
        Math.floor(((minimumDiskSpaceNeeded - freeDiskSpace) / 1073741824) * 1000) / 1000
      }GBs more free.`
    );
  }
};


module.exports = checkForEnoughDiskSpace;
