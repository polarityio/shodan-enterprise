const fp = require('lodash/fp');

const config = require('../../config/config');
const checkDiskSpace = require('check-disk-space').default;
const { getFileSizeInGB } = require('../dataTransformations');

const {
  FINAL_DB_DECOMPRESSION_FILEPATH,
  TEMP_DB_DECOMPRESSION_FILEPATH
} = require('../constants');
const { getLocalStorageProperty } = require('./localStorage');

const checkForEnoughDiskSpace = async (Logger) => {
  const freeDiskSpace = fp.get('free', await checkDiskSpace('/'));

  //Guestimate to start ~42GBs
  let minimumDiskSpaceNeeded = 45000000000;
  
  let databaseFileSize =
  getFileSizeInGB(TEMP_DB_DECOMPRESSION_FILEPATH) ||
  getFileSizeInGB(FINAL_DB_DECOMPRESSION_FILEPATH);
  
  const dataHasBeenLoadedIntoIpsTable =
  getLocalStorageProperty('dataHasBeenLoadedIntoIpsTable');
  
  if (databaseFileSize) {
    //Make guestimate proportional to file size
    databaseFileSize = databaseFileSize * 1000000000;
    minimumDiskSpaceNeeded = dataHasBeenLoadedIntoIpsTable
    ? databaseFileSize * 1.3
    : databaseFileSize * 2.05;
  }

  if (config.minimizeEndDatabaseSize && !dataHasBeenLoadedIntoIpsTable)
    minimumDiskSpaceNeeded *= 1.6;
  
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
