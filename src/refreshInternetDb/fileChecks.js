const { getLocalStorageProperty } = require('./localStorage');
const { getFileSizeInGB } = require('../dataTransformations');
const {
  FINAL_DB_DECOMPRESSION_FILEPATH,
  COMPRESSED_DB_FILEPATH
} = require('../constants');


const shouldDownloadAndDecompress = (downloadLink) =>
  _fileIsOutOfDate(downloadLink, FINAL_DB_DECOMPRESSION_FILEPATH);

const shouldSkipCompressedDatabaseDownload = (downloadLink) =>
  !_fileIsOutOfDate(downloadLink, COMPRESSED_DB_FILEPATH);

const _fileIsOutOfDate = (downloadLink, filepath) => {
  const oldDownloadLink = getLocalStorageProperty('oldDownloadLink');

  const downloadLinkHasChanged = downloadLink !== oldDownloadLink;
  const databaseFileIsEmpty = !getFileSizeInGB(filepath);

  return downloadLinkHasChanged || databaseFileIsEmpty;
};

module.exports = { shouldDownloadAndDecompress, shouldSkipCompressedDatabaseDownload };