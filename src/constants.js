const IGNORED_IPS = new Set(['127.0.0.1', '255.255.255.255', '0.0.0.0']);

const COMPRESSED_DB_FILEPATH = './data/new-internetdb.sqlite.bz2';
const FINAL_DB_DECOMPRESSION_FILEPATH = './data/internetdb.sqlite';
const TEMP_DB_DECOMPRESSION_FILEPATH = './data/new-internetdb.sqlite';
const LOCAL_STORAGE_FILEPATH = './data/local-storage.json';

module.exports = {
  IGNORED_IPS,
  COMPRESSED_DB_FILEPATH,
  FINAL_DB_DECOMPRESSION_FILEPATH,
  TEMP_DB_DECOMPRESSION_FILEPATH,
  LOCAL_STORAGE_FILEPATH
};
