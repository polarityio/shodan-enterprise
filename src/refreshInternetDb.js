const fp = require('lodash/fp');
const fs = require('fs');
const request = require('request');
const config = require('../config/config');
const util = require('util');
const exec = util.promisify(require('child_process').exec);

const COMPRESSED_DB_FILEPATH = './data/new-internetdb.sqlite.bz2';
const FINAL_DB_DECOMPRESSION_FILEPATH = './data/internetdb.sqlite';
const TEMP_DB_DECOMPRESSION_FILEPATH = './data/new-internetdb.sqlite';

const refreshInternetDb =
  (
    knex,
    setKnex,
    setDataIsLoadedIn,
    requestDefaults,
    requestWithDefaults,
    Logger
  ) =>
  async () => {
    Logger.info('Starting Database Refresh');

    const startTime = new Date();

    let shodanEnterpriseApiKey = fp.get('shodanEnterpriseApiKey', config) || process.env.SHODAN_ENTERPRISE_API_KEY;
    const lessStorageMoreDowntime = fp.get('lessStorageMoreDowntime', config);

    if (!shodanEnterpriseApiKey) {
      throw new Error('Shodan Enterprise API Key not set in config.js');
    }

    const databaseFileExists = fs.existsSync(FINAL_DB_DECOMPRESSION_FILEPATH);

    const downloadLink = await getDownloadLink(
      shodanEnterpriseApiKey,
      requestWithDefaults,
      Logger
    );

    if (linkIsNew(downloadLink, databaseFileExists)) {
      Logger.info(
        'Downloading and Decompressing Entire Database. This could take a few minutes.'
      );

      await downloadFile(downloadLink, requestDefaults, Logger);

      await decompressDatabase(
        knex,
        setKnex,
        setDataIsLoadedIn,
        databaseFileExists,
        lessStorageMoreDowntime,
        Logger
      );
    }

    await readInFileToKnex(
      knex,
      setKnex,
      setDataIsLoadedIn,
      databaseFileExists,
      lessStorageMoreDowntime,
      Logger
    );

    const endTime = new Date();

    const loadTime = millisToHoursMinutesAndSeconds(startTime - endTime);

    Logger.info(`Refreshing Database Complete. Load Time: ${loadTime}`);
  };

const getDownloadLink = async (apiKey, requestWithDefaults, Logger) => {
  Logger.trace('Getting Database Download Link...');

  const downloadLinksResult = await requestWithDefaults({
    url: 'https://api.shodan.io/shodan/data/internetdb',
    qs: { key: apiKey },
    json: true
  });

  const downloadLink = fp.flow(
    fp.get('body'),
    fp.find(fp.flow(fp.get('name'), fp.includes('sqlite'))),
    fp.get('url')
  )(downloadLinksResult);

  Logger.trace(`Database Download Link: ${downloadLink}`);

  return downloadLink;
};

const linkIsNew = (downloadLink, databaseFileExists) => {
  const oldDownloadPath = './data/oldDownloadLink';
  const oldDownloadLink = fs.existsSync(oldDownloadPath)
    ? fs.readFileSync(oldDownloadPath, 'utf8')
    : '';

  if (downloadLink === oldDownloadLink && databaseFileExists) return false;

  fs.writeFileSync(oldDownloadPath, downloadLink);
  return true;
};

const downloadFile = async (downloadLink, requestDefaults, Logger) => {
  try {
    Logger.trace('Starting Compressed Database Download');

    if (fs.existsSync(COMPRESSED_DB_FILEPATH))
      fs.unlinkSync(COMPRESSED_DB_FILEPATH);

    const file = fs.createWriteStream(COMPRESSED_DB_FILEPATH);

    await new Promise((resolve, reject) =>
      request({ ...requestDefaults, uri: downloadLink })
        .pipe(file)
        .on('finish', resolve)
        .on('error', reject)
    );

    const compressedDatabaseFileSize =
      fs.statSync(COMPRESSED_DB_FILEPATH).size / (1024 * 1024);

    Logger.info(
      `Compressed Database Download Complete. File Size: ${compressedDatabaseFileSize}MB`
    );
  } catch (error) {
    Logger.error(error, 'Error when Downloading Compressed Database File');
    throw error;
  }
};

const decompressDatabase = async (
  knex,
  setKnex,
  setDataIsLoadedIn,
  databaseFileExists,
  lessStorageMoreDowntime,
  Logger
) => {
  try {
    Logger.trace('Starting Database Decompression');

    let decompressionFilePath = lessStorageMoreDowntime
      ? TEMP_DB_DECOMPRESSION_FILEPATH
      : FINAL_DB_DECOMPRESSION_FILEPATH;

    if (lessStorageMoreDowntime) {
      Logger.info(
        'Deleting Current Database before Decompression. Searching will be disabled during this time.'
      );

      if (knex && knex.destroy) {
        setDataIsLoadedIn(false);
        await knex.destroy();
      }
      if (databaseFileExists) fs.unlinkSync(FINAL_DB_DECOMPRESSION_FILEPATH);

      setKnex(undefined);
    }

    const { stdout: databaseDecompressionMessage, stderr } = await exec(
      `bzip2 -dc1 ${COMPRESSED_DB_FILEPATH} > ${decompressionFilePath}`
    );

    if (stderr) {
      throw new Error(`Database Decompression Failed -> ${stderr}`);
    }

    const databaseFileSize = fs.statSync(decompressionFilePath).size / (1024 * 1024);

    Logger.info(
      `Database Decompression Complete. Database File Size: ${databaseFileSize}MB`,
      { databaseDecompressionMessage }
    );

  } catch (error) {
    Logger.error(error, 'Error when Decompressing Database File');
    throw error;
  }
};

const readInFileToKnex = async (
  knex,
  setKnex,
  setDataIsLoadedIn,
  databaseFileExists,
  lessStorageMoreDowntime,
  Logger
) => {
  if (!lessStorageMoreDowntime){
    Logger.info(
      `Deleting Old Database after Database Decompression. Searching will be disabled for a moment.`
    );

    if (knex && knex.destroy) {
      setDataIsLoadedIn(false);
      await knex.destroy();
    }

    const newdatabaseFileExists = fs.existsSync(TEMP_DB_DECOMPRESSION_FILEPATH);

    if (databaseFileExists && newdatabaseFileExists) fs.unlinkSync(FINAL_DB_DECOMPRESSION_FILEPATH);

    if (newdatabaseFileExists)
      fs.renameSync(TEMP_DB_DECOMPRESSION_FILEPATH, FINAL_DB_DECOMPRESSION_FILEPATH);
  }

  const numberOfDatabaseRecords = await new Promise((resolve, reject) =>
    setKnex(
      require('knex')({
        client: 'sqlite3',
        connection: {
          filename: FINAL_DB_DECOMPRESSION_FILEPATH
        },
        pool: {
          afterCreate: async (conn, cb) => {
            try {
              const { count } = await conn('data').count('id').first();

              cb(null, conn);

              resolve(count);
            } catch (error) {
              Logger.error(error, 'Failed to Load Database')
              reject(error);
            }
          }
        }
      })
    )
  );
  
  setDataIsLoadedIn(true);

  Logger.info(`Loaded in Database and Searching is now Enabled. ${numberOfDatabaseRecords} Records Found.`);
};

const millisToHoursMinutesAndSeconds = (millis) => {
  let remainingMillis = millis; 
  const seconds = Math.floor((remainingMillis / 1000) % 60);
  remainingMillis -= seconds * 1000;

  const minutes = Math.floor((remainingMillis / 60000) % 60);
  remainingMillis -= minutes * 60000;
    
  const hours = Math.floor(remainingMillis / 3600000);

  return `${
    hours ? `${hours} hours, ` : ''
  }${
    minutes ? `${minutes} minutes, ` : ''
  }${
    seconds ? `${seconds} seconds` : ''
  }`;
};

module.exports = refreshInternetDb;
