const fp = require('lodash/fp');
const fs = require('fs');
const request = require('request');
const config = require('../config/config');
const util = require('util');
const exec = util.promisify(require('child_process').exec);

const COMPRESSED_DB_FILEPATH = './data/new-internetdb.sqlite.bz2';
const FINAL_DB_DECOMPRESSION_FILEPATH = './data/internetdb.sqlite';
const TEMP_DB_DECOMPRESSION_FILEPATH = './data/new-internetdb.sqlite';
const LOCAL_STORAGE_FILEPATH = './data/local-storage.json';

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

    const shodanEnterpriseApiKey =
      fp.get('shodanEnterpriseApiKey', config) || process.env.SHODAN_ENTERPRISE_API_KEY;
      
    if (!shodanEnterpriseApiKey) {
      throw new Error('Shodan Enterprise API Key not set in config.js');
    }

    const lessStorageMoreDowntime = fp.get('lessStorageMoreDowntime', config);


    const databaseFileExists = fs.existsSync(FINAL_DB_DECOMPRESSION_FILEPATH);

    const downloadLink = await getDownloadLink(
      shodanEnterpriseApiKey,
      requestWithDefaults,
      Logger
    );

    if (shouldDownloadAndDecompress(downloadLink, databaseFileExists)) {
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
    const loadTime = millisToHoursMinutesAndSeconds(endTime - startTime, Logger);

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

const shouldDownloadAndDecompress = (downloadLink, databaseFileExists) => {
  const oldDownloadLink = getLocalStorageProperty('oldDownloadLink');

  const downloadLinkHasntChanged = downloadLink === oldDownloadLink;
  const databaseHasSize = !!getFileSizeInGB(FINAL_DB_DECOMPRESSION_FILEPATH);
  
  return !(
    downloadLinkHasntChanged &&
    databaseFileExists &&
    databaseHasSize
  );
};

const downloadFile = async (downloadLink, requestDefaults, Logger) => {
  try {
    Logger.trace('Starting Compressed Database Download');
    const oldDownloadLink = getLocalStorageProperty('oldDownloadLink');

    const compressedDatabaseExists = fs.existsSync(COMPRESSED_DB_FILEPATH);

    if (downloadLink === oldDownloadLink && compressedDatabaseExists) {
      Logger.trace(
        'Compressed Database File Exists and is up to date.  Skipping Compressed Database Download.'
      );
      return;
    }

    setLocalStorageProperty('oldDownloadLink', downloadLink);

    if (compressedDatabaseExists)
      fs.unlinkSync(COMPRESSED_DB_FILEPATH);

    const file = fs.createWriteStream(COMPRESSED_DB_FILEPATH);

    await new Promise((resolve, reject) =>
      request({ ...requestDefaults, uri: downloadLink })
        .pipe(file)
        .on('finish', resolve)
        .on('error', reject)
    );

    const compressedDatabaseFileSize = getFileSizeInGB(COMPRESSED_DB_FILEPATH);

    Logger.info(
      `Compressed Database Download Complete. File Size: ${compressedDatabaseFileSize}GB`
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

    let decompressionFilePath = TEMP_DB_DECOMPRESSION_FILEPATH;

    if (lessStorageMoreDowntime) {
      Logger.info(
        'Deleting Current Database before Decompression. Searching will be disabled during this time.'
      );

      decompressionFilePath = FINAL_DB_DECOMPRESSION_FILEPATH;

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

const readInFileToKnex = async (
  knex,
  setKnex,
  setDataIsLoadedIn,
  databaseFileExists,
  lessStorageMoreDowntime,
  Logger
) => {
  Logger.trace('Started Loading in Database');

  if (!lessStorageMoreDowntime) {
    Logger.info(
      `Deleting Old Database after Database Decompression. Searching will be disabled for a moment.`
    );

    const newDatabaseFileExists = fs.existsSync(TEMP_DB_DECOMPRESSION_FILEPATH);

    if (databaseFileExists && newDatabaseFileExists)
      fs.unlinkSync(FINAL_DB_DECOMPRESSION_FILEPATH);

    if (newDatabaseFileExists)
      fs.renameSync(TEMP_DB_DECOMPRESSION_FILEPATH, FINAL_DB_DECOMPRESSION_FILEPATH);
  }

  if (knex && knex.destroy) {
    setDataIsLoadedIn(false);
    await knex.destroy();
  }

  const _knex = await require('knex')({
    client: 'sqlite3',
    connection: {
      filename: FINAL_DB_DECOMPRESSION_FILEPATH
    }
  });

  const enableDomainAndCveSearching = fp.get('enableDomainAndCveSearching', config);
  const settingChanged =
    enableDomainAndCveSearching !==
    getLocalStorageProperty('previousEnableDomainAndCveSearching');

  if (settingChanged) {
    Logger.info('Running Indexing of Database for faster searching.');
    await _knex.raw('DROP INDEX IF EXISTS data_ip_idx;');
    await _knex.raw('DROP TABLE IF EXISTS data_fts;');

    if (enableDomainAndCveSearching) {
      await _knex.raw('CREATE VIRTUAL TABLE data_fts USING fts4(ip TEXT, ports TEXT, tags TEXT, cpes TEXT, vulns TEXT, hostnames TEXT);');
      await _knex.raw('INSERT INTO data_fts SELECT ip, ports, tags, cpes, vulns, hostnames FROM data;');
    } else {
      await _knex.raw('CREATE UNIQUE INDEX data_ip_idx ON data(ip);');
    }
    getLocalStorageProperty(
      'previousEnableDomainAndCveSearching',
      enableDomainAndCveSearching
    );
  }
  
  let numberOfDatabaseRecords;
  if (fp.get('showNumberOfDatabaseRecords', config)) {
    const { 'count(`ip`)': count } = await _knex('data').count('ip').first();
    numberOfDatabaseRecords = count;
  }

  setKnex(_knex);

  setDataIsLoadedIn(true);
  
  const databaseFileSize = getFileSizeInGB(FINAL_DB_DECOMPRESSION_FILEPATH);

  Logger.info(
    'Loaded in Database. Searching is now Enabled.' +
      (databaseFileSize ? ` Database File Size After Indexing ${databaseFileSize}GB` : '') +
      (numberOfDatabaseRecords ? ` ${numberOfDatabaseRecords} Records Found.` : '')
  );
};

const getFileSizeInGB = (filepath) =>
  Math.floor((fs.statSync(filepath).size / 1073741824) * 1000) / 1000;


const millisToHoursMinutesAndSeconds = (millis, Logger) => {
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
  }${
    !hours && !minutes && !seconds ? `${millis}ms` : ''
  }`;
};

const getLocalStorageProperty = (propertyName) => {
  const localStorage = fs.existsSync(LOCAL_STORAGE_FILEPATH)
    ? JSON.parse(fs.readFileSync(LOCAL_STORAGE_FILEPATH, 'utf8'))
    : {};

  return localStorage[propertyName];
};

const setLocalStorageProperty = (propertyName, newValue) => {
  const localStorage = fs.existsSync(LOCAL_STORAGE_FILEPATH)
    ? JSON.parse(fs.readFileSync(LOCAL_STORAGE_FILEPATH, 'utf8'))
    : {};

  localStorage[propertyName] = newValue;

  fs.writeFileSync(LOCAL_STORAGE_FILEPATH, JSON.stringify(localStorage));

  return localStorage;
};

module.exports = refreshInternetDb;
