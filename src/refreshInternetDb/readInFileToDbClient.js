const fs = require('fs');

const {
  lessStorageMoreDowntime,
  enableDomainAndCveSearching,
  showNumberOfDatabaseRecords
} = require('../../config/config');

const { getFileSizeInGB } = require('../dataTransformations');
const {
  TEMP_DB_DECOMPRESSION_FILEPATH,
  FINAL_DB_DECOMPRESSION_FILEPATH
} = require('../constants');

const { getLocalStorageProperty, setLocalStorageProperty } = require('./localStorage');

const readInFileToDbClient = async (
  knex,
  setKnex,
  Logger
) => {
  Logger.trace('Started Loading in Database');

  if (knex && knex.destroy) {
    setKnex(undefined);
    await knex.destroy();
  }

  const _knex = await require('knex')({
    client: 'sqlite3',
    connection: {
      filename: FINAL_DB_DECOMPRESSION_FILEPATH
    }
  });

  const enableDomainAndCveSearchingSettingChanged =
    enableDomainAndCveSearching !==
    getLocalStorageProperty('previousEnableDomainAndCveSearchingSetting');

  if (enableDomainAndCveSearchingSettingChanged) {
    Logger.info('Running Indexing of Database for faster searching.');
    await _knex.raw('DROP INDEX IF EXISTS data_ip_idx;');
    await _knex.raw('DROP TABLE IF EXISTS data_fts;');

    if (enableDomainAndCveSearching) {
      await _knex.raw(
        'CREATE VIRTUAL TABLE data_fts USING fts4(ip TEXT, ports TEXT, tags TEXT, cpes TEXT, vulns TEXT, hostnames TEXT);'
      );
      await _knex.raw(
        'INSERT INTO data_fts SELECT ip, ports, tags, cpes, vulns, hostnames FROM data;'
      );
    } else {
      await _knex.raw('CREATE UNIQUE INDEX data_ip_idx ON data(ip);');
    }
    setLocalStorageProperty(
      'previousEnableDomainAndCveSearchingSetting',
      enableDomainAndCveSearching
    );
  }

  const { 'count(`ip`)': numberOfDatabaseRecords } = showNumberOfDatabaseRecords
    ? await _knex('data').count('ip').first()
    : {};

  setKnex(_knex);

  const databaseFileSize = getFileSizeInGB(FINAL_DB_DECOMPRESSION_FILEPATH);

  Logger.info(
    'Loaded in Database. Searching is now Enabled.' +
      (databaseFileSize ? ` Database File Size After Indexing ${databaseFileSize}GB.` : '') +
      (numberOfDatabaseRecords ? ` ${numberOfDatabaseRecords} Records Found.` : '')
  );
};

module.exports = readInFileToDbClient;