const fs = require('fs');
const { flow, chunk, map, keys, flatMap } = require('lodash/fp');

const { getFileSizeInGB } = require('../dataTransformations');
const config = require('../../config/config');

const {
  FINAL_DB_DECOMPRESSION_FILEPATH,
  TEMP_DB_DECOMPRESSION_FILEPATH,
  MAX_ROW_BATCH_SIZE,
  DEFAULT_ROW_BATCH_SIZE,
  MAX_HOSTNAME_BATCH_SIZE,
  MIN_HOSTNAME_BATCH_SIZE,
  MAX_PRIMARY_DOMAINS,
  SQL_SPLIT_HOSTNAME_COLUMN,
  SQL_DROP_TABLE,
  SQL_CREATE_IPS_TABLE,
  SQL_ADD_DATA_TO_IPS,
  SQL_CREATE_DOMAINS_TABLE,
  SQL_CREATE_IPS_DOMAINS_RELATIONAL_TABLE,
  SQL_CREATE_INDICES_IF_NOT_EXISTS
} = require('../constants');

const { getLocalStorageProperty, setLocalStorageProperty } = require('./localStorage');

let _newBatchSizeForPrimaryDomainLessening = 0;

const readInFileToDbClient = async (knex, setKnex, Logger) => {
  Logger.info('Started Loading in Database');

  let _knex = await require('knex')({
    client: 'sqlite3',
    connection: {
      filename: config.lessStorageMoreDowntime
        ? FINAL_DB_DECOMPRESSION_FILEPATH
        : TEMP_DB_DECOMPRESSION_FILEPATH
    }
  });

  const numberOfDatabaseRecords = await reformatDatabaseForSearching(_knex, Logger);

  if (!config.lessStorageMoreDowntime) {
    Logger.info(
      'Deleting Old Database. Searching will be disabled for a moment.'
    );

    if (knex && knex.destroy) {
      setKnex(undefined);
      await knex.destroy();
    }

    deleteOldDbAndRenameTempFile();

    _knex = await require('knex')({
      client: 'sqlite3',
      connection: {
        filename: FINAL_DB_DECOMPRESSION_FILEPATH
      }
    });
  }

  setKnex(_knex);

  const databaseFileSize = getFileSizeInGB(FINAL_DB_DECOMPRESSION_FILEPATH);

  Logger.info(
    'Loaded in Database. Searching is now Enabled.' +
      (databaseFileSize ? ` Database File Size After Indexing ${databaseFileSize}GB.` : '') +
      (numberOfDatabaseRecords ? ` ${numberOfDatabaseRecords} Records Found.` : '')
  );
};

const reformatDatabaseForSearching = async (_knex, Logger) => {
  if (getLocalStorageProperty('databaseReformatted')) return;

  Logger.info('Reformatting Database for Searching. This will take at least 4 hours.');

  await createSchema(_knex, Logger);

  const { 'count(`id`)': totalRows } = await _knex('ips').count('id').first();

  Logger.trace(`Beginning to Process Records. Total Records to Process: ${totalRows}`);
  let count = 0;
  while (count < totalRows) {
    count = await reformatDatabaseChunk(count, totalRows, _knex, Logger);
  }

  if (config.minimizeEndDatabaseSize) await _knex.raw('VACUUM;');

  await _knex.raw(SQL_CREATE_INDICES_IF_NOT_EXISTS);

  setLocalStorageProperty('databaseReformatted', true);

  Logger.info('Finished Reformatting Database for Searching');

  return totalRows;
};;

const createSchema = async (_knex, Logger) => {
  await _knex.raw('PRAGMA journal_mode=OFF');

  const dataTableExists = await _knex.schema.hasTable('data');
  const dataTableHasContent =
    dataTableExists && (await _knex.raw('SELECT ip FROM data LIMIT 2')).length;
  if (dataTableExists && dataTableHasContent) {
    Logger.info('Starting upload to newly formatted IP Address Table');
    try {
      setLocalStorageProperty('dataHasBeenLoadedIntoIpsTable', false);
      await _knex.raw(SQL_DROP_TABLE('ips'));
      await _knex.raw(SQL_CREATE_IPS_TABLE);
      await _knex.raw(SQL_ADD_DATA_TO_IPS);
      Logger.info('Finished uploading to new IP Address Table. Deleting unformatted Data Table.');
      await _knex.raw(SQL_DROP_TABLE('data'));
      setLocalStorageProperty('dataHasBeenLoadedIntoIpsTable', true);
    } catch (error) {
      Logger.error(error, 'Error on IP Table Insert');
      throw error;
    }
  }

  await _knex.raw(SQL_DROP_TABLE('domains'));
  await _knex.raw(SQL_DROP_TABLE('ips_domains'));
  await _knex.raw(SQL_CREATE_DOMAINS_TABLE);
  await _knex.raw(SQL_CREATE_IPS_DOMAINS_RELATIONAL_TABLE);

  if (config.minimizeEndDatabaseSize) await _knex.raw('VACUUM;');
};

const reformatDatabaseChunk = async (counter, totalRows, _knex, Logger) => {
  let { fullDomainNamesWithIpIds, rowBatchSize } = await getFullDomainNamesWithIpIds(
    counter,
    _knex,
    Logger
  );

  Logger.trace(`Reformatting ${rowBatchSize} records at position ${counter}.`);

  let primaryDomainByIpIds = getPrimaryDomainByIpIds(fullDomainNamesWithIpIds);  
  fullDomainNamesWithIpIds = null;

  if (Object.keys(primaryDomainByIpIds).length > MAX_PRIMARY_DOMAINS) {
    _newBatchSizeForPrimaryDomainLessening = Math.round(rowBatchSize - rowBatchSize * 0.3); 
    return counter;
  }

  await insertPrimaryDomainsAndRelationships(primaryDomainByIpIds, _knex);
  primaryDomainByIpIds = null;
  
  return Math.min(counter + rowBatchSize, totalRows);
};


const getFullDomainNamesWithIpIds = async (counter, _knex, Logger) => {
  let fullDomainNamesWithIpIds = { length: MAX_HOSTNAME_BATCH_SIZE + 1 },
    rowBatchSize =
      DEFAULT_ROW_BATCH_SIZE +
      DEFAULT_ROW_BATCH_SIZE * 0.2 +
      DEFAULT_ROW_BATCH_SIZE * 0.05,
    batchSizeMaxAndHostnameSizeAcceptable,
    hostnameSizeInCorrectRange;

  if (_newBatchSizeForPrimaryDomainLessening) {
    rowBatchSize = _newBatchSizeForPrimaryDomainLessening;
    fullDomainNamesWithIpIds = await _knex.raw(
      SQL_SPLIT_HOSTNAME_COLUMN(rowBatchSize, counter)
    );
    _newBatchSizeForPrimaryDomainLessening = 0;
  } else {
    while (!batchSizeMaxAndHostnameSizeAcceptable && !hostnameSizeInCorrectRange) {
      rowBatchSize =
        fullDomainNamesWithIpIds.length > MAX_HOSTNAME_BATCH_SIZE
          ? Math.round(rowBatchSize - rowBatchSize * 0.2)
          : Math.min(MAX_ROW_BATCH_SIZE, rowBatchSize * 2);

      fullDomainNamesWithIpIds = null;
      fullDomainNamesWithIpIds = await _knex.raw(
        SQL_SPLIT_HOSTNAME_COLUMN(rowBatchSize, counter)
      );

      batchSizeMaxAndHostnameSizeAcceptable =
        rowBatchSize === MAX_ROW_BATCH_SIZE &&
        fullDomainNamesWithIpIds.length <= MAX_HOSTNAME_BATCH_SIZE;

      hostnameSizeInCorrectRange =
        fullDomainNamesWithIpIds.length >= MIN_HOSTNAME_BATCH_SIZE &&
        fullDomainNamesWithIpIds.length <= MAX_HOSTNAME_BATCH_SIZE;
    }
  }

  return { fullDomainNamesWithIpIds, rowBatchSize };
};

const roughPrimaryDomainRegex = /[^.]*\.[^.]{2,3}(?:\.[^.]{2,3})?$/;
const getPrimaryDomainByIpIds = (fullDomainNamesWithIpIds) =>
  fullDomainNamesWithIpIds.reduce(function (rv, x) {
    var v = roughPrimaryDomainRegex.exec(x.domain);
    v = v && v[0];
    rv[v] = rv[v] || [];
    rv[v].push(x.ip_id);
    return rv;
  }, {});

const insertPrimaryDomainsAndRelationships = async (primaryDomainByIpIds, _knex) =>
  Promise.all(
    flow(
      keys,
      chunk(5000),
      flatMap(
        map(async (domain) => {
          const [domain_id] = await _knex('domains')
            .returning('id')
            .insert({ domain })
            .onConflict('domain')
            .ignore();

          await _knex.batchInsert(
            'ips_domains',
            map((ip_id) => ({ ip_id, domain_id }), primaryDomainByIpIds[domain]),
            500
          );
        })
      )
    )(primaryDomainByIpIds)
  );

const deleteOldDbAndRenameTempFile = () => {
  const newDatabaseFileExists = fs.existsSync(TEMP_DB_DECOMPRESSION_FILEPATH);
  const databaseFileExists = fs.existsSync(FINAL_DB_DECOMPRESSION_FILEPATH);

  if (databaseFileExists && newDatabaseFileExists) {
    fs.unlinkSync(FINAL_DB_DECOMPRESSION_FILEPATH);
  }

  if (newDatabaseFileExists) {
    fs.renameSync(TEMP_DB_DECOMPRESSION_FILEPATH, FINAL_DB_DECOMPRESSION_FILEPATH);
  }
};

module.exports = readInFileToDbClient;
