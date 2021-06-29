const { flow, chunk, map, keys, flatMap } = require('lodash/fp');

const { getFileSizeInGB } = require('../dataTransformations');
const {
  FINAL_DB_DECOMPRESSION_FILEPATH,
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
  SQL_CREATE_INDICES
} = require('../constants');

const { getLocalStorageProperty, setLocalStorageProperty } = require('./localStorage');

let _newBatchSizeForPrimaryDomainLessening = 0;

const readInFileToDbClient = async (knex, setKnex, Logger) => {
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

  const numberOfDatabaseRecords = await reformatDatabaseForSearching(_knex, Logger);

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

  Logger.trace('Reformatting Database for Searching');

  await createSchema(_knex, Logger);

  const { 'count(`id`)': totalRows } = await _knex('ips').count('id').first();

  let count = 0;
  while (count < totalRows) {
    count = await reformatDatabaseChunk(count, totalRows, _knex, Logger);
  }

  await _knex.raw(SQL_CREATE_INDICES);
  await _knex.raw('PRAGMA incremental_vacuum;');

  setLocalStorageProperty('databaseReformatted', true);

  Logger.trace('Finished Reformatting Database for Searching');

  return totalRows;
};

const createSchema = async (_knex, Logger) => {
  _knex.raw('PRAGMA auto_vacuum = INCREMENTAL;');

  const dataTableExists = await _knex.schema.hasTable('data');
  const dataTableHasContent =
    dataTableExists && (await _knex.raw('SELECT ip FROM data LIMIT 2')).length;
  if (dataTableExists && dataTableHasContent) {
    await _knex.raw(SQL_DROP_TABLE('ips'));
    await _knex.raw(SQL_CREATE_IPS_TABLE);
    
    Logger.trace('Starting to create new IP Table');
    try {
      await _knex.raw(SQL_ADD_DATA_TO_IPS);
      Logger.trace('Created new IP Table. Deleting Data Table.');
      await _knex.raw(SQL_DROP_TABLE('data'));
    } catch (error) {
      Logger.error(error, 'Error on IP Table Insert');
      throw error;
    }
  }

  await _knex.raw(SQL_DROP_TABLE('domains'));
  await _knex.raw(SQL_DROP_TABLE('ips_domains'));
  await _knex.raw(SQL_CREATE_DOMAINS_TABLE);
  await _knex.raw(SQL_CREATE_IPS_DOMAINS_RELATIONAL_TABLE);

  await _knex.raw('PRAGMA incremental_vacuum;');
};

const reformatDatabaseChunk = async (counter, totalRows, _knex, Logger) => {
  let fullDomains = { length: MAX_HOSTNAME_BATCH_SIZE + 1 },
    rowBatchSize =
      DEFAULT_ROW_BATCH_SIZE +
      DEFAULT_ROW_BATCH_SIZE * 0.2 +
      DEFAULT_ROW_BATCH_SIZE * 0.05;

  if(_newBatchSizeForPrimaryDomainLessening) {
    rowBatchSize = _newBatchSizeForPrimaryDomainLessening;
    fullDomains = await _knex.raw(SQL_SPLIT_HOSTNAME_COLUMN(rowBatchSize, counter));
    _newBatchSizeForPrimaryDomainLessening = 0;
  } else {
    while (
      !(rowBatchSize === MAX_ROW_BATCH_SIZE && fullDomains.length <= MAX_HOSTNAME_BATCH_SIZE) && 
      !(fullDomains.length >= MIN_HOSTNAME_BATCH_SIZE && fullDomains.length <= MAX_HOSTNAME_BATCH_SIZE) 
    ) {
      rowBatchSize =
        fullDomains.length > MAX_HOSTNAME_BATCH_SIZE
          ? Math.round(rowBatchSize - rowBatchSize * 0.2)
          : Math.min(MAX_ROW_BATCH_SIZE, rowBatchSize * 2);

      fullDomains = null;
      fullDomains = await _knex.raw(SQL_SPLIT_HOSTNAME_COLUMN(rowBatchSize, counter));
    }
  }

  Logger.trace(`Reformatting ${rowBatchSize} records at position ${counter}.`);

  let groupedFullDomains = fullDomains.reduce(function (rv, x) {
    var v = /[^.]*\.[^.]{2,3}(?:\.[^.]{2,3})?$/.exec(x.domain);
    v = v && v[0];
    rv[v] = rv[v] || [];
    rv[v].push(x.ip_id);
    return rv;
  }, {});
  fullDomains = null;

  if (Object.keys(groupedFullDomains).length > MAX_PRIMARY_DOMAINS) {
    _newBatchSizeForPrimaryDomainLessening = Math.round(rowBatchSize - rowBatchSize * 0.3); 
    return counter;
  }

  await Promise.all(
    flow(
      keys,
      chunk(5000),
      flatMap(map(async (domain) => {
        const [domain_id] = await _knex('domains')
          .returning('id')
          .insert({ domain })
          .onConflict('domain')
          .ignore();

        await _knex.batchInsert(
          'ips_domains',
          map((ip_id) => ({ ip_id, domain_id }), groupedFullDomains[domain]),
          500
        );
      }))
    )(groupedFullDomains)
  );

  await _knex.raw('PRAGMA incremental_vacuum;');

  return Math.min(counter + rowBatchSize, totalRows);
};

module.exports = readInFileToDbClient;
