const { flow, chunk, map } = require('lodash/fp');
const { showNumberOfDatabaseRecords } = require('../../config/config');

const { getFileSizeInGB } = require('../dataTransformations');
const { FINAL_DB_DECOMPRESSION_FILEPATH } = require('../constants');

let DEFAULT_ROW_BATCH_SIZE = 100000;
let MAX_HOSTNAME_BATCH_SIZE = 1000000;

const { getLocalStorageProperty, setLocalStorageProperty } = require('./localStorage');

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
      (databaseFileSize
        ? ` Database File Size After Indexing ${databaseFileSize}GB.`
        : '') +
      (numberOfDatabaseRecords ? ` ${numberOfDatabaseRecords} Records Found.` : '')
  );
};

const reformatDatabaseForSearching = async (_knex, Logger) => {
  if (getLocalStorageProperty('databaseReformatted')) return;

  Logger.trace('Reformatting Database for Searching');

  await createSchema(_knex, Logger);

  const { 'count(`id`)': totalRows } = await _knex('ips').count('id').first();

  let count = 0;
  while (count <= totalRows) {
    count = await reformatDatabaseChunk(count, totalRows, _knex, Logger);
  }

  await dropColumn(_knex, 'domains', 'ip_ids');

  setLocalStorageProperty('databaseReformatted', true);

  Logger.trace('Finished Reformatting Database for Searching');

  return totalRows;
};

const createSchema = async (_knex, Logger) => {
  const dataTableExists = await _knex.schema.hasTable('data');
  const dataTableHasContent =
    dataTableExists && (await _knex.raw('SELECT ip FROM data LIMIT 2')).length;
  if (dataTableExists && dataTableHasContent) {
    await _knex.raw(`DROP TABLE IF EXISTS ips;`);
    await _knex.schema.createTable('ips', function (table) {
      table.increments('id').primary();
      table.string('ip').notNullable().unique().index();
      table.string('ports');
      table.string('tags');
      table.string('cpes');
      table.string('vulns');
      table.string('hostnames');
    });
    Logger.trace('Starting to create new IP Table');
    try {
      await _knex.raw(`INSERT INTO ips SELECT NULL as id, * FROM data;`);
      Logger.trace('Created new IP Table. Deleting Data Table.');
      await _knex.raw(`DROP TABLE IF EXISTS data;`);
    } catch (error) {
      Logger.error(error, 'error on insert');
      throw error;
    }
  }

  await _knex.raw(`DROP TABLE IF EXISTS domains;`);
  await _knex.raw(`DROP TABLE IF EXISTS ips_domains;`);

  await _knex.schema
    .createTable('domains', function (table) {
      table.increments('id').primary();
      table.string('domain').notNullable().unique().index();
      table.string('ip_ids');
    })
    .createTable('ips_domains', function (table) {
      table.increments('id').primary();
      table.integer('ip_id').references('id').inTable('ips').index();
      table.integer('domain_id').references('id').inTable('domains').index();
    });
};

const reformatDatabaseChunk = async (counter, totalRows, _knex, Logger) => {
  let fullDomains = { length: MAX_HOSTNAME_BATCH_SIZE + 1 },
    rowBatchSize = DEFAULT_ROW_BATCH_SIZE + DEFAULT_ROW_BATCH_SIZE * 0.2;
  while (fullDomains.length > MAX_HOSTNAME_BATCH_SIZE) {
    rowBatchSize = Math.round(rowBatchSize - rowBatchSize * 0.2);

    fullDomains = await _knex.raw(
      `WITH split(id, domain, str) AS
      (SELECT id, '', hostnames||',' FROM (SELECT id, hostnames FROM ips LIMIT ${rowBatchSize} OFFSET ${counter}) UNION ALL SELECT id, substr(str, 0, instr(str, ',')), substr(str, instr(str, ',')+1) FROM split WHERE str!='') 
      SELECT id as ip_id, domain FROM split WHERE domain!='' and domain is not null;`
    );
  }

  Logger.trace(`Reformatting ${rowBatchSize} records at position ${counter}.`);

  let groupedFullDomains = fullDomains.reduce(function (rv, x) {
    var v = /[^.]*\.[^.]{2,3}(?:\.[^.]{2,3})?$/.exec(x.domain);
    v = v && v[0];
    rv[v] = rv[v] || '';
    rv[v] += x.ip_id;
    rv[v] += ',';
    return rv;
  }, {});
  fullDomains = null;

  let domainsWithIpIds = Object.keys(groupedFullDomains).map((domain) => ({
    ip_ids: groupedFullDomains[domain].slice(0, -1),
    domain
  }));
  groupedFullDomains = null;

  await Promise.all(
    flow(
      chunk(5000),
      map((partition) =>
        _knex.transaction((trx) => {
          let queries = partition.map(async (row) =>
            _knex('domains').insert(row).onConflict('domain').merge().transacting(trx)
          );
          return Promise.all(queries).then(trx.commit).catch(trx.rollback);
        })
      )
    )(domainsWithIpIds)
  );
  domainsWithIpIds = null;

  await _knex.raw(
    `INSERT INTO ips_domains
      WITH split(id, ip_id, str) AS 
      (SELECT id, '', ip_ids||',' FROM domains UNION ALL SELECT id, substr(str, 0, instr(str, ',')), substr(str, instr(str, ',')+1) FROM split WHERE str!='') 
      SELECT NULL as id, CAST(ip_id AS INTEGER) as ip_id, id as domain_id FROM split WHERE ip_id!='' and ip_id is not null;`
  );
  await _knex.raw(`UPDATE domains SET ip_ids=''`);

  return Math.min(counter + rowBatchSize, totalRows);
};

const dropColumn = async (knex, tableName, columnName) => {
  // knex does not have a dropColumnIfExists
  await knex.schema.hasColumn(tableName, columnName).then((hasColumn) => {
    if (hasColumn) {
      return knex.schema.alterTable(tableName, (table) => {
        table.dropColumn(columnName);
      });
    } else {
      return null;
    }
  });
};
module.exports = readInFileToDbClient;
