const config = require('../../config/config');

const { getLocalStorageProperty } = require('./localStorage');

const { getFileSizeInGB } = require('../dataTransformations');
const { FINAL_DB_DECOMPRESSION_FILEPATH } = require('../constants');


const shouldntRunDbRefresh = async (knex, setKnex, Logger) => {
  const databaseFileHasContent = !!getFileSizeInGB(FINAL_DB_DECOMPRESSION_FILEPATH);
  const databaseFinishedReformatting = getLocalStorageProperty('databaseReformatted');

  const shouldSetupAndCheckKnex =
    databaseFileHasContent && databaseFinishedReformatting;

  if (shouldSetupAndCheckKnex) {
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

    const allNeededTablesExist =
      (await tableExistsAndHasContent(_knex, 'ips')) &&
      (await tableExistsAndHasContent(_knex, 'domains')) &&
      (await tableExistsAndHasContent(_knex, 'ips_domains'));

    if (!allNeededTablesExist) return false;

    setKnex(_knex);
    
    const userSetNeverUpdate = config.shodanDataRefreshTime === 'never-update';

    return userSetNeverUpdate;
  }
  return false;
};

const tableExistsAndHasContent = async (_knex, tableName) => {
  const dataTableExists = await _knex.schema.hasTable(tableName);
  const dataTableHasContent =
    dataTableExists && !!(await _knex.raw(`SELECT * FROM ${tableName} LIMIT 2`)).length;

  return dataTableHasContent;
};

module.exports = shouldntRunDbRefresh;
