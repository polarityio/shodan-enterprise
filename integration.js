const fp = require('lodash/fp');

const createRequestWithDefaults = require('./src/createRequestWithDefaults');
const schedule = require('node-schedule');
const config = require('./config/config');

const { getLookupResults } = require('./src/getLookupResults');
const refreshInternetDb = require('./src/refreshInternetDb');

let Logger;
let requestWithDefaults;
let job;
let knex;
let dataIsLoadedIn;

const setDataIsLoadedIn = (value) => {
  dataIsLoadedIn = value;
};

const setKnex = (value) => {
  knex = value;
};

const startup = async (logger) => {
  Logger = logger;
  
  try {
    const { requestWithDefaults: _requestWithDefaults, requestDefaults } =
      createRequestWithDefaults(Logger);

    requestWithDefaults = _requestWithDefaults;

    if (job) job.cancel();
    
    await refreshInternetDb(
      knex,
      setKnex,
      setDataIsLoadedIn,
      requestDefaults,
      requestWithDefaults,
      Logger
    )();

    const shodanDataRefreshTime = fp.get('shodanDataRefreshTime', config);

    job = schedule.scheduleJob(
      shodanDataRefreshTime,
      refreshInternetDb(
        knex,
        setKnex,
        setDataIsLoadedIn,
        requestDefaults,
        requestWithDefaults,
        Logger
      )
    );
  } catch (error) {
    Logger.error(error, 'Error in startup function');
    throw error;
  }
};

const doLookup = async (entities, options, cb) => {
  let lookupResults;
  try {
    lookupResults = await getLookupResults(entities, dataIsLoadedIn, knex, Logger);
  } catch (error) {
    Logger.error(error, 'Get Lookup Results Failed');
    return cb({
      detail: error.message.includes('Knex: Timeout')
        ? 'Too Many Entities Searched at Once'
        : 'Search Failed',
      err: JSON.parse(JSON.stringify(error, Object.getOwnPropertyNames(error)))
    });
  }

  Logger.trace({ lookupResults }, 'Lookup Results');
  cb(null, lookupResults);
};

module.exports = {
  doLookup,
  startup
};
