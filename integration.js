const fp = require('lodash/fp');

const createRequestWithDefaults = require('./src/createRequestWithDefaults');
const schedule = require('node-schedule');
const { shodanDataRefreshTime } = require('./config/config');

const { getLookupResults } = require('./src/getLookupResults');
const refreshInternetDb = require('./src/refreshInternetDb/index');

let Logger;
let job;
let knex;
let dataIsLoadedIn;


const setKnex = (value) => {
  knex = value;
  dataIsLoadedIn = !!value;
};

const startup = async (logger) => {
  Logger = logger;
  
  try {
    const requestAndWithDefaults = createRequestWithDefaults(Logger);

    if (job) job.cancel();
    
    await refreshInternetDb(knex, setKnex, requestAndWithDefaults, Logger)();

    if (shodanDataRefreshTime !== 'never-update') {
      job = schedule.scheduleJob(
        shodanDataRefreshTime,
        refreshInternetDb(knex, setKnex, requestAndWithDefaults, Logger)
      );
    }
  } catch (error) {
    Logger.error(error, 'Error on Startup');
    throw error;
  }
};

const doLookup = async (entities, options, cb) => {
  let lookupResults;
  try {
    lookupResults = await getLookupResults(
      entities,
      options,
      dataIsLoadedIn,
      knex,
      Logger
    );
  } catch (error) {
    Logger.error(error, 'Get Lookup Results Failed');
    return cb({
      detail: 
        error.message.includes('Knex: Timeout') ?
          'Too Many Entities searched at once.  Try less at one time.' :
        error.message.includes('Currently Refreshing Database') ?
          error.message :
          'Searching Failed',
      err: JSON.parse(JSON.stringify(error, Object.getOwnPropertyNames(error)))
    });
  }

  Logger.trace({ lookupResults }, 'Lookup Results');
  cb(null, lookupResults);
};

const validateOptions = (userOptions, cb) => {
  const errors =
    userOptions.maxResults < 1
      ? [{ key: 'maxResults', message: 'Must be greater than 0' }]
      : [];

  cb(null, errors);
}

module.exports = {
  doLookup,
  validateOptions,
  startup
};
