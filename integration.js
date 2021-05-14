const fp = require('lodash/fp');

const reduce = require('lodash/fp/reduce').convert({ cap: false });
const createRequestWithDefaults = require('./src/createRequestWithDefaults');

const { handleError } = require('./src/handleError');
const { getLookupResults } = require('./src/getLookupResults');

let Logger;
let requestWithDefaults;
let startJob;
let job;
let validateOptionsStartedJob;
let collectionObjectsDB;
let collectionsDB;
let _options;

const startup = async (logger) => {
  Logger = logger;

  requestWithDefaults = createRequestWithDefaults(Logger);
  
  //TODO: Load in database
};

const doLookup = async (entities, options, cb) => {  
  let lookupResults; 
  try {

    lookupResults = await getLookupResults(
      entities,
      collectionsDB,
      collectionObjectsDB,
      Logger
    );
  } catch (error) {
    Logger.error(error, 'Get Lookup Results Failed');
    return cb(handleError(error));
  }

  Logger.trace({ lookupResults }, 'Lookup Results');
  cb(null, lookupResults);
};

const validateOptions = async (options, callback) => {
  const stringOptionsErrorMessages = {
    url: 'You must provide a valid Url.',
  };

  const stringValidationErrors = _validateStringOptions(
    stringOptionsErrorMessages,
    options
  );

  const urlError = fp.flow(fp.get('url.value'), fp.endsWith('/'))(options)
    ? [{ key: 'url', message: 'Your Url must not end with "/".' }]
    : [];

  callback(null, stringValidationErrors.concat(urlError));
};

const _validateStringOptions = (stringOptionsErrorMessages, options, otherErrors = []) =>
  reduce((agg, message, optionName) => {
    const isString = typeof options[optionName].value === 'string';
    const isEmptyString = isString && fp.isEmpty(options[optionName].value);

    return !isString || isEmptyString
      ? agg.concat({
          key: optionName,
          message
        })
      : agg;
  }, otherErrors)(stringOptionsErrorMessages);

module.exports = {
  doLookup,
  startup,
  validateOptions
};
