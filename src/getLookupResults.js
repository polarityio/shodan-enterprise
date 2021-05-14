const fp = require('lodash/fp');

const { splitOutIgnoredIps } = require('./dataTransformations');
const createLookupResults = require('./createLookupResults');

const getLookupResults = async (entities,  Logger) => {
  const { entitiesPartition, ignoredIpLookupResults } = splitOutIgnoredIps(entities);

  const foundEntities = await _getFoundEntities(
    entitiesPartition,
    Logger
  );

  const lookupResults = createLookupResults(foundEntities, Logger);

  Logger.trace({ lookupResults, foundEntities }, 'Lookup Results');

  return lookupResults.concat(ignoredIpLookupResults);
};


const _getFoundEntities = async (
  entitiesPartition,
  Logger
) =>
  Promise.all(
    fp.map(async (entity) => {
      //TODO
    }, entitiesPartition)
  );


module.exports = {
  getLookupResults
};
