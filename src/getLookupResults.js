const fp = require('lodash/fp');

const { splitOutIgnoredIps } = require('./dataTransformations');
const createLookupResults = require('./createLookupResults');

const getLookupResults = async (entities, options, dataIsLoadedIn, knex, Logger) => {
  if (!dataIsLoadedIn) {
    throw new Error(
      'Currently Refreshing Database.  Searching is not possible at this time.'
    );
  }

  const { entitiesPartition, ignoredIpLookupResults } = splitOutIgnoredIps(entities);

  const foundEntities = await _getFoundEntities(entitiesPartition, options, knex, Logger);

  const lookupResults = createLookupResults(foundEntities, Logger);

  return lookupResults.concat(ignoredIpLookupResults);
};

const _getFoundEntities = async (entitiesPartition, options, knex, Logger) =>
  Promise.all(
    fp.map(async (entity) => {
      let queryResult;
      if (entity.isDomain) {

        let query = `
          SELECT i.* 
          FROM ips i, domains d, ips_domains di 
          WHERE di.ip_id = i.id AND di.domain_id IN (SELECT id from domains where domain = '${entity.value}')
          LIMIT ${options.maxResults};
        `
        queryResult = await knex.raw(query);
      } else if (entity.isIP) {
        queryResult = await knex('data').select('*').where('ip', '=', entity.value);
      }

      return { entity, queryResult };
    }, entitiesPartition)
  );

module.exports = {
  getLookupResults
};
