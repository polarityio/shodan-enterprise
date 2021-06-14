const fp = require('lodash/fp');

const { splitOutIgnoredIps } = require('./dataTransformations');
const createLookupResults = require('./createLookupResults');
const config = require('../config/config');

const ENTITY_TYPE_TO_COLUMN = {
  IPv4: 'ip',
  IPv6: 'ip',
  domain: 'hostnames',
  cve: 'vulns'
};

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
      if (fp.get('enableDomainAndCveSearching', config)) {
        const column = ENTITY_TYPE_TO_COLUMN[entity.type];

        const coreQuery = `SELECT * FROM data_fts WHERE ${column} MATCH '${entity.value}' LIMIT ${options.maxResults}`;
        const query = entity.value.includes('-')
          ? coreQuery
          : `SELECT * FROM (${coreQuery}) WHERE ${column} LIKE '%${entity.value}%'`;

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
