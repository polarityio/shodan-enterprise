const fp = require('lodash/fp');

const createLookupResults = (foundEntities, Logger) =>
  fp.flow(
    fp.map(({ entity, queryResult }) => {
      let lookupResult;
      if (fp.size(queryResult)) {
        const formattedQueryResult = formatQueryResult(queryResult);
        lookupResult = {
          entity,
          data: {
            summary: createSummary(formattedQueryResult),
            details: formattedQueryResult
          }
        };
      } else {
        lookupResult = {
          entity,
          isVolatile: true,
          data: null
        };
      }
      return lookupResult;
    }),
    fp.compact
  )(foundEntities);

const createSummary = (queryResult) => [
  ...(fp.size(queryResult) > 1
    ? [`Results Found: ${fp.size(queryResult)}`]
    : ['Result Found']),
  ...fp.flow(fp.flatMap(fp.get('tags')), fp.uniq)(queryResult)
];

const formatQueryResult = fp.map(({ ip, ...otherColumns }) => ({
  ip,
  ...fp.mapValues(fp.flow(fp.split(','), fp.compact), otherColumns)
}));

module.exports = createLookupResults;
