const fp = require('lodash/fp');

const createLookupResults = (foundEntities, Logger) =>
  fp.flow(
    fp.map(({ entity, ...data }) =>
      true
        ? {
            entity,
            data: {
              summary: createSummary(),
              details: { data }
            }
          }
        : {
            entity,
            data: null
          }
    ),
    fp.compact
  )(foundEntities);

const createSummary = (collectionsWithFoundObjects) => {
  return [];
};

module.exports = createLookupResults;
