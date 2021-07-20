const _ = require('lodash');

const { IGNORED_IPS } = require('./constants');
const fs = require('fs');

const splitOutIgnoredIps = (_entitiesPartition) => {
  const { ignoredIPs, entitiesPartition } = _.groupBy(
    _entitiesPartition,
    ({ isIP, value }) =>
      !isIP || (isIP && !IGNORED_IPS.has(value)) ? 'entitiesPartition' : 'ignoredIPs'
  );

  return {
    entitiesPartition,
    ignoredIpLookupResults: _.map(ignoredIPs, (entity) => ({
      entity,
      data: null
    }))
  };
};

const getFileSizeInGB = (filepath) =>
  fs.existsSync(filepath)
    ? Math.floor((fs.statSync(filepath).size / 1073741824) * 1000) / 1000
    : 0;

const millisToHoursMinutesAndSeconds = (millis) => {
  let remainingMillis = millis;

  const seconds = Math.floor((remainingMillis / 1000) % 60);
  remainingMillis -= seconds * 1000;

  const minutes = Math.floor((remainingMillis / 60000) % 60);
  remainingMillis -= minutes * 60000;

  const hours = Math.floor(remainingMillis / 3600000);

  return (
    (hours ? `${hours} hours, ` : '') +
    (minutes ? `${minutes} minutes, ` : '') +
    (seconds ? `${seconds} seconds` : '') +
    (!hours && !minutes && !seconds ? `${millis}ms` : '')
  );
};

module.exports = {
  splitOutIgnoredIps,
  getFileSizeInGB,
  millisToHoursMinutesAndSeconds
};
