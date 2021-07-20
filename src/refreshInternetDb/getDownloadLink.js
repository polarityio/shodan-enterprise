const fp = require('lodash/fp');

const config = require('../../config/config');


const getDownloadLink = async (requestWithDefaults, Logger) => {
  const shodanEnterpriseApiKey =
    fp.get('shodanEnterpriseApiKey', config) || process.env.SHODAN_ENTERPRISE_API_KEY;

  if (!shodanEnterpriseApiKey) {
    throw new Error('Shodan Enterprise API Key not set in config.js');
  }

  Logger.trace('Getting Database Download Link...');

  const downloadLinksResult = await requestWithDefaults({
    url: 'https://api.shodan.io/shodan/data/internetdb',
    qs: { key: shodanEnterpriseApiKey },
    json: true
  });

  const downloadLink = fp.flow(
    fp.get('body'),
    fp.find(fp.flow(fp.get('name'), fp.includes('sqlite'))),
    fp.get('url')
  )(downloadLinksResult);

  Logger.trace(`Database Download Link: ${downloadLink}`);

  return downloadLink;
};

module.exports = getDownloadLink;
