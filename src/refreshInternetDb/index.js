const { millisToHoursMinutesAndSeconds } = require('../dataTransformations');

const getDownloadLink = require('./getDownloadLink');
const { shouldDownloadAndDecompress } = require('./fileChecks');
const downloadCompressedDatabase = require('./downloadCompressedDatabase');
const decompressDatabase = require('./decompressDatabase');
const readInFileToDbClient = require('./readInFileToDbClient');


const refreshInternetDb =
  (
    knex,
    setKnex,
    { requestWithDefaults, requestDefaults },
    Logger
  ) =>
  async () => {
    Logger.info('Starting Database Refresh');

    const startTime = new Date();

    const downloadLink = await getDownloadLink(requestWithDefaults, Logger);

    if (shouldDownloadAndDecompress(downloadLink)) {
      Logger.info(
        'Downloading and Decompressing Entire Database. This could take a some time.'
      );

      await downloadCompressedDatabase(downloadLink, requestDefaults, Logger);

      await decompressDatabase(knex, setKnex, Logger);
    }

    await readInFileToDbClient(knex, setKnex, Logger);

    const endTime = new Date();
    const loadTime = millisToHoursMinutesAndSeconds(endTime - startTime);

    Logger.info(`Refreshing Database Complete. Load Time: ${loadTime}`);
  };

module.exports = refreshInternetDb;
