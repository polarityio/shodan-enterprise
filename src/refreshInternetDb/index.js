const { millisToHoursMinutesAndSeconds } = require('../dataTransformations');

const shouldntRunDbRefresh = require('./shouldntRunDbRefresh');
const checkForEnoughDiskSpace = require('./checkForEnoughDiskSpace');
const getDownloadLink = require('./getDownloadLink');
const { shouldDownloadAndDecompress } = require('./fileChecks');
const downloadCompressedDatabase = require('./downloadCompressedDatabase');
const {
  decompressDatabase,
  decompressPreformattedDatabase
} = require('./decompressDatabase');
const readInFileToDbClient = require('./readInFileToDbClient');
const config = require('../../config/config');


const refreshInternetDb =
  (
    knex,
    setKnex,
    { requestWithDefaults, requestDefaults },
    Logger
  ) =>
  async () => {
    Logger.info('Starting Getting Database');
    const startTime = new Date();

    if (config.usePreformattedDatabase)
      await decompressPreformattedDatabase(knex, setKnex, Logger);

    if(await shouldntRunDbRefresh(knex, setKnex, Logger)) {
      Logger.info('Finished Getting Database');
      const endTime = new Date();
      const loadTime = millisToHoursMinutesAndSeconds(endTime - startTime);
      
      Logger.info(`Refreshing Database Complete. Load Time: ${loadTime}`);
      return;
    }

    const downloadLink = await getDownloadLink(requestWithDefaults, Logger);

    if (shouldDownloadAndDecompress(downloadLink)) {
      Logger.info(
        'Downloading and Decompressing Entire Database. This could take a some time.'
      );

      await checkForEnoughDiskSpace(Logger);

      await downloadCompressedDatabase(downloadLink, requestDefaults, Logger);

      await decompressDatabase(knex, setKnex, Logger);
    }

    await checkForEnoughDiskSpace(Logger);

    await readInFileToDbClient(knex, setKnex, Logger);

    const endTime = new Date();
    const loadTime = millisToHoursMinutesAndSeconds(endTime - startTime);

    Logger.info(`Refreshing Database Complete. Load Time: ${loadTime}`);
  };

module.exports = refreshInternetDb;
