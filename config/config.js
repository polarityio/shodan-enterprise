module.exports = {
  name: 'Shodan Enterprise',
  acronym: 'SHO-E',
  description:
    "Polarity's Shodan Enterprise Integration gives users access to the Shodan data/internetdb " +
    'endpoint data containing network information about virtually all IPs on the Internet.',
  entityTypes: ['IPv4', 'IPv6', 'domain'],
  styles: ['./styles/styles.less'],
  defaultColor: 'light-pink',
  onDemandOnly: false,
  block: {
    component: {
      file: './components/block.js'
    },
    template: {
      file: './templates/block.hbs'
    }
  },
  request: {
    cert: '',
    key: '',
    passphrase: '',
    ca: '',
    proxy: '',
    rejectUnauthorized: true
  },
  logging: {
    level: 'info' //trace, debug, info, warn, error, fatal
  },
  options: [
    {
      key: 'maxResults',
      name: 'Max Number Of Results',
      description:
        'The maximum number of results we will return from the internetdb data set when you search.',
      default: 30,
      type: 'number',
      userCanEdit: true,
      adminOnly: false
    }
  ],
  /**
   * Shodan Enterprise Api Key:
   *  Your API Key used to access the '/data/internetdb' endpoint on the Shodan API
   *  If you would prefer to set this in an environment variable instead, use SHODAN_ENTERPRISE_API_KEY as
   *  the variable name.  The value of the property in the `config.js` will be prioritized
   *  over the environment variable.
   */
  shodanEnterpriseApiKey: '',
  /**
   * Shodan Data Refresh Time:
   *  How often/When to refresh the local data source with the up to date data from the
   *  Shodan API.  This is outline in Cron Format and is defaulted to the first of
   *  every month at midnight UTC. Helpful Resources: https://crontab.guru/.
   * '* * * * * *'
   *  ┬ ┬ ┬ ┬ ┬ ┬
   *  │ │ │ │ │ └ day of week (0 - 7) (0 or 7 is Sun)
   *  │ │ │ │ └── month (1 - 12)
   *  │ │ │ └──── day of month (1 - 31)
   *  │ │ └────── hour (0 - 23)
   *  │ └──────── minute (0 - 59)
   *  └────────── second (0 - 59, OPTIONAL)
   */
  // '42 * * * *' -> Execute when the minute is 42 (e.g. 19:42, 20:42, etc.).
  // '*/5 * * * *' -> Execute every 5th minute
  // '0 0 1 * *' -> Execute at 00:00 on day-of-month 1.
  shodanDataRefreshTime: '0 0 1 * *',
  /**
   * Less Storage More Downtime:
   *  If true, this setting will half the total data storage requirements during the
   *  Refreshing process of your local Database. With this setting to false, the total
   *  file storage requirements can at times be in excess of 60-120GB.
   *
   *  This being set to true, however, will make the integration no longer work for the
   *  entire database download and decompression time which could possibly be more than 30
   *  minutes.  If you set this to true, we would recommend you set your
   *  'shodanDataRefreshTime' config property to a time of the day where users are not
   *  typically using the integration.
   */
  lessStorageMoreDowntime: true,
  /**
   * Minimize End Database Size:
   *  If true, this setting will double the total data storage requirements during the
   *  database reformatting process (from ~45GBs upwards to ~90+GBs), but after the 
   *  reformatting process will almost half the amount of storage required for the 
   *  database file (from ~45GBs to ~27GBs) and improve search speeds slightly. 
   */
  minimizeEndDatabaseSize: true
};
