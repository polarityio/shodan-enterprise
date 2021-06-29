const IGNORED_IPS = new Set(['127.0.0.1', '255.255.255.255', '0.0.0.0']);

const COMPRESSED_DB_FILEPATH = './data/new-internetdb.sqlite.bz2';
const FINAL_DB_DECOMPRESSION_FILEPATH = './data/internetdb.sqlite';
const TEMP_DB_DECOMPRESSION_FILEPATH = './data/new-internetdb.sqlite';
const LOCAL_STORAGE_FILEPATH = './data/local-storage.json';
const MAX_ROW_BATCH_SIZE = 4000000;
const DEFAULT_ROW_BATCH_SIZE = 200000;
const MAX_HOSTNAME_BATCH_SIZE = 180000;
const MIN_HOSTNAME_BATCH_SIZE = 50000;

const SQL_SPLIT_HOSTNAME_COLUMN = (
  rowBatchSize,
  counter
) => `WITH split(id, domain, str) AS
      (SELECT id, '', hostnames||',' FROM (SELECT id, hostnames FROM ips LIMIT ${rowBatchSize} OFFSET ${counter}) UNION ALL SELECT id, substr(str, 0, instr(str, ',')), substr(str, instr(str, ',')+1) FROM split WHERE str!='') 
      SELECT id as ip_id, LOWER(domain) as domain FROM split WHERE domain!='' and domain is not null;`;

const SQL_DROP_TABLE = (tableName) => `DROP TABLE IF EXISTS ${tableName};`;

const SQL_CREATE_IPS_TABLE =
  'CREATE TABLE ips (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, ip TEXT NOT NULL UNIQUE, ports TEXT, tags TEXT, cpes TEXT, vulns TEXT, hostnames TEXT);';

const SQL_ADD_DATA_TO_IPS =
  'INSERT INTO ips SELECT NULL as id, ip, ports, tags, cpes, vulns, hostnames FROM data;';
  
const SQL_CREATE_DOMAINS_TABLE =
  'CREATE TABLE domains (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, domain TEXT NOT NULL UNIQUE);';

const SQL_CREATE_IPS_DOMAINS_RELATIONAL_TABLE =
  'CREATE TABLE ips_domains (ip_id INTEGER, domain_id INTEGER, FOREIGN KEY(ip_id) REFERENCES ips(id), FOREIGN KEY(domain_id) REFERENCES domains(id));';

const SQL_CREATE_INDICES = 
`CREATE INDEX IF NOT EXISTS ips_ip_index ON ips (ip);
  CREATE INDEX IF NOT EXISTS domains_domain_index ON domains (domain);
  CREATE INDEX IF NOT EXISTS ips_domains_ip_id_index ON ips_domains (ip_id);
  CREATE INDEX IF NOT EXISTS ips_domains_domain_id_index ON ips_domains (domain_id);`;

module.exports = {
  IGNORED_IPS,
  COMPRESSED_DB_FILEPATH,
  FINAL_DB_DECOMPRESSION_FILEPATH,
  TEMP_DB_DECOMPRESSION_FILEPATH,
  LOCAL_STORAGE_FILEPATH,
  MAX_ROW_BATCH_SIZE,
  DEFAULT_ROW_BATCH_SIZE,
  MAX_HOSTNAME_BATCH_SIZE,
  MIN_HOSTNAME_BATCH_SIZE,
  SQL_SPLIT_HOSTNAME_COLUMN,
  SQL_DROP_TABLE,
  SQL_CREATE_IPS_TABLE,
  SQL_ADD_DATA_TO_IPS,
  SQL_CREATE_DOMAINS_TABLE,
  SQL_CREATE_IPS_DOMAINS_RELATIONAL_TABLE,
  SQL_CREATE_INDICES
};
