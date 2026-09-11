--- catalog creation

CREATE CATALOG IF NOT EXISTS retail
MANAGED LOCATION 'abfss://retail@storage_account.dfs.core.windows.net/'
COMMENT 'This is the main catalog for retail demo project';

--- schema creation

CREATE SCHEMA IF NOT EXISTS retail.landing;

CREATE SCHEMA IF NOT EXISTS retail.bronze
MANAGED LOCATION 'abfss://retail@storage_account.dfs.core.windows.net/bronze';

CREATE SCHEMA IF NOT EXISTS retail.silver
MANAGED LOCATION 'abfss://retail@storage_account.dfs.core.windows.net/silver';

CREATE SCHEMA IF NOT EXISTS retail.gold
MANAGED LOCATION 'abfss://retail@storage_account.dfs.core.windows.net/gold';
