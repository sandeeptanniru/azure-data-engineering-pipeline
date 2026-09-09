--- catalog creation

CREATE CATALOG IF NOT EXISTS retail_catalog
MANAGED LOCATION 'abfss://container@stoarge_account.dfs.core.windows.net/'
COMMENT 'This is the main catalog for retail demo project';

--- schema creation

CREATE SCHEMA IF NOT EXISTS retail_catalog.landing;

CREATE SCHEMA IF NOT EXISTS retail_catalog.bronze
MANAGED LOCATION 'abfss://container@stoarge_account.dfs.core.windows.net/bronze';

CREATE SCHEMA IF NOT EXISTS retail_catalog.curated
MANAGED LOCATION 'abfss://container@stoarge_account.dfs.core.windows.net/sliver';

CREATE SCHEMA IF NOT EXISTS retail_catalog.analytics
MANAGED LOCATION 'abfss://container@stoarge_account.dfs.core.windows.net/gold';
