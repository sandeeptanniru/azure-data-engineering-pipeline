--External Location 

CREATE EXTERNAL LOCATION IF NOT EXISTS `adb-xx-ext-location`
URL 'abfss://retail@storage_account.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `az-xx-xxx-storage-credential`)
COMMENT 'EXTERNAL LOCATION FOR az-databricks-container container'