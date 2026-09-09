--External Location 

CREAT EXTERNAL LOCATION IF NOT EXISTS '<external_location>'
URL 'abfss://container@storage_account.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL '<storage_credential>')
COMMENT 'EXTERNAL LOCATION FOR demo project'