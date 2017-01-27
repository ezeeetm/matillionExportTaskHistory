# matillionExportTaskHistory
extracts all groups, project names, and task histories from Matillion ETL, and uploads them to S3.

Matillion keeps its logs in a local mongoDB store, which is not exposed. However, we are able to extract the groups and projects via the Matillion API, and then combine those as query string params in a second API call to obtain the specific project's task history. Those task histories can then be archived offline (e.g., S3).
