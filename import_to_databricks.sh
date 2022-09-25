databricks fs cp ./data/ dbfs:/tmp/datalake/ --overwrite --recursive
echo "Uploaded data to dbfs under path: dbfs:/tmp/datalake/";