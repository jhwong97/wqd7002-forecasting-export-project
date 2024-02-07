import logging
from airflow.exceptions import AirflowFailException
from google.cloud.exceptions import NotFound

# Define a function for uploading data to Google Storage Bucket    
def upload_to_bucket(storage_client,
                     bucket_name,
                     df_list):
    if df_list is None:
        logging.warning("Please ensure df_list has a value.")
    
    my_bucket = storage_client.bucket(bucket_name)
    # Check if the specified bucet_name exists or not
    if not my_bucket.exists(): # If the bucket does not exist
        try:
            logging.info(f'Bucket - {bucket_name} is not found.')
            logging.info(f"Creating {bucket_name} in progress ...")
            my_bucket.create() # Create the bucket with the specified name
            logging.info(f"SUCCESS: {bucket_name} has been created.")
            
        except Exception as e:
            logging.info(f"Error creating bucket: {e}")
            raise AirflowFailException('Failure of the task due to encountered error.')
                  
    else:
        logging.info(f"Bucket - {bucket_name} is found.")
   
    try:
        # To create a list to store the gsutil uri 
        gsutil_uri_list = []
        for item in df_list:
            # timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S ')
            # blob_name = item.name + " " + timestamp
            blob_name = item.name
            blob = my_bucket.blob(blob_name)
            logging.info(f"Uploading data to Google Storage Bucket in progress ...")
            blob.upload_from_string(item.to_csv(index=False), 'text/csv')
            logging.info(f'SUCCESS: {blob} has successfully uploaded to {my_bucket}.')
            gsutil_uri = f"gs://{bucket_name}/{blob_name}"
            gsutil_uri_list.append(gsutil_uri) 
        return gsutil_uri_list

    except Exception as e:
        logging.error(f"Error: {e}")
        raise AirflowFailException('Failure of the task due to encountered error.')
        
# Define a function to load data from google bucket to google bigquery
def upload_to_bigquery(client, dataset_name, table_name, job_config, gsutil_uri):
    try:
        for i in range(len(table_name)): 
            # Look up for dataset
            dataset_ref = client.dataset(dataset_name)
            try:
                targeted_dataset = client.get_dataset(dataset_ref)
                logging.info(f'Dataset {targeted_dataset.dataset_id} already exists.')
            # Create new dataset if not found
            except NotFound:
                logging.info(f"Dataset {dataset_ref} is not found")
                logging.info(f"Creating dataset - {dataset_ref} in progress ...")
                targeted_dataset = client.create_dataset(dataset_ref)
                logging.info(f'Dataset {targeted_dataset.dataset_id} created.')

            # Look up for table
            table_ref = dataset_ref.table(table_name[i])
            try:
                targeted_table = client.get_table(table_ref)
                logging.info(f'Table {targeted_table.table_id} already exists.')
            # Create new table if not found
            except NotFound:
                logging.info(f"Dataset {table_ref} is not found")
                logging.info(f"Creating table - {table_ref} in progress ...")
                targeted_table = client.create_table(table_ref)
                logging.info(f'Table {targeted_table.table_id} created.')

            # Upload the data to bigquery table using gsutil URI
            load_job = client.load_table_from_uri(gsutil_uri[i],
                                                  targeted_table, 
                                                  job_config=job_config)

            logging.info(load_job.result())
            logging.info(f"SUCCESS: The data has been loaded to Google BigQuery.")
        
    except Exception as e:
        logging.error(f"Error: {e}")