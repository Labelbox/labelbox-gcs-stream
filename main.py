import labelbox
from labelbox import Client, Dataset
from labelbox.schema.data_row_metadata import DataRowMetadata, DataRowMetadataField, DataRowMetadataKind, DeleteDataRowMetadata
from processing.metadata import *
import os

def stream_data_rows(event, context):
    """ 
    Creates a data row when a new asset is uploaded to a GCS bucket. 
    - Row Data URL is pulled from the event payload
    - Global Key is determined by the GCS Object Name; it will use the unique GCS Object ID if the first choice is taken
    - Dataset is determined by the GCS Bucket Name; if there isn't one with that name, this script creates one
    Environment Variables:
        labelbox_api_key            : Required (str) : Labelbox API Key
        labelbox_integration_name   : Optional (str) : Labelbox Delegated Access Integration Name
    Args:
         event                      : Required (dict) : Event payload.
         context                    : Required (google.cloud.functions.Context) : Metadata for the event.
    """
    # Get environment variables
    labelbox_api_key = os.environ.get("labelbox_api_key", '')
    # Connect to Labelbox
    try:
        client = Client(api_key=labelbox_api_key)
    except:
        print(f'Could not connect to Labelbox. Please provide this function runtime with a Labelbox API KEY as an Environment Variable with the name "labelbox_api_key"')    
    # Get event details
    gcs_bucket_name = event['bucket']
    gcs_object_id = event['id']
    gcs_object_name = event['name']
    url = f"gs://{gcs_bucket_name}/{gcs_object_name}"
    print(f'Row Data URL {url}')
    # Get or create a Labelbox dataset with the same name as the GCS bucket
    datasets = client.get_datasets(where=Dataset.name == gcs_bucket_name)
    lb_dataset = next(datasets, None)
    if not lb_dataset:
        # If creating a new dataset, we need to set the Delegated Access Integration
        lb_integration = os.environ.get("labelbox_integration_name", 'DEFAULT')
        for integration in client.get_organization().get_iam_integrations():
            if integration.name == lb_integration:
                lb_integration = integration
        lb_dataset = client.create_dataset(name=gcs_bucket_name, iam_integration=lb_integration)
        da_name = lb_integration if type(lb_integration) == str else lb_integration.name
        print(f'Created Labelbox Dataset {lb_dataset.name} with ID {lb_dataset.uid} and Delegated Access Integration Name {da_name}')
    # Create a Labelbox Data Row using the GCS Object Name as the Labelbox Global Key value. 
    # If that Global Key value is unavailable, use the GCS-unique Object ID value instead.
    try:
        lb_data_row = lb_dataset.create_data_row(row_data=url, global_key=gcs_object_name)
        print(f'Created Data Row with ID {lb_data_row.uid} and Global Key {gcs_object_name}')
    except:
        print(f'Data Row with Global Key "{gcs_object_name}" already exists. Creating a Data Row with GCS Object ID "{gcs_object_id}"')
        lb_data_row = lb_dataset.create_data_row(row_data=url, global_key=gcs_object_id)
        print(f'Created Data Row with ID {lb_data_row.uid} and Global Key {gcs_object_id}')        
    return True

def update_metadata(event, context):
    """ 
    Updates labelbox metadata when object metadata is updated in GCS. If an object has no metadata, this script deletes all Labelbox metadata. 
    - Uses Global Keys to find Data Rows. First will use GCS Object ID, defers to GCS Object Name 
    - This script can be customized to not delete Labelbox metadata for specific metadata schema IDs. See environment variable 'keep_metadata_name'
    Environment Variables:
        labelbox_api_key            : Required (str) : Labelbox API Key
        keep_metadata_name          : Optional (list) : List of metadata field names to **not** delete metadata from
    Args:
         event                      : Required (dict) : Event payload.
         context                    : Required (google.cloud.functions.Context) : Metadata for the event.
    """    
    # Get environment variables
    labelbox_api_key = os.environ.get("labelbox_api_key", '')
    keep_metadata_name = os.environ.get("keep_metadata_name", [])
    # Connect to Labelbox
    try:
        client = Client(api_key=labelbox_api_key)
    except:
        print(f'Could not connect to Labelbox. Please provide this function runtime with a Labelbox API KEY as an Environment Variable with the name "labelbox_api_key"')      
    # Get event details
    gcs_object_id = event['id']
    gcs_object_name = event['name']
    # Grab Labelbox Data Row ID given the Global Key 
    try: 
        lb_data_row_id = client.get_data_row_ids_for_global_keys([gcs_object_id])['results'][0]
        lb_global_key = gcs_object_id
    except:
        try:
            lb_data_row_id = client.get_data_row_ids_for_global_keys([gcs_object_name])['results'][0]
            lb_global_key = gcs_object_name
        except:
            print(f'No data row with Global Key {gcs_object_name} or {gcs_object_id} exist')
            quit()    
    # If there's no event['metadata'] then that means metadata must have been deleted
    try:
        gcs_metadata = event['metadata']
        gcs_metadata_names = list(gcs_metadata.keys())
        lb_metadata_list = []
    except:
        gcs_metadata = False
        gcs_metadata_names = [] 
    # Sync Metadata fields between Labelbox and GCS
    lb_mdo = __sync_metadata_fields_as_strings(client, gcs_metadata_names)    
    metadata_schema_to_name_key = __get_metadata_schema_to_name_key(lb_mdo)
    metadata_name_key_to_schema = {v: k for k, v in metadata_schema_to_name_key.items()}
    # If there's metadata in GCS, update it in Labelbox
    if gcs_metadata:
        lb_metadata_list = [DataRowMetadataField(schema_id=metadata_name_key_to_schema[gcs_metadata_name],value=gcs_metadata[gcs_metadata_name]) for gcs_metadata_name in gcs_metadata]
        lb_mdo.bulk_upsert(
            [
                DataRowMetadata(
                    data_row_id=lb_data_row_id,
                    fields=lb_metadata_list
                )
            ]
        )
        print(f'Updated Data Row with ID {lb_data_row_id} and Global Key {lb_global_key}')
    # If there isn't metadata in GCS, then we can assume metadata was deleted in GCS, delete it in Labelbox        
    else: 
        delete_metadata_names = [field.name for field in lb_mdo.bulk_export([lb_data_row_id])[0].fields]
        for keep_metadata_name in keep_metadata_names:
            delete_metadata_names.remove(keep_metadata_name)
        __delete_data_row_metadata(lb_mdo, [lb_data_row_id], delete_metadata_names, metadata_name_key_to_schema)
        print(f'Deleted Metadata for Data Row with ID {lb_data_row_id} and Global Key {lb_global_key}')
    return True

def delete_data_rows(event, context):
    """ Delete a data row from Labelbox
    - Uses Global Keys to find Data Rows. First will use GCS Object ID, defers to GCS Object Name 
    Args:
         event (dict)                                   : Event payload.
         context (google.cloud.functions.Context)       : Metadata for the event.
    """    
    # Get environment variables
    labelbox_api_key = os.environ.get("labelbox_api_key", '')
    # Connect to Labelbox
    try:
        client = Client(api_key=labelbox_api_key)
    except:
        print(f'Could not connect to Labelbox. Please provide this function runtime with a Labelbox API KEY as an Environment Variable with the name "labelbox_api_key"')      
    # Get event details
    gcs_object_id = event['id']
    gcs_object_name = event['name'] 
    # Grab Labelbox Data Row ID given the Global Key 
    try:
        print(f'Checking if data row with global_key {gcs_object_id} exists')
        lb_data_row_id = client.get_data_row_ids_for_global_keys([gcs_object_id])['results'][0]
        lb_global_key = gcs_object_id
    except:
        lb_data_row_id = client.get_data_row_ids_for_global_keys([gcs_object_name])['results'][0]
        lb_global_key = gcs_object_name
    # Delete Data Row from Labelbox
    print(f'Deleting Data Row with ID {lb_data_row_id} and Global Key {lb_global_key}')
    lb_data_row = client.get_data_row(lb_data_row_id)
    lb_data_row.delete()
    return True
