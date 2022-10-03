import labelbox
from labelbox import Client, Dataset
from labelbox.schema.data_row_metadata import DataRowMetadata, DataRowMetadataField, DataRowMetadataKind
import os

# Add your API key below
labelbox_api_key = os.environ.get("labelbox_api_key", '')
try:
    client = Client(api_key=labelbox_api_key)
except:
    print(f'Could not connect to Labelbox. Please provide the Cloud Function with a Labelbox API KEY as an Environment Variable with the name "labelbox_api_key"')


def stream_data_rows(event, context):
    """ Uploads an asset to Catalog when a new asset is uploaded to GCP bucket. Creates a dataset if a dataset with this bucket name doesn't exist yet
    Args:
         event (dict)                                   : Event payload.
         context (google.cloud.functions.Context)       : Metadata for the event.
    """    
    gcs_bucket_name = event['bucket']
    gcs_object_id = event['id']
    gcs_object_name = event['name']

    datasets = client.get_datasets(where=Dataset.name == gcs_bucket_name)
    lb_dataset = next(datasets, None)
    if not lb_dataset:
        lb_integration = os.environ.get("labelbox_integration_name", 'DEFAULT')
        for integration in client.get_organization().get_iam_integrations():
            if integration.name == labelbox_integration:
                lb_integration = integration
        print(f'Creating Labelbox Dataset {lb_dataset.name} with ID {lb_dataset.uid}')
        lb_dataset = client.create_dataset(name=gcs_bucket_name, iam_integration=lb_integration)
        
    url = f"gs://{gcs_bucket_name}/{gcs_object_name}"
    print(f'Row Data URL {url}')

    try:
        lb_data_row = lb_dataset.create_data_row(row_data=url, global_key=gcs_object_name)
        print(f'Created Data Row with ID {lb_data_row.uid} and Global Key {gcs_object_name}')
    except:
        print(f'Data Row with Global Key "{gcs_object_name}" already exists. Creating a Data Row with GCS Object ID "{gcs_object_id}"')
        lb_data_row = lb_dataset.create_data_row(row_data=url, global_key=gcs_object_id)
        print(f'Created Data Row with ID {lb_data_row.uid} and Global Key {gcs_object_id}')
        
    return True

def update_metadata(event, context):
    """ Updates a data row's metadata with the current metadata in GCS. Meant to be triggered whenever there's a change to metadata in GCS. 
    Args:
         event (dict)                                   : Event payload.
         context (google.cloud.functions.Context)       : Metadata for the event.
    """    
    gcs_object_id = event['id']
    gcs_object_name = event['name']
    
    # If no 'metadata', then that means metadata was deleted    
    try:
        gcs_metadata = event['metadata']
    except:
        gcs_metadata = [] 
    lb_metadata_list = []
    
    # If there's gcs_metadata, create a `lb_metadata_list` list of Labelbox Metadata Fields to upload
    if gcs_metadata:
        mdo = client.get_data_row_metadata_ontology()
        lb_metadata_names = [field['name'] for field in mdo._get_ontology()]        
        # Ensure all your metadata fields in this object are in Labelbox - if not, we'll create "string" metadata given the field name
        for gcs_metadata_field in gcs_metadata.keys():
            if gcs_metadata_field not in lb_metadata_names:
                mdo.create_schema(name=gcs_metadata_field, kind=DataRowMetadataKind.string)
                mdo = client.get_data_row_metadata_ontology()
                lb_metadata_names = [field['name'] for field in mdo._get_ontology()]    
        print(f'GCS Metadata: {gcs_metadata}')
        # Create a `mdo_index` dictionary where {key=metadata_field_name : value=metadata_schema_id}
        metadata_dict = mdo.reserved_by_name
        metadata_dict.update(mdo.custom_by_name)
        mdo_index = {}
        for mdo_name in metadata_dict:
            if type(metadata_dict[mdo_name]) == dict:
                mdo_index.update({str(mdo_name) : {"schema_id" :  list(metadata_dict[mdo_name].values())[0].parent}})
                for enum_option in metadata_dict[mdo_name]:
                    mdo_index.update({str(enum_option) : {"schema_id" : metadata_dict[mdo_name][enum_option].uid, "parent_schema_id" : metadata_dict[mdo_name][enum_option].parent}})
            else:
              mdo_index.update({str(mdo_name):{"schema_id" : metadata_dict[mdo_name].uid}})
        # Create a `lb_metadata_list` list of Labelbox Metadata Fields to upload
        for gcs_metadata_field in gcs_metadata.keys():
            lb_metadata_list.append(
                DataRowMetadataField(
                    schema_id=mdo_index[gcs_metadata_field]['schema_id'], 
                    value=gcs_metadata[gcs_metadata_field]
                )
            )
    # Grab Labelbox Data Row ID given the global_key
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
            
    # Update Labelbox Metadata
    mdo = client.get_data_row_metadata_ontology()
    task = mdo.bulk_upsert([
        DataRowMetadata(
            data_row_id = lb_data_row_id,
            fields = lb_metadata_list
        )
    ])
    print(f'Updated Data Row with ID {lb_data_row_id} and Global Key {lb_global_key}\nErrors: {task.errors}')
    
    return True

def delete_data_rows(event, context):
    """ Deletes data rows from Labelbox. First checks to see if the global key for gcs_object_id exists before using the gcs_object_name
    Args:
         event (dict)                                   : Event payload.
         context (google.cloud.functions.Context)       : Metadata for the event.
    """    
    gcs_object_id = event['id']
    gcs_object_name = event['name']    
    
    # Get your Labelbox Data Row ID
    try:
        # gcs_object_id is used as the global key when a data row with the object name as the global key already exists
        print(f'Checking if data row with global_key {gcs_object_id} exists')
        lb_data_row_id = client.get_data_row_ids_for_global_keys([gcs_object_id])['results'][0]
        lb_global_key = gcs_object_id
    except:
        # If the gcs_object_id was not used as the global key, the gcs_object_name was
        lb_data_row_id = client.get_data_row_ids_for_global_keys([gcs_object_name])['results'][0]
        lb_global_key = gcs_object_name
        
    # Delete the Labelbox Data Row
    print(f'Deleting Data Row with ID {lb_data_row_id} and Global Key {lb_global_key}')
    lb_data_row = client.get_data_row(lb_data_row_id)
    lb_data_row.delete()
    
    return True
