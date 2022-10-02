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
    bucket_name = event['bucket']
    object_id = event['id']
    object_name = event['name']

    datasets = client.get_datasets(where=Dataset.name == bucket_name)
    dataset = next(datasets, None)
    if not dataset:
        labelbox_integration = os.environ.get("labelbox_integration_name", 'DEFAULT')
        for integration in client.get_organization().get_iam_integrations():
            if integration.name == labelbox_integration:
                labelbox_integration = integration
        dataset = client.create_dataset(name=bucket_name, iam_integration=labelbox_integration)
    url = f"gs://{bucket_name}/{object_name}"

    try:
        data_row = dataset.create_data_row(row_data=url, global_key=object_name)
        print(f'Created Data Row with ID {data_row.uid} and Global Key {object_name}')
    except:
        print(f'Data Row with Global Key "{object_name}" already exists. Creating a Data Row with GCS Object ID "{object_id}"')
        data_row = dataset.create_data_row(row_data=url, global_key=object_id)
        print(f'Created Data Row with ID {data_row.uid} and Global Key {object_id}')

    return True

def update_metadata(event, context):
    """ Updates a data row's metadata with the current metadata in GCS. Meant to be triggered whenever there's a change to metadata in GCS. 
    Args:
         event (dict)                                   : Event payload.
         context (google.cloud.functions.Context)       : Metadata for the event.
    """    
    bucket_name = event['bucket']
    object_id = event['id']
    object_name = event['name']
    gcs_metadata = event['metadata']
    
    mdo = client.get_data_row_metadata_ontology()
    lb_metadata_names = [field['name'] for field in mdo._get_ontology()]
    # Ensure all your metadata fields in this object are in Labelbox - if not, we'll create "string" metadata 
    for gcs_metadata_field in gcs_metadata.keys():
        if gcs_metadata_field not in lb_metadata_names:
            mdo.create_schema(name=gcs_metadata_field, kind=DataRowMetadataKind.string)
            mdo = client.get_data_row_metadata_ontology()
            lb_metadata_names = [field['name'] for field in mdo._get_ontology()]    
    print(f'GCS Metadata: {gcs_metadata}')

    # Create a dictionary where {key=metadata_field_name : value=metadata_schema_id}
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
    print(f'MDO Index {mdo_index}')     
    
    # Create your list of Labelbox Metadata Fields to upload
    labelbox_metadata = []
    for gcs_metadata_field in gcs_metadata.keys():
        print(f"Schema ID : {mdo_index[gcs_metadata_field]['schema_id']}")
        print(f"Value : {gcs_metadata[gcs_metadata_field]}")
        labelbox_metadata.append(
            DataRowMetadataField(
                schema_id=mdo_index[gcs_metadata_field]['schema_id'], 
                value=gcs_metadata[gcs_metadata_field]
            )
        )
    print(f'Labelbox Metadata {labelbox_metadata}')
    
    # Update your Labelbox Data Row
    try:
        data_row = client.get_data_row_ids_for_global_keys([object_name])['results'][0]
        mdo = client.get_data_row_metadata_ontology()
        task = mdo.bulk_upsert([
            DataRowMetadata(
                data_row_id = data_row.uid,
                fiels = labelbox_metadata
            )
        ])
        print(f'Updated Data Row ID {data_row.uid}\nErrors: {task.errors}')
    except:
        print(f'No data row with Global Key {object_name} exists')

    print(f'Updated Metadata for Labelbox Data Row ID {data_row.uid} with Global Key {object_name}')

    return True
