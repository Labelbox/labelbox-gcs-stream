from labelbox.schema.data_row_metadata import DataRowMetadata, DataRowMetadataKind, DataRowMetadataOntology, DeleteDataRowMetadata

def __sync_metadata_fields_as_strings(client, metadata_names=[]):
    """ Ensures Labelbox's Metadata Ontology has all necessary metadata fields given a list of names - this script will always sycn metadata fields as strings
    Args:
        client              :   Required (labelbox.Client) : Labelbox client object
        metadata_names      :   Required (list) : List of metadata field names to sync with Labelbox
    Returns: 
        Updated Labelbox metadata ontology object
    """
    lb_mdo = client.get_data_row_metadata_ontology()
    lb_metadata_names = [field['name'] for field in lb_mdo._get_ontology()]
    metadata_names += ['lb_integration_source']
    # Iterate over your metadata_index, if a metadata_index key is not an existing metadata_field, then create it in Labelbox
    for metadata_name in metadata_names:
        if metadata_name not in lb_metadata_names:
            lb_mdo.create_schema(name=metadata_name, kind=DataRowMetadataKind.string)
            lb_mdo = client.get_data_row_metadata_ontology()
            lb_metadata_names = [field['name'] for field in lb_mdo._get_ontology()]
    return lb_mdo

def __get_metadata_schema_to_name_key(
    lb_mdo:DataRowMetadataOntology
    ):
    """ Creates a dictionary where {key=metadata_schema_id: value=metadata_name_key} 
    - name_key is name for all metadata fields, and for enum options, it is "parent_name>>child_name"
    Args:
        lb_mdo              :   Required (labelbox.schema.data_row_metadata.DataRowMetadataOntology) - Labelbox metadata ontology
    Returns:
        Dictionary where {key=metadata_schema_id: value=metadata_name_key}
    """
    lb_metadata_dict = lb_mdo.reserved_by_name
    lb_metadata_dict.update(lb_mdo.custom_by_name)
    metadata_schema_to_name_key = {}
    for metadata_field_name in lb_metadata_dict:
        if type(lb_metadata_dict[metadata_field_name]) == dict:
            metadata_schema_to_name_key[lb_metadata_dict[metadata_field_name][next(iter(lb_metadata_dict[metadata_field_name]))].parent] = str(metadata_field_name)
            for enum_option in lb_metadata_dict[metadata_field_name]:
                metadata_schema_to_name_key[lb_metadata_dict[metadata_field_name][enum_option].uid] = f"{str(metadata_field_name)}///{str(enum_option)}"
        else:
            metadata_schema_to_name_key[lb_metadata_dict[metadata_field_name].uid] = str(metadata_field_name)
    return metadata_schema_to_name_key

def __delete_data_row_metadata(
    lb_mdo:DataRowMetadataOntology, 
    data_row_ids:list, metadata_field_names:list, metadata_name_key_to_schema:dict
    ):
    """ Deletes metadata values from a given set of data rows given a list of metadata field names
    Args:
        lb_mdo                      : Required (labelbox.schema.data_row_metadata.DataRowMetadataOntology) - Labelbox metadata ontology
        data_row_ids                : Required (dict) : List of data row IDs to delete metadata from
        metadata_field_names        : Required (list) : List of metadata schemas to delete for each data row
        metadata_name_key_to_schema : Required (dict) : Dictionary where {key=metadata_schema_id: value=metadata_name_key}
    Returns:
        True
    """
    if 'lb_integration_source' in metadata_field_names:
        metadata_field_names.remove('lb_integration_source')
    schemas_to_delete = [metadata_name_key_to_schema[name] for name in metadata_field_names]
    for data_row_id in data_row_ids:
        lb_mdo.bulk_delete([
            DeleteDataRowMetadata(
                data_row_id = data_row_id,
                fields = schemas_to_delete
            )
        ])
    return True