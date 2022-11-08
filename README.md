# Data Streaming with GCS using Google Cloud Functions

This repo will provide with 3 deployable Cloud Functions:
 * **stream_data_rows** - Creates a Labelbox data row every time a new asset is uploaded to the GCS bucket
 * **update_metadata** - Updates Labelbox metadata any time updates in GCS asset metadata are made
 * **delete_data_rows** - Deletes a data row in Labelbox if that data row is deleted in GCS


Prerequisites:
1. Set up Delegated Access to Labelbox, save the integration name for deployment
    * See how here: https://docs.labelbox.com/docs/using-google-cloud-storage 
2. GCS Account with permissions to create GCS functions and read bucket events
3. Labelbox API Key - needed to create/delete data rows and update data row metadata values

* *See latest supported data types here: https://docs.labelbox.com/docs/supported-data-types*

## How to Create / Deploy Google Cloud Functions from your Command Line
### Setup
Use ```gcloud auth login``` to authenticate Google Cloud - you should expect a pop-up from Google prompting a sign in:
```
gcloud auth login
```
[Recommended] Use ```gcloud config``` to set google project & region for your cloud function / bucket (example region - "us-east1")
* This can be found on the *Configuration* tab in the GCS Bucket UI
```
gcloud config set project PROJECT_NAME
gcloud config set functions/region "us-east1"
```
Clone this repo, defile your CLI enviornment variables
```
git clone https://github.com/Labelbox/labelbox-gcs-stream.git
cd labelbox-gcs-stream
GCS_BUCKET_NAME=my_bucket_name
LABELBOX_API_KEY=my_api_key
LABELBOX_INTEGRATION_NAME=my_integration_name
```
### Create GCS Functions from GitHub using ```gcloud functions delploy```
* If no funciton with the name provided exists, ```gcloud functions delploy``` creates a Google Cloud function
* If a funciton with the name provided exists, ```gcloud functions delploy``` updates the existing Google Cloud function
* You can deploy cloud functions from GitHub by giving your root directory a ```main.py``` file and a ```requirements.txt``` file like in this repo - for different cloud functions in one repo, specify which python function for which cloud function with the ```--entry_point``` parameter in ```gcloud functions delploy```

Deploy ```stream_data_rows```:
```
gcloud functions deploy stream_data_rows_function --entry-point stream_data_rows --runtime python37 --trigger-bucket=$GCS_BUCKET_NAME --timeout=540 --set-env-vars=labelbox_api_key=$LABELBOX_API_KEY,labelbox_integration_name=$LABELBOX_INTEGRATION_NAME
```
Deploy ```update_metadata```:
```
gcloud functions deploy update_metadata --entry-point update_metadata --runtime python37 --trigger-resource=$GCS_BUCKET_NAME --trigger-event="google.storage.object.metadataUpdate" --timeout=540 --set-env-vars=labelbox_api_key=$LABELBOX_API_KEY,labelbox_integration_name=$LABELBOX_INTEGRATION_NAME
```
Deploy ```delete_data_rows```:
```
gcloud functions deploy delete_data_rows_function --entry-point delete_data_rows --runtime python37 --trigger-bucket=$GCS_BUCKET_NAME --timeout=540 --set-env-vars=labelbox_api_key=$LABELBOX_API_KEY
```
