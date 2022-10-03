# Data Streaming with GCS using Google Cloud Functions

## This repo will provide with 3 deployable Cloud Functions:
 * **stream_data_rows** - This will create a Labelbox data row every time a new asset is uploaded to the GCS bucket
 * **update_metadata** - This will update Labelbox Metadata for existing data rows any time an object's metadata is modified in GCS
 * **delete_data_rows** - This will delete a data row every time an asset is deleted in the GCS bucket


Prerequisites:
1. Set up Delegated Access to Labelbox, and ensure that the default setting is to the target bucket.
    * See how here: https://docs.labelbox.com/docs/using-google-cloud-storage 
2. Only files supported by Labelbox will be uploaded
    * See latest supported data types here: https://docs.labelbox.com/docs/supported-data-types
3. The Google account performing these steps has access to the Google bucket and permissions to create Lambda functions
4. The Labelbox account performing these steps has Admin-level permissions in a Labelbox organization

## How to Create / Deploy Google Cloud Functions from your Command Line
### Setup
   * Use ```gcloud auth login``` to authenticate Google Cloud - you should expect a pop-up from Google prompting a sign in:
```
gcloud auth login
```
   * [Recommended] Use ```gcloud config`` to set google project & region for your cloud function / bucket (example region - "us-east1")
   * This can be found on the *Configuration* tab in the GCS Bucket UI
```
gcloud config set project PROJECT_NAME
gcloud config set functions/region "us-east1"
```
### Create GCS Functions from GitHub using ```gcloud functions delploy```
    * You can deploy cloud functions from GitHub by putting your code in your main.py file and specifying the function to run with the ```--entry_point``` parameter in ```gcloud functions delploy```
    * If no funciton with the name provided exists, ```gcloud functions delploy``` creates a Google Cloud function
    * If a funciton with the name provided exists, ```gcloud functions delploy``` updates the existing Google Cloud function
```
git clone https://github.com/Labelbox/labelbox-gcs-stream.git
cd labelbox-gcs-stream
GCS_BUCKET_NAME="my-bucket"
LABELBOX_API_KEY="my-api-key"
LABELBOX_INTEGRATION_NAME="my-integration-name"
```
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
