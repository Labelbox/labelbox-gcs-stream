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

## How to Use:
1. Authenticate your GCloud CLI in your Terminal:
```
gcloud auth login
```
2. [Recommended] Configure gcloud config to the region your GCS Bucket is in (example region - "us-east1")
* This can be found on the *Configuration* tab in the GCS Bucket UI
```
gcloud config set project PROJECT_NAME
gcloud config set functions/region "us-east1"
```
3. Create / Redeploy your GCS function from GitHub:
    * All cloud functions are deployed from the same main.py file, only difference is in the deployment parameters and the ```entry-point``` argument provided
    * Running ```gcloud functions deploy FUNCTION_NAME``` will create a google cloud function of said name or update an exsting one

4. Example using this repo:

Clone Repo and Define Variables:
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
