Data Streaming with GCS using Google Cloud Functions

This repo will provide you the core capabilities required to stream a Google Bucket into Labelbox.

Prerequisites:
1. Set up Delegated Access to Labelbox, and ensure that the default setting is to the target bucket.
    * See how here: https://docs.labelbox.com/docs/using-google-cloud-storage 
2. Only files supported by Labelbox will be uploaded - please see here for the latest: https://docs.labelbox.com/docs/supported-data-types
3. The Google account performing these steps has access to the Google bucket and permissions to create Lambda functions
4. The Labelbox account performing these steps has Admin-level permissions in a Labelbox organization

Steps:
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
    * The  example functions in this notebook's main.py file are:
        * **stream_data_rows** - This will create a data row every time a new asset is uploaded to the bucket
        * **update_metadata** - This will update metadata any time object metadata is modified in GCS

4. Example using this repo:

```
git clone labelbox-gcs-stream
cd labelbox-gcs-stream
GCS_BUCKET_NAME="my-bucket"
LABELBOX_API_KEY="my-api-key"
LABELBOX_INTEGRATION_NAME="my-integration-name"
gcloud functions deploy stream_data_rows_function --entry-point stream_data_rows --runtime python37 --trigger-bucket=$GCS_BUCKET_NAME --timeout=540 --set-env-vars=labelbox_api_key=$LABELBOX_API_KEY,labelbox_integration_name=$LABELBOX_INTEGRATION_NAME
gcloud functions deploy GCLOUD_FUNCTION_NAME --entry-point update_metadata --runtime python37 --trigger-bucket=GCS_BUCKET_NAME --timeout=540 --set-env-vars=labelbox_api_key=$LABELBOX_API_KEY,labelbox_integration_name=$LABELBOX_INTEGRATION_NAME
```
