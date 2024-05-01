# garmin-azure-integration
## Overview
This Python script is designed to process data from Garmin wearable devices stored in Azure Blob Storage. It performs the following main tasks:

1. **Unzipping Garmin Data**: Extracts data from zip files stored in Azure Blob Storage.
2. **Reading Data**: Reads and processes JSON files containing various health and fitness data collected from Garmin devices.
3. **Data Analysis**: Provides functions to analyze and aggregate Garmin data for individual subjects or groups of subjects.
## Requirements
- Python 3.x
- Azure Blob Storage account
- Access to Garmin data stored in Azure Blob Storage
## Setup
1. **Azure Blob Storage Configuration**:
- Obtain the account URL and SAS token for your Azure Blob Storage.
- Ensure that your account URL starts with "https://" and ends with ".net/".
- Ensure that your SAS token starts with a "?".
2. **Installation**:
- Install the required Python packages using pip install -r requirements.txt.
## Usage
1. Running the Script:
- Modify the account_url and sas_token variables in the script with your Azure Blob Storage credentials.
- Optionally, adjust any other parameters or settings according to your requirements.
- Run the script using Python: python garmin_data_processor.py.
2. **Processing Garmin Data**:
- Specify the subject or list of subjects whose data you want to process.
- The script will download and process the relevant data from Azure Blob Storage.
- Processed data will be saved as a CSV file named "garmin_data.csv" in the current directory.
3. **Data Analysis**:
- Analyze the processed data using built-in functions provided in the script.
- Example analyses include calculating stress averages for individual subjects and groups.
