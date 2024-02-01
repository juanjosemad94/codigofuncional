import logging
import datetime
import pandas as pd
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import time
import azure.functions as func
import os

app = func.FunctionApp()

# Azure Blob Storage details (retrieve from environment variables)
account_url = os.environ.get('ACCOUNT_URL')
credential = os.environ.get('CREDENTIAL')
container_name = os.environ.get('BLOB_CONTAINER')
blob_name = os.environ.get('BLOB_NAME')

# Create the blob client
blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)

# Kusto details (retrieve from environment variables)
cluster = os.environ.get('CLUSTER')
database = os.environ.get('DATABASE')

# ADX Connection Credentials (retrieve from environment variables)
clientID = os.environ.get('CLIENT_ID')
clientSecret = os.environ.get('CLIENT_SECRET')
tenantID = os.environ.get('TENANT_ID')

# Client Details (retrieve from environment variables)
clientName = os.environ.get('CLIENT_NAME')
resourceGroup = os.environ.get('RESOURCE_GROUP')

# Save data to blob
def save_to_blob(dataframe, container_name, blob_name):
    csv_data = dataframe.to_csv(index=False)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(csv_data, overwrite=True)


# Connect to and run defined queries against ADX
def run_adx_query(query):
    try:
        # Build the connection string with AAD token authentication
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, clientID, clientSecret, tenantID)

        # Create the KustoClient and execute the query
        client = KustoClient(kcsb)
        response = client.execute(database, query)
        df = dataframe_from_result_table(response.primary_results[0])
        return df

    except Exception as e:
        logging.error(f"Error executing ADX query: {e}")
        return None

# Save data to a CSV file.
def save_to_csv(dataframes):
    for dataframe, filename in dataframes:
        dataframe.to_csv(filename, index=False)

# Query 90 Day EPS
def query_90DayEPS():
    query = """
    let StartDateTime= now(-90d);
    let EndDateTime = now();
    TimeSeriesData
    | where Timestamp between (StartDateTime .. EndDateTime)
    | summarize totalEvents = count()
    | extend avgEventsPerSecond = totalEvents / (90*24*60*60)
    | project avgEventsPerSecond
    """
    return run_adx_query(query)

# Query 30 Day EPS
def query_30DayEPS():
    query = """
    let StartDateTime= now(-30d);
    let EndDateTime = now();
    TimeSeriesData
    | where Timestamp between (StartDateTime .. EndDateTime)
    | summarize totalEvents = count()
    | extend avgEventsPerSecond = totalEvents / (30*24*60*60)
    | project avgEventsPerSecond
    """
    return run_adx_query(query)

# Query 7 Day EPS
def query_7DayEPS():
    query = """
    let StartDateTime= now(-7d);
    let EndDateTime = now();
    TimeSeriesData
    | where Timestamp between (StartDateTime .. EndDateTime)
    | summarize totalEvents = count()
    | extend avgEventsPerSecond = totalEvents / (7*24*60*60)
    | project avgEventsPerSecond
    """
    return run_adx_query(query)

# Query Total Tags
def query_TotalTags():
    query = """
    DataStreamMetaData 
    | distinct TimeSeriesId 
    | count
    """
    return run_adx_query(query)

# Query Active Tags
def query_ActiveTags():
    query = """
    TimeSeriesData
    | where Timestamp > ago(7d) 
    | summarize tagCount = count_distinct(TimeSeriesId)
    """
    return run_adx_query(query)

# Query number of users for the day
def query_FusionUsers():
    query = f"""
    .show queries 
    | where Database == "{database}"
    | where StartedOn >= ago(1d)
    | where Text contains_cs "TimeSeriesData"
    | summarize count() by User
    | extend UserInfo = pack('User', User, 'Count', count_)
    | summarize UserList = make_list(UserInfo)
    | project Users = tostring(UserList)
    """
    return run_adx_query(query)

# Query Tag Latency
def query_Latency():
    query = """
    let step=1s;
    let start=startofday(ago(1d));
    let end=bin(ago(step), step);
    TimeSeriesData
    | where ingestion_time() between(start .. end)
        and DeviceId != "Manual Ingestion"    // this filters out historical data requests which is not commonly needed
        and Action == "Add"
    | extend Latency = (ingestion_time() - Timestamp) / 1s   // Latency in number of seconds
    | summarize Latency=minif(Latency, Latency > 0.1) by Timestamp=bin(ingestion_time(), step)   // This is a quick fix to avoid negative latency due to future dated timestamps. Will return tuning based on the data, maybe a 2 second minimum.
    | fork
        aggregation = (summarize
        round(mean=avgif(Latency, Latency < 120), 2)) // Remove data handled by outage handler
        raw_Latency_data = (where 1==1)
    """
    return run_adx_query(query)

# Main function
def execute_main():
    utc_timestamp = datetime.datetime.now(datetime.timezone.utc)
    start_time = time.time()

    data_90DayEPS = query_90DayEPS()
    data_30DayEPS = query_30DayEPS()
    data_7DayEPS = query_7DayEPS()
    data_TotalTags = query_TotalTags()
    data_ActiveTags = query_ActiveTags()
    data_FusionUsers = query_FusionUsers()
    data_Latency = query_Latency()
        
    combined_data = {
        "Client": clientName, 
        "Resource Group": resourceGroup,
        "Timestamp": [utc_timestamp],
        "7 Day EPS": [data_7DayEPS.iloc[0, 0]],
        "30 Day EPS": [data_30DayEPS.iloc[0, 0]],
        "90 Day EPS": [data_90DayEPS.iloc[0, 0]],
        "Tags": [data_TotalTags.iloc[0, 0]],
        "Active Tags": [data_ActiveTags.iloc[0, 0]],
        "Daily Users": [data_FusionUsers.iloc[0, 0]],
        "Latency": [data_Latency.iloc[0, 0]]
    }

    df_combined = pd.DataFrame(combined_data)
    
    save_to_blob(df_combined, container_name, blob_name)

    # Log function runtime
    end_time = time.time()  
    runtime = end_time - start_time  # Calculate the runtime
    logging.info(f"Function executed in: {round(runtime, 2)} seconds")

# Run function on a time trigger
@app.schedule(schedule="0 0 0 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    #execute_main()
    logging.info(f"Function executed in: seconds")