# Import necessary modules and classes
from datetime import datetime

from fleet_performance import CMMS
from fleet_performance.helpers.data_acquisition.utilities import get_dataframe
from fleet_performance.interfaces.cmms.helpers.enums import WorkOrderStatus

# Get the current date and time
current_date = datetime.now()

# Get the first day of the current month
start_date = current_date.replace(day=1)

# Calculate the first day of the next month
next_month = start_date.month % 12 + 1  # Get the next month, wrapping to January if necessary
next_month_year = start_date.year + (1 if next_month == 1 else 0)  # Increment year if next month is January
end_date = start_date.replace(year=next_month_year, month=next_month)

# Format start and end dates as strings
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# Initialize CMMS instance with superuser privileges and production environment
cmms = CMMS(superuser=True, environment='production')

# SQL query to retrieve AssetID and SCADA_GUID from the database
sqlquery = '''
SELECT AssetID, SCADA_GUID
FROM Business.vwSites
WHERE OnM_Project_Status = 'In Operation'
AND CAT_Analyst_Email like 'faizul%'
'''
# Get a DataFrame with AssetID and SCADA_GUID
df_assetID = get_dataframe(sqlquery)

# Function to retrieve and save work orders for a given asset and SCADA GUID
def get_wo(assetid, scada_guid):
    open_wo = cmms.get_work_orders_by_global_fed_site_id(asset_id=assetid,
                                                         status=WorkOrderStatus.Open,
                                                         start_date_time=start_date_str,
                                                         end_date_time=end_date_str,
                                                         include_asset_line_item=True
                                                         )
    open_wo.to_csv(f'Quiz2_openWO/{scada_guid}_open_WO_from_{start_date_str}_to_{end_date_str}.csv')

# Iterate through rows of df_assetID and call get_wo function for each AssetID and SCADA_GUID
for i in range(len(df_assetID)):
    get_wo(df_assetID.at[i, 'AssetID'], df_assetID.at[i, 'SCADA_GUID'])
