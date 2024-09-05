from fleet_performance import CMMS
import pandas as pd
from fleet_performance.interfaces.cmms.helpers.enums import WorkOrderStatus
from datetime import datetime

today = datetime.today()
d = today.strftime('%Y%m%d')
# Assuming 'cmms', 'POA_outlier_data', and 'WS_assetTitle_list' are already defined
cmms = CMMS(environment='production', superuser=True)

region3_file = 'C:/Users/FaizulBinMdNor/OneDrive - NovaSource Power Services/Documents/Monthly Reporting/Monthly Reporting Checklist.xlsx'
worksheet = 'Jan 2024'
region3 = pd.read_excel(region3_file, sheet_name=worksheet)
region3 = region3.drop(index=range(0, 8))
region3.reset_index(drop=True, inplace=True)
region3.columns = region3.iloc[0]
region3 = region3.drop(index=0).reset_index(drop=True)

analyst = region3[['Analysts', 'SCADA GUID', 'SiteAssetTitle']]

POA_outlier_path = 'C:/Users/FaizulBinMdNor/Documents/fleet_performance_Sales_Force_1.4.2/Python_Script/'
POA_outlier_file = 'POA Outliers Summary.csv'
POA_outlier_data = pd.read_csv(POA_outlier_path + POA_outlier_file)
POA_outlier_data['Scada_Guid'] = POA_outlier_data['Name'].str[:4]
WS_assetTitle_list = POA_outlier_data['Name'].tolist()
POA_outlier_data = pd.merge(POA_outlier_data, analyst, left_on='Scada_Guid', right_on='SCADA GUID')

# Retrieve work orders
WS_WO = cmms.get_work_orders_by_asset_titles(asset_titles=WS_assetTitle_list,
                                             status=WorkOrderStatus.Open)

# Group by 'assetTitle' and aggregate 'workOrderNumber' as lists
WS_WO_groupby_assetTitle = WS_WO.groupby(['assetTitle'])['workOrderNumber'].agg(list).reset_index()
WS_WO_groupby_assetTitle.rename(columns={'workOrderNumber': 'Existing WOnum'}, inplace=True)

# Display the grouped DataFrame
print(WS_WO_groupby_assetTitle)

# Merge with POA_outlier_data
POA_outlier_WO_Exist = pd.merge(POA_outlier_data, WS_WO_groupby_assetTitle,
                                left_on='Name', right_on='assetTitle', how='outer')

# Display the merged DataFrame
POA_outlier_WO_Exist = POA_outlier_WO_Exist[['Date', 'Name', 'Category', 'Analysts', 'SCADA GUID', 'SiteAssetTitle', 'Existing WOnum']]
POA_outlier_WO_Exist = POA_outlier_WO_Exist.dropna(subset=['Date'])

POA_WS = POA_outlier_WO_Exist['Name'].tolist()
for i in range(len(POA_WS)):
    try:
        SF_WS = cmms.get_asset_by_title(POA_WS[i])
        e = SF_WS['assetId']
        print(e)
        POA_outlier_WO_Exist.loc[i, 'SF assetId'] = e
    except:
        e = f'{POA_WS[i]} not in Sales Force.'
        print(e)
        POA_outlier_WO_Exist.loc[i, 'SF assetId'] = e

POA_outlier_WO_Exist.to_csv(f'{d}_{POA_outlier_file}', index=False)
