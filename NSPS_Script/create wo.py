
# from fleet_performance.interfaces.cmms.maximo_interface import MaximoInterface
from fleet_performance import CMMS
import pandas as pd

# assign maximo interface to a variable, mi and specify variables superuser {True, False} and environment {'production, 'qa'}
# note: you will be prompted to log in with maximo username and password
# mi = MaximoInterface(superuser=False, environment='production')
cmms = CMMS(superuser=True, environment='production')

# read the workorders from CSV file. Ensure the CSV file is in same folder where this code is running
workorders = pd.read_csv('workorders_bulk_create.csv');
nwonum =['WORKORDERID','WONUM']
# iterate through the CSV file for each asset and create the workorder
for index, wo in workorders.iterrows():
    print(f'Creating Workorder for {wo.AssetTitle}, {wo.AssetID}, {wo.Description}')
    response = cmms.create_work_order(
        asset_id=str(wo.AssetTitle),
        description=wo.Description,
        detailed_description=wo.Long_Description
    )
    wo=pd.Series(response.json()['WORKORDER'])
    nwonum.loc[index, 'WORKORDERID'] = wo.workorderid.astype(str)
    nwonum.loc[index, 'WONUM'] = wo.wonum.astype(str)
    print(f'Created Workorder {wo.AssetTitle}, {wo.AssetID}, {wo.Description}')
nwonum.to_csv('new_WO_created.csv')