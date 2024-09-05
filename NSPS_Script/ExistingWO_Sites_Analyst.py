from fleet_performance import CMMS, Site
import pandas as pd
import numpy as np
from fleet_performance.interfaces.cmms.helpers.enums import WorkOrderStatus
from datetime import datetime
from pathlib import Path


now = datetime.now()
n = now.strftime('%Y%m%d%H%M')
cmms = CMMS(superuser=True, environment='production')
inputfile = 'C:/Users/FaizulBinMdNor/Documents/fleet_performance_Sales_Force_1.4.2/Work Order Salesforce/region_1.csv'
data = pd.read_csv(inputfile)


def get_wo_by_sites(sites_list, analyst):
    workorders_allsites = pd.DataFrame(None)

    for site in sites_list:
        try:
            s = Site(site)
            msg = f'{s.SCADA_GUID} does not have open WO.'
            workorders = cmms.get_work_orders_by_global_fed_site_id(asset_id=s.SiteAssetID,
                                                                status=WorkOrderStatus.Open,
                                                                include_asset_line_item=True)
        except ValueError as e:
            print(e)

        if not workorders.empty:
            workorders = workorders[
                ['workOrderId', 'maximoWorkOrderId', 'workOrderNumber', 'maximoWorkOrder', 'siteId', 'plantName',
                 'origination', 'createdByName', 'reportedBy', 'createdDate', 'description', 'longDescription',
                 'assetId',
                 'workTypeCategory', 'workType', 'statusDate', 'status', 'completedDate', 'assetType', 'assetTitle',
                 'assetLineItems']]

            workorders = workorders.explode('assetLineItems', ignore_index=True)



        else:
            print(msg)
            workorders = pd.DataFrame(columns=['workOrderId', 'maximoWorkOrderId', 'workOrderNumber', 'maximoWorkOrder',
                                               'siteId', 'plantName', 'origination', 'createdByName', 'reportedBy',
                                               'createdDate', 'description', 'longDescription', 'assetId',
                                               'workTypeCategory', 'workType', 'statusDate', 'status', 'completedDate',
                                               'assetType', 'assetTitle',
                                               'assetLineItems'])
            workorders['plantName'] = s.title
            workorders['status'] = msg

        outputfile = f'{analyst}/{s.SCADA_GUID}_{s}_open_workorders_{n}.csv'
        # workorders.to_csv(outputfile, index=False)
        workorders_allsites = pd.concat([workorders, workorders_allsites])

    workorders_allsites.to_csv(f'{analyst}/{analyst}_sites_open_workorders.csv', index=False)
    return workorders_allsites


analysts = data[data['Analyst'] != 'End']
analysts_list = analysts['Analyst'].unique().tolist()

for i in analysts_list:
    Path(i).mkdir(parents=True, exist_ok=True)
    analysts_site = data[data['Analyst'] == i]
    sites_list = analysts_site['scada_guid'].unique().tolist()
    sites_workorders = get_wo_by_sites(sites_list, i)
    sites_workorders['createdDate2'] = pd.to_datetime(sites_workorders['createdDate'])
    sites_workorders['createdDate2'] = sites_workorders['createdDate2'].apply(lambda x: x.date())
    sites_workorders['createdDate2'] = sites_workorders['createdDate2'].apply(lambda x: x.strftime('%Y-%m-%d'))

    irflyover_wo = sites_workorders[sites_workorders['origination'] == 'IR AERIAL'].reset_index(drop=True)
    irflyover_wo.to_csv(f'{i}/IRFlyover_WO.csv')

    data_analysis_wo = sites_workorders[
        (sites_workorders['origination'] == 'Data Analysis') & (sites_workorders['assetTitle'] != '')].reset_index(
        drop=True)

    field_analysis_wo = sites_workorders[
        (sites_workorders['origination'] == 'Field Analysis') & (sites_workorders['assetTitle'] != '')].reset_index(
        drop=True)

    data_analysis_wo_dup = data_analysis_wo[data_analysis_wo.duplicated('assetTitle', keep=False)]
    data_analysis_wo_dup = data_analysis_wo_dup.sort_values(by=['assetTitle', 'createdDate'], ascending=False).reset_index(drop=True)
    data_analysis_wo_dup.to_csv(f'{i}/data_analysis_WO_duplicate_assetTitle_{n}.csv')

    field_analysis_wo_dup = field_analysis_wo[field_analysis_wo.duplicated('assetTitle', keep=False)]
    field_analysis_wo_dup = field_analysis_wo_dup.sort_values(by='assetTitle', ascending=False).reset_index(drop=True)
    field_analysis_wo_dup.to_csv(f'{i}/field_analysis_WO_duplicate_assetTitle_{n}.csv')

    duplicate_wo = pd.concat([data_analysis_wo_dup, field_analysis_wo_dup])
    duplicate_wo_IR_flyover = duplicate_wo.loc[duplicate_wo.assetTitle.isin(irflyover_wo.assetLineItems)].reset_index(drop=True)
    duplicate_wo_IR_flyover.to_csv(f'{i}/duplicate_wo_IR_flyover.csv')
