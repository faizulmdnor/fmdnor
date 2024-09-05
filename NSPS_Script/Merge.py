from fleet_performance import CMMS, Site
from fleet_performance.interfaces.cmms.helpers.enums import WorkOrderStatus

site = Site('IRS1')
cmms = CMMS(superuser=True, environment='production')

inv = site.inverter_metadata
inverters = inv[['AssetTitle', 'Make_Model']]

wo_by_sites = cmms.get_work_orders_by_global_fed_site_id(
    asset_id=site.SiteAssetID,
    status=WorkOrderStatus.Open,
    include_asset_line_item=True
)

print(wo_by_sites.columns)

wo_by_sites = wo_by_sites[
    ['workOrderNumber', 'origination', 'reportedBy', 'createdDate', 'description', 'longDescription', 'status', 'statusDate',
     'assetTitle', 'assetDescription', 'assetType']]

wo_by_sites.to_csv(f'{site.title}_work_order.csv', index=False)


