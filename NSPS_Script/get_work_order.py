import pandas as pd
from fleet_performance import CMMS, Site
from datetime import datetime

from fleet_performance.interfaces.cmms.helpers.enums import WorkOrderStatus

today = datetime.today()
t = today.strftime('%Y-%m-%d%H:%M')

cmms = CMMS(superuser=True, environment='production')
s = Site('PRS2')

wo = cmms.get_work_orders_by_global_fed_site_id(asset_id=s.SiteAssetID,)
ir_wo = wo[['workOrderId', 'origination', 'reportedBy', 'dateReceived', 'createdDate', 'status']]
ir_wo = ir_wo[(ir_wo['origination'] == 'IR AERIAL') & (ir_wo['reportedBy'] == 'FaizulBinMdNor')]
ir_wo.to_csv(f'{s.title}_{s.SCADA_GUID}_WORKORDERID-OPEN.csv')

list_workOrderId = ir_wo['workOrderId'].to_list()
canceledWo = []
df_canceledWo = pd.DataFrame()
for w in list_workOrderId:
    print(w)
    cmms.update_status(work_order_id=w,
                       status=WorkOrderStatus.Open)
    print(f'WorkOrderId {w} status changed to Cancel.')

    cancelWo = cmms.get_work_order_by_work_order_id(work_order_id=w,)
    cancelWo = cancelWo[['workOrderId', 'origination', 'reportedBy', 'dateReceived', 'createdDate', 'status']]
    cancelWo = cancelWo.T
    canceledWo.append(cancelWo)
    df_canceledWo = pd.concat([df_canceledWo, cancelWo])

dff_canceledWo = df_canceledWo
dff_canceledWo.to_csv(f'{s.title}_{s.SCADA_GUID}_canceledWO.csv')
