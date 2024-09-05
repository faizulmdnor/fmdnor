from fleet_performance import CMMS
from fleet_performance.interfaces.cmms.helpers.enums import WorkOrderStatus

cmms = CMMS(superuser=True, environment='production')

wonum = ['00565021', '00559114', '00561444', '00561445', '00565085', '00561446', '00558798', '00558808', '00558821',
         '00558797', '00558777', '00558916', '00558826', '00558828', '00558807', '00558795', '00558824', '00558810',
         '00558779', '00558784', '00558773', '00558812', '00558804', '00558820', '00558763', '00558764', '00558776',
         '00558925', '00558806', '00558834', '00558819', '00558825', '00558818', '00558800', '00558778', '00558811',
         '00558823', '00558817', '00558833', '00558917', '00558803', '00558921', '00558770', '00558792', '00558814',
         '00558767', '00558919', '00558920', '00558923', '00558799', '00558766', '00558787', '00558789', '00558831',
         '00558924', '00558765', '00558802', '00558815', '00558829', '00558793', '00558771', '00558922', '00558813',
         '00558915', '00558790', '00558781', '00558816', '00558918', '00558775', '00558830', '00558832', '00558768',
         '00558809', '00558827', '00558952', '00558942', '00558944', '00558943', '00558945', '00558951', '00558948',
         '00558949', '00558950', '00558947', '00565444']

df_wo = cmms.get_work_orders_by_work_order_numbers(work_order_numbers=wonum)

for i, r in df_wo.iterrows():
    print('\n', df_wo.workOrderNumber[i], '\n', df_wo.workOrderId[i], '\n', df_wo.createdDate[i], '\n',
    df_wo.description[i], '\n', df_wo.status[i])

df2_wo = df_wo[['workOrderNumber', 'workOrderId', 'status', 'plantName','reportedBy']]
print(df2_wo)

openStatus = cmms.get_statuses(WorkOrderStatus.Open)
closeStatus = cmms.get_statuses(WorkOrderStatus.Close)
