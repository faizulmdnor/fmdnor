from fleet_performance.core.configs.paths import DATA_DIRECTORY, LOGS_DIRECTORY
from fleet_performance.interfaces.cmms import CMMS
from fleet_performance import Fleet
from datetime import datetime, timedelta, timezone
import pandas as pd
from dateutil import parser
from fleet_performance.core.utils.dev.loggers import get_logger
import functools as ft
import os
from fleet_performance.core.utils.email import send_via_smtp
from fleet_performance.core.configs import FPP_ENVIRONMENT
from fleet_performance.core.helpers.enums import Environment
import numpy as np
import math
import matplotlib
import matplotlib.pyplot as plt
from fleet_performance.helpers.data_acquisition.utilities import get_dataframe
import multiprocessing as mp
# from concurrent.futures import ThreadPoolExecutor

logger = get_logger(os.path.splitext(os.path.basename(__file__))[0])
matplotlib.use('TkAgg')

DEVELOPER_EMAIL_LIST = [
    'faizul.mdnoor@novasourcepower.com'
]
from_email = 'BusinessAnalytics@novasourcepower.com'


def flow_from_df(dataframe: pd.DataFrame, chunk_size: int = 10):
    for start_row in range(0, dataframe.shape[0], chunk_size):
        end_row = min(start_row + chunk_size, dataframe.shape[0])
        yield dataframe.iloc[start_row:end_row, :]


def get_critical_customers():
    critical_customers = ['Southern Power Company', 'Arevon', 'Total Energies', 'Silicon Ranch Corporation',
                          'Longroad Energy Partners', 'Orsted', 'Lightsource BP', 'EDP Renewables North America',
                          'Vesper Energy', 'Excelsior', 'Shell', 'Enbridge Green Energy', 'Swift Current Energy',
                          'Plus Power', 'Intersect', 'Key Capture Energy', 'Idemitsu', 'NTUA', 'Doral',
                          'Solar Proponent', 'Power']
    # TODO are these critical customers missing? Doral, Solar Proponent, Power
    return critical_customers


def get_non_supply_chain_product_request_statuses():
    statuses = ['Received', 'Draft', 'Requested', 'Pending Purchase Approval', 'More Info Requested',
                'Rejected Purchase Approval: For Review']
    return statuses


def get_escalations_dict():
    escalations_dict = {'1a': [1, 2, 3, 4, 5, 'major lost energy'],
                        '1b': [3, 5, 7, 10, 14, 'mid lost energy'],
                        '1c': [10, 15, 20, 30, 40, 'minor lost energy'],
                        '2a': [1, 2, 3, 4, 5, 'major capacity offline'],
                        '2b': [3, 5, 7, 10, 14, 'mid capacity offline'],
                        '2c': [10, 15, 20, 30, 40, 'minor capacity offline'],
                        '3': [-10, -5, 0, 30, 60, 'past due PMs'],
                        '4': [15, 21, 30, 45, 60, 'delayed priority 2-6'],
                        '5': [21, 30, 45, 60, 75, 'delayed priority 0-1'],
                        '6': [30, 60, 90, 120, 150, 'other']}
    return escalations_dict


def attempt_function(fun, *args):
    for attempt in range(10):
        try:
            result = fun(*args)
        except:
            logger.exception(f'{fun} failed - attempt {attempt + 1}')
            continue
        break
    return result


def get_operator_log_work_orders(work_order_numbers):
    sql_query = f'''
    -- get the operator log info for work orders. 1 row is returned for each work order. 
    -- When there are multiple logs for each work order, sum the lost energy and take the classification information from the log with the most lost energy
;with A as
   (select OperatorLogID,MaximoWONum
	 	 from [GlobalFED].[Alarms].[OperatorLogsWorkOrders] A with(nolock)     
	 where MaximoWONum in ('{"', '".join(list(map(str, work_order_numbers)))}')
	 	 )

SELECT work_order_number, lost_kWh_sum/1000.0 as lost_MWh, GADSRecordId ,GadsCodeCategory ,GADSCodeDescription ,OutageTypeTitle FROM
(
    SELECT 
    A.MaximoWONum as work_order_number
    ,B.GADSRecordId
    ,B.GadsCodeCategory
    ,B.GADSCodeDescription
    ,B.OutageTypeTitle
    ,B.TotalTODLostkWh
    ,SUM(B.TotalTODLostkWh) OVER (PARTITION BY A.MaximoWONum) as lost_kWh_sum
    ,ROW_NUMBER() OVER (PARTITION BY A.MaximoWONum ORDER BY B.TotalTODLostkWh DESC) as r         
    from A     
    left join [GlobalFED].Business.vwOperatorLogGADSHeaderRecords B 	 with(nolock) 
    on A.OperatorLogID = B.GADSRecordId
    and OutageTypeID not in (3451, 3452) -- exclude nuisance and comms     
)C     
where r = 1  

    '''

    return get_dataframe(sql_query)


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def process_work_orders_for_prioritization_reports(work_orders, cmms, fleet_metadata, start,
                                                   start_process=datetime.now()):
    escalations_dict = get_escalations_dict()

    columns = ['level_1', 'level_2', 'level_3', 'level_4', 'level_5', 'category_name']
    escalations = pd.DataFrame.from_dict(escalations_dict, orient='index', columns=columns).reset_index().rename(
        columns={'index': 'category'})

    def calculate_days_aging(row):
        delta = start_process - row['target_complete_date']
        return delta.days

    def calculate_work_order_age(row):
        delta = start_process - parser.parse(row['createdDate']).astimezone(timezone.utc)
        return delta.days + 1

    def calculate_days_in_current_status(row):
        delta = start_process - parser.parse(row['statusDate']).astimezone(timezone.utc)
        return delta.days

    def assign_categories(row):
        row['lost_MWh'] = pd.to_numeric(row['lost_MWh'])
        row['capacityOffline'] = pd.to_numeric(row['capacityOffline'])

        if row['workTypeCategory'] == 'UPP Preventative Maintenance' and row['suggestedMaintenanceDate'] is not None:
            return '3'
        elif row['lost_MWh'] >= 50:
            return '1a'
        elif row['capacityOffline'] >= 500:
            return '2a'
        elif row['lost_MWh'] >= 10:
            return '1b'
        elif row['capacityOffline'] >= 100:
            return '2b'
        elif row['lost_MWh'] >= 0:
            return '1c'
        elif row['capacityOffline'] > 0:
            return '2c'
        elif row['workTypeCategory'] not in ['UPP Special Projects', 'UPP Ancillary', 'UPP Internal',
                                             'UPP Training'] and row['wopriority_int'] >= 2:
            return '4'
        elif row['workTypeCategory'] not in ['UPP Special Projects', 'UPP Ancillary', 'UPP Internal',
                                             'UPP Training'] and row['wopriority_int'] < 2:
            return '5'
        elif row['workTypeCategory'] in ['UPP Special Projects', 'UPP Ancillary', 'UPP Internal', 'UPP Training']:
            return '6'

    def assign_escalations(row, days='escalation_days'):
        if row[days] > row['level_5']:
            return 5
        elif row[days] > row['level_4']:
            return 4
        elif row[days] > row['level_3']:
            return 3
        elif row[days] > row['level_2']:
            return 2
        elif row[days] > row['level_1']:
            return 1
        else:
            return 0

    def assign_sort(row):
        if row['critical_customer'] == 1:
            if row['category'] in ['1a', '1b', '1c', '2a', '2b', '2c']:
                return 1
            elif row['category'] in ['3']:
                return 2
            else:
                return 3
        else:
            if row['category'] in ['1a', '1b', '1c', '2a', '2b', '2c']:
                return 4
            elif row['category'] in ['3']:
                return 5
            else:
                return 6

    def assign_target_complete_date(row):
        if row['category'] == '3':
            target_complete_date = parser.parse(row['suggestedMaintenanceDate'] if row['suggestedMaintenanceDate'] is not None else datetime.today().strftime(
                '%Y-%m-%d')).astimezone(timezone.utc)
        else:
            target_complete_date = parser.parse(row['createdDate']).astimezone(timezone.utc) + timedelta(
                days=row['level_3'])
        return target_complete_date

    def assign_PIC(row, status='status'):
        if row['workTypeCategory'] == 'UPP Preventative Maintenance' and row[status] in ['New', 'Ready to Schedule']:
            return 'Planning/Scheduling'
        # TODO: add criteria no future service appointments for planning and scheduling PIC. remove PM criteria. Add statuses: 'Reschedule', 'Pricing/Material Review Needed', 'Scheduled', 'Scheduling in Progress'

        elif row['supply_chain_PR'] == 1 and row[status] in ['Pricing/Material Review Needed',
                                                             'Pending Materials/Equipment', 'Pending PO',
                                                             'Pending Warranty Provider',
                                                             'Pending Subcontractor Response']:
            return 'Supply Chain'

        elif row['schedStart'] is not None:
            return 'Technician'

        elif row[status] in ['Pending Customer Response', 'Pending Warranty Provider']:
            return 'Account Management'

        elif row[status] in ['Billing Review', 'Ready to Invoice']:
            return 'Billing'

        # TODO: Add criteria for Technical support needed status

        else:
            return 'Site Supervisor'

    def assign_is_active(row, status='status'):
        if row[status] in cmms.open_statuses:
            return 1
        else:
            return 0

    def assign_is_billable(row):
        return 0

    # Need to add field showing if work order is billable or not (entitlement?)

    def assign_is_past_due(row):
        if row['status'] in cmms.open_statuses and row['target_complete_date'] < start_process:
            return 1
        else:
            return 0

    def assign_is_past_due_30(row):
        if row['status_30'] in cmms.open_statuses and row['target_complete_date'] < start_datetime:
            return 1
        else:
            return 0

    def parse_date(row, column):
        if row[column] is None:
            return None
        else:
            return parser.parse(row[column]).astimezone(timezone.utc)

    escalation_names_dict = {'escalation': [5, 4, 3, 2, 1, 0],
                             'escalation_name': ['CEO', 'President', 'Region Manager', 'Area Manager',
                                                 'Site Supervisor', 'None']}
    escalation_names = pd.DataFrame(escalation_names_dict)

    # get the GADS Record Categories where applicable
    operator_log_work_orders = get_operator_log_work_orders(work_orders['workOrderNumber'])
    work_orders = work_orders.merge(operator_log_work_orders, how='left', left_on='workOrderNumber',
                                    right_on='work_order_number')

    # replace wopriority values and assign integers
    work_orders['wopriority'].fillna('1 - Not Affecting Energy/No Potential for Energy Loss', inplace=True)
    work_orders['wopriority'].replace({'Critical': '6 - Safety/Environmental/Contractual',
                                       'High': '5 - PVIS/Sunstation Energy Loss/Impending PVIS/Substation Energy Loss',
                                       'Normal': '3 - Inverter Energy Loss/Impending Inverter Energy Loss',
                                       'Low': '1 - Not Affecting Energy/No Potential for Energy Loss'}, inplace=True)
    work_orders['wopriority_int'] = work_orders['wopriority'].str[0].astype(int)

    # Get the work order histories (2000 work orders at a time due to API restriction)
    start_datetime = pd.to_datetime(start).tz_localize('UTC')
    work_order_histories = pd.DataFrame()

    max_rows = 2000
    iterations = math.ceil(len(work_orders) / max_rows)
    get_chunk = flow_from_df(work_orders, max_rows)
    for i in range(iterations):
        chunk = next(get_chunk)
        chunk_histories = attempt_function(cmms.get_work_orders_histories, 'status', chunk['workOrderId'].tolist())
        work_order_histories = pd.concat([work_order_histories, chunk_histories])

    work_order_histories.rename(columns={'newValue': 'status_30'}, inplace=True)

    work_order_histories.sort_values(by=['workOrderId', 'createdDate'], ascending=[True, False], inplace=True)
    work_order_histories['createdDate'] = pd.to_datetime(work_order_histories['createdDate'], errors='coerce')
    work_order_histories['status_end'] = work_order_histories.groupby('workOrderId')['createdDate'].shift().fillna(
        value=pd.Timestamp.utcnow())
    past_work_order_statuses = work_order_histories[
        (work_order_histories['createdDate'] < start_datetime) & (
                work_order_histories['status_end'] > start_datetime)].copy()
    past_work_order_statuses['days_in_current_status_30'] = (
            start_datetime - past_work_order_statuses['createdDate']).dt.days
    work_orders = pd.merge(work_orders,
                           past_work_order_statuses[['workOrderId', 'status_30', 'days_in_current_status_30']],
                           how='left',
                           on='workOrderId')

    # get product request, warranty, and purchase order info
    get_chunk = flow_from_df(work_orders, max_rows)
    chunk = pd.DataFrame()
    product_requests = pd.DataFrame()
    for i in range(iterations):
        chunk = next(get_chunk)
        chunk_product_requests = attempt_function(cmms.get_product_requests_by_work_order_ids,
                                                  chunk['workOrderId'].tolist())
        product_requests = pd.concat([product_requests, chunk_product_requests])

    non_supply_chain_product_request_statuses = get_non_supply_chain_product_request_statuses()
    if len(product_requests) > 0:
        product_requests.reset_index(inplace=True)
        product_requests['supply_chain_PR'] = product_requests.apply(
            lambda x: 1 if x['status'] not in non_supply_chain_product_request_statuses else 0, axis=1)
        product_requests = product_requests[product_requests['status'] != 'Canceled']
        # TODO add Filter on requesttype != warranty labor

    if len(product_requests) > 0:
        product_requests_grouped = product_requests[
            ['workOrderId', 'productRequestNumber', 'status', 'supply_chain_PR']].fillna('').groupby(
            'workOrderId', as_index=False).agg(
            {'productRequestNumber': ', '.join, 'status': ', '.join, 'supply_chain_PR': 'max'})
        warranties_grouped = product_requests[['workOrderId', 'warrantyNumber']].dropna().groupby('workOrderId',
                                                                                                  as_index=False).agg(
            ', '.join)
        purchase_orders_grouped = product_requests[['workOrderId', 'poNumber']].dropna().groupby('workOrderId',
                                                                                                 as_index=False).agg(
            ', '.join)
        product_requests_grouped.rename(columns={'productRequestNumber': 'PR_numbers', 'status': 'PR_statuses'},
                                        inplace=True)
        warranties_grouped.rename(columns={'warrantyNumber': 'warranty_numbers'}, inplace=True)
        purchase_orders_grouped.rename(columns={'poNumber': 'purchase_order_numbers'}, inplace=True)
        work_orders = pd.merge(work_orders, product_requests_grouped, on='workOrderId', how='left', suffixes=('', '_y'))
        work_orders = pd.merge(work_orders, warranties_grouped, on='workOrderId', how='left', suffixes=('', '_y'))
        work_orders = pd.merge(work_orders, purchase_orders_grouped, on='workOrderId', how='left', suffixes=('', '_y'))
    else:
        work_orders['PR_numbers'] = ''
        work_orders['PR_statuses'] = ''
        work_orders['warranty_numbers'] = ''
        work_orders['PR_numbers'] = ''
        work_orders['purchase_order_numbers'] = ''
        # TODO: if PO number is blank, replace using the PO field from the work order object
        work_orders['supply_chain_PR'] = 0

    # further calculations on work_orders
    work_orders['capacityOffline'] = pd.to_numeric(work_orders['capacityOffline'])
    work_orders['category'] = work_orders.apply(assign_categories, axis=1)
    work_orders = pd.merge(work_orders, escalations, how='left', on='category')
    work_orders['target_complete_date'] = work_orders.apply(assign_target_complete_date, axis=1)
    work_orders['escalation_days'] = work_orders.apply(calculate_days_aging, axis=1)
    work_orders['escalation_days_30'] = work_orders['escalation_days'] - 30

    work_orders.loc[work_orders['escalation_days_30'] < 0, 'escalation_days_30'] = np.nan
    work_orders['capacityOffline'] = work_orders['capacityOffline'].apply(pd.to_numeric, errors='ignore')

    work_orders['escalation'] = work_orders.apply(assign_escalations, axis=1)
    work_orders['escalation_30'] = work_orders.apply(lambda x: assign_escalations(x, days='escalation_days_30'), axis=1)
    work_orders = pd.merge(work_orders.dropna(subset='escalation'), escalation_names, how='left', on='escalation')
    work_orders = pd.merge(work_orders.dropna(subset='escalation_30'), escalation_names.rename(
        columns={'escalation': 'escalation_30', 'escalation_name': 'escalation_name_30'}), how='left',
                           on='escalation_30')

    work_orders['is_active'] = work_orders.apply(assign_is_active, axis=1)
    work_orders['is_active_30'] = work_orders.apply(lambda x: assign_is_active(x, status='status_30'), axis=1)
    work_orders['is_billable'] = work_orders.apply(assign_is_billable, axis=1)
    work_orders['is_past_due'] = work_orders.apply(assign_is_past_due, axis=1)
    work_orders['is_past_due_30'] = work_orders.apply(assign_is_past_due_30, axis=1)

    # calculate approximations for offline capacity and lost energy
    sun_hours_per_day = 6.0
    work_orders['work_order_age'] = work_orders.apply(calculate_work_order_age, axis=1)
    # work_orders['approx_lost_mwh'] = np.where(work_orders['lost_MWh'].notna(), work_orders['lost_MWh'], work_orders['capacityOffline'] / 1000 * sun_hours_per_day * work_orders['work_order_age'])  # assume 6 full sunlight hours per day, this is the approximate lost MWh for one day
    work_orders['approx_lost_mwh'] = np.where(
        work_orders['lost_MWh'] > work_orders['capacityOffline'] / 1000 * sun_hours_per_day * work_orders[
            'work_order_age'], work_orders['lost_MWh'],
        work_orders['capacityOffline'] / 1000 * sun_hours_per_day * work_orders['work_order_age'])

    work_orders['approx_offline_kw'] = work_orders['approx_lost_mwh'] * 1000 / sun_hours_per_day / work_orders[
        'work_order_age']

    work_orders['PIC'] = work_orders.apply(assign_PIC, axis=1)
    work_orders['PIC_30'] = work_orders.apply(lambda x: assign_PIC(x, status='status_30'), axis=1)
    work_orders['days_in_current_status'] = work_orders.apply(calculate_days_in_current_status, axis=1)
    work_orders['days_in_current_status_bins'] = pd.cut(work_orders['days_in_current_status'],
                                                        bins=[0, 30, 90, 180, 500],
                                                        labels=['a. <=30', 'b. 30-90', 'c. 90-180', 'd. 180-500'],
                                                        include_lowest=True, precision=0)
    work_orders['days_in_current_status_bins_30'] = pd.cut(work_orders['days_in_current_status_30'],
                                                           bins=[0, 30, 90, 180, 500],
                                                           labels=['a. <=30', 'b. 30-90', 'c. 90-180', 'd. 180-500'],
                                                           include_lowest=True, precision=0)
    work_orders = pd.merge(work_orders, fleet_metadata, how='left', left_on='plantName', right_on='AssetTitle')

    critical_customer = get_critical_customers()
    work_orders['critical_customer'] = work_orders.apply(lambda x: 1 if x['Customer_Name'] in critical_customer else 0,
                                                         axis=1)

    work_orders['sort'] = work_orders.apply(assign_sort, axis=1)

    work_orders.sort_values(by=['sort', 'approx_offline_kw', 'approx_lost_mwh', 'target_complete_date'],
                            ascending=[True, False, False, True], inplace=True)

    work_orders['completedDate'] = work_orders.apply(lambda x: parse_date(x, column='completedDate'), axis=1)

    return work_orders


def add_next_dataframe(dataframe, writer, sheet_name, startrow, startcol=0, index=False, summary_rows=0,
                       use_number_format=False, use_percentage_format=False, delta_icons=False,
                       progress_summary_format=False, title=None):
    dataframe.reset_index(inplace=True, drop=True)
    dataframe.to_excel(writer, sheet_name=sheet_name, startrow=startrow, startcol=startcol, index=index)

    cell_format_header = writer.book.add_format(
        {'bold': True, 'font_color': 'white', 'bg_color': '#006a76', 'align': 'center', 'text_wrap': True})
    cell_format_total_row = writer.book.add_format({'bold': True})
    cell_format_total_row_number = writer.book.add_format({'bold': True, 'num_format': '#,##0'})
    cell_format_number = writer.book.add_format({'num_format': '#,##0'})
    cell_format_percent = writer.book.add_format({'num_format': '0.0%'})
    cell_format_total_row_percent = writer.book.add_format({'bold': True, 'num_format': '0.0%'})
    cell_format_percent_high = writer.book.add_format({'num_format': '0.0%', 'bg_color': '#dba6ad'})
    cell_format_percent_mid = writer.book.add_format({'num_format': '0.0%', 'bg_color': '#ffeb9c'})
    cell_format_percent_low = writer.book.add_format({'num_format': '0.0%', 'bg_color': '#a4ccac'})
    cell_format_title = writer.book.add_format(
        {'bold': True, 'font_color': '#006a76', 'font_size': 12})
    cell_format_symbol_up = writer.book.add_format(
        {'font': 'Wingdings 3', 'font_color': '#d65532', 'align': 'center', 'bold': True})  # red
    cell_format_symbol_mid = writer.book.add_format(
        {'font': 'Wingdings 3', 'font_color': '#eac282', 'align': 'center', 'bold': True})  # yellow
    cell_format_symbol_down = writer.book.add_format(
        {'font': 'Wingdings 3', 'font_color': '#68a490', 'align': 'center', 'bold': True})  # green

    # Formatting
    if index is True:
        pad = 1 + startcol
    else:
        pad = 0 + startcol

    if title is not None:
        writer.sheets[sheet_name].write(startrow - 1, pad, title, cell_format_title)

    # apply format to header row
    for col_num, value in enumerate(dataframe.columns.values):
        writer.sheets[sheet_name].write(startrow, col_num + pad, value, cell_format_header)

    # apply number format
    if use_number_format:
        for index, row in dataframe.iterrows():
            for col_num, value in enumerate(row.values):
                if is_number(value):
                    writer.sheets[sheet_name].write(startrow + index + 1, col_num + pad, value, cell_format_number)

    if use_percentage_format:
        for index, row in dataframe.iloc[:, :-2].iterrows():
            for col_num, value in enumerate(row.values):
                if is_number(value):
                    writer.sheets[sheet_name].write(startrow + index + 1, col_num + pad, value, cell_format_percent)

    # apply format to total rows
    if summary_rows > 0:
        for index, row in dataframe.iloc[-summary_rows:].iterrows():
            for col_num, value in enumerate(row.values):
                if is_number(value):
                    if use_percentage_format and col_num != len(row) - 2:
                        writer.sheets[sheet_name].write(startrow + index + 1, col_num + pad, value,
                                                        cell_format_total_row_percent)
                    else:
                        writer.sheets[sheet_name].write(startrow + index + 1, col_num + pad, value,
                                                        cell_format_total_row_number)
                else:
                    writer.sheets[sheet_name].write(startrow + index + 1, col_num + pad, value, cell_format_total_row)

    if delta_icons is not False:
        for col_num, col in enumerate(dataframe.columns):
            if '_delta' in col:
                for index, row in dataframe.iterrows():
                    # font is 'Wingdings 3' for custom symbols and colors
                    if row[col] >= 0.2:
                        writer.sheets[sheet_name].write(startrow + index + 1, col_num + pad, 'h',
                                                        cell_format_symbol_up)
                    elif row[col] >= 0.1:
                        writer.sheets[sheet_name].write(startrow + index + 1, col_num + pad, 'k',
                                                        cell_format_symbol_mid)
                    elif row[col] >= -0.1:
                        writer.sheets[sheet_name].write(startrow + index + 1, col_num + pad, 'g',
                                                        cell_format_symbol_mid)
                    elif row[col] >= -0.2:
                        writer.sheets[sheet_name].write(startrow + index + 1, col_num + pad, 'm',
                                                        cell_format_symbol_mid)
                    elif row[col] < -0.2:
                        writer.sheets[sheet_name].write(startrow + index + 1, col_num + pad, 'i',
                                                        cell_format_symbol_down)

    if progress_summary_format:
        for col in dataframe.iloc[:, -2:]:
            writer.sheets[sheet_name].conditional_format(startrow + 1, dataframe.columns.get_loc(col) + pad,
                                                         startrow + len(dataframe.index),
                                                         dataframe.columns.get_loc(col) + pad,
                                                         {'type': 'cell', 'criteria': '>=', 'value': 0.2,
                                                          'format': cell_format_percent_high})
            writer.sheets[sheet_name].conditional_format(startrow + 1, dataframe.columns.get_loc(col) + pad,
                                                         startrow + len(dataframe.index),
                                                         dataframe.columns.get_loc(col) + pad,
                                                         {'type': 'cell', 'criteria': '>=', 'value': 0.05,
                                                          'format': cell_format_percent_mid})
            writer.sheets[sheet_name].conditional_format(startrow + 1, dataframe.columns.get_loc(col) + pad,
                                                         startrow + len(dataframe.index),
                                                         dataframe.columns.get_loc(col) + pad,
                                                         {'type': 'cell', 'criteria': '<', 'value': 0.05,
                                                          'format': cell_format_percent_low})
    # return the start row for next dataframe
    next_start = startrow + len(dataframe.index) + 3
    return next_start


def run_prioritization_report(email, position, environment=FPP_ENVIRONMENT.value, cc_list=None):
    """
    Construct and send prioritization report. SMTP response will be a blank dict if there are no errors
    position can be 'OnM_Site_Manager', 'OnM_Area_Manager', 'OnM_Regional_Manager', 'OnM_Planner_Scheduler',
    'OnM_Account_Manager', or 'Supply_Chain, or 'fleet' or 'north_america'
    """

    start_process = datetime.now().astimezone(timezone.utc)
    logger.info(f'Initiating prioritization report at {start_process}. ({email}: {position})')

    today = start_process.strftime('%Y-%m-%d')
    start_datetime = start_process - timedelta(days=30)
    start = start_datetime.strftime('%Y-%m-%d')

    if position in ['Supply_Chain', 'fleet', 'north_america']:
        fleet = Fleet('In Operation', 'OnM_Project_Status')
    else:
        fleet = Fleet(email, position)

    fleet_metadata = fleet.get_summary_dataframe(
        columns=['AssetTitle', 'Customer_Name', 'OnM_Regional_Manager', 'OnM_Area_Manager', 'OnM_Site_Manager',
                 'Site Region', 'Capacity_AC']).rename(
        columns={'OnM_Regional_Manager': 'Region Manager', 'OnM_Area_Manager': 'Area Manager',
                 'OnM_Site_Manager': 'Site Supervisor'})
    fleet_metadata.sort_values(by='Capacity_AC', ascending=False, inplace=True)
    sites = fleet.site_asset_titles
    if len(sites) == 0:
        logger.exception(f'There are no sites for {email} - {position}')
        return

    cmms = CMMS(superuser=True, environment=FPP_ENVIRONMENT.value)
    work_orders = attempt_function(cmms.get_all_work_orders_by_site_titles, start, today, sites)

    if len(work_orders) == 0:
        logger.exception(f'There are no work orders to prioritize for {email} - {position}')
        return

    work_orders = process_work_orders_for_prioritization_reports(work_orders, cmms, fleet_metadata, start,
                                                                 start_process)

    # filter processed work_orders
    if position == 'OnM_Account_Manager':
        work_orders = work_orders[work_orders['PIC'] == 'Account Management']
    if position == 'OnM_Planner_Scheduler':
        work_orders = work_orders[work_orders['PIC'] == 'Planning/Scheduling']
    if position == 'Supply_Chain':
        work_orders = work_orders[work_orders['PIC'] == 'Supply Chain']

    # critical_customer = get_critical_customers()
    # work_orders = work_orders[work_orders['Customer_Name'].isin(critical_customer)]

    if position in ['north_america']:
        work_orders = work_orders[(work_orders['Site Region'] == 'NA') | (work_orders['Site Region'].isna())]

    if len(work_orders[work_orders['is_active'] == 1]) == 0:
        logger.exception(f'There are no active work orders to prioritize for {email} - {position}')
        return

    ###############################
    # create "days in current status" summary tables
    # Summarize active work orders by PIC
    pic_summary_int = pd.pivot_table(work_orders[work_orders['is_active'] == 1], values='workOrderId', index=['PIC'],
                                     columns=['days_in_current_status_bins'], aggfunc='count',
                                     margins=True).fillna(0)
    pic_summary = pic_summary_int[pic_summary_int.index != 'All'].sort_values(by='All', ascending=False)
    pic_summary = pd.concat([pic_summary, pic_summary_int[pic_summary_int.index == 'All'].fillna(0)]).rename(
        columns={'All': 'total'})

    # summarize work orders active 30 days ago by PIC
    if len(work_orders[work_orders['is_active_30'] == 1]) != 0:
        pic_summary_int_30 = pd.pivot_table(work_orders[work_orders['is_active_30'] == 1], values='workOrderId',
                                            index=['PIC_30'],
                                            columns=['days_in_current_status_bins_30'], aggfunc='count',
                                            margins=True).fillna(0)
        pic_summary_30 = pic_summary_int_30[pic_summary_int_30.index != 'All'].sort_values(by='All', ascending=False)
        pic_summary_30 = pd.concat(
            [pic_summary_30, pic_summary_int_30[pic_summary_int_30.index == 'All'].fillna(0)]).rename(
            columns={'All': 'total'})
    else:
        pic_summary_30 = pd.DataFrame(0.0, columns=pic_summary.columns, index=pic_summary.index)

    # compare the current PIC summary with the past PIC summary
    pic_summary_delta = pic_summary.div(pic_summary_30).sub(1)
    pic_summary_with_values_and_deltas = pd.merge(pic_summary, pic_summary_delta, how='left', left_index=True,
                                                  right_index=True, suffixes=('', '_delta'))
    cols = sorted(pic_summary_with_values_and_deltas.columns.tolist())
    pic_summary_with_values_and_deltas = pic_summary_with_values_and_deltas[cols].replace({np.nan: 0, np.inf: 2})

    # reset indexes (for report formatting)
    pic_summary_with_values_and_deltas.reset_index(inplace=True)
    pic_summary_with_values_and_deltas.rename(columns={'PIC': 'WO Count (by PIC and Days in Status)'}, inplace=True)

    ####################################
    # Summarize active work orders by status
    status_summary_int = pd.pivot_table(work_orders[work_orders['is_active'] == 1], values='workOrderId',
                                        index=['status'], columns=['days_in_current_status_bins'], aggfunc='count',
                                        margins=True).fillna(0)
    status_summary = status_summary_int[status_summary_int.index != 'All'].sort_values(by='All', ascending=False)
    status_summary = pd.concat([status_summary, status_summary_int[status_summary_int.index == 'All']]).rename(
        columns={'All': 'total'})

    # summarize work orders active 30 days ago by PIC
    if len(work_orders[work_orders['is_active_30'] == 1]) != 0:
        status_summary_int_30 = pd.pivot_table(work_orders[work_orders['is_active_30'] == 1], values='workOrderId',
                                               index=['status'], columns=['days_in_current_status_bins_30'],
                                               aggfunc='count',
                                               margins=True).fillna(0)
        status_summary_30 = status_summary_int_30[status_summary_int_30.index != 'All'].sort_values(by='All',
                                                                                                    ascending=False)
        status_summary_30 = pd.concat(
            [status_summary_30, status_summary_int_30[status_summary_int_30.index == 'All']]).rename(
            columns={'All': 'total'})
    else:
        status_summary_30 = pd.DataFrame(0.0, columns=status_summary.columns, index=status_summary.index)

    # compare the current status summary with the past status summary
    status_summary_delta = status_summary.div(status_summary_30).sub(1)
    status_summary_with_values_and_deltas = pd.merge(status_summary, status_summary_delta, how='left', left_index=True,
                                                     right_index=True, suffixes=('', '_delta'))
    cols = sorted(status_summary_with_values_and_deltas.columns.tolist())
    status_summary_with_values_and_deltas = status_summary_with_values_and_deltas[cols].replace({np.nan: 0, np.inf: 2})

    # reset indexes (for report formatting)
    status_summary_with_values_and_deltas.reset_index(inplace=True)
    status_summary_with_values_and_deltas.rename(columns={'status': 'WO Count (by Status and Days in Status)'},
                                                 inplace=True)

    # create "work order count" summary tables
    # by customer
    is_active_by_customer = pd.pivot_table(work_orders[work_orders['is_active'] == 1], values='workOrderId',
                                           index=['Customer_Name'], columns=[], aggfunc='count', margins=True).rename(
        columns={'workOrderId': 'is_active'})
    is_active_by_customer_30 = pd.pivot_table(work_orders[work_orders['is_active_30'] == 1], values='workOrderId',
                                              index=['Customer_Name'], columns=[], aggfunc='count',
                                              margins=True).rename(
        columns={'workOrderId': 'is_active'})
    is_past_due_by_customer = pd.pivot_table(work_orders[work_orders['is_past_due'] == 1], values='workOrderId',
                                             index=['Customer_Name'], columns=[], aggfunc='count', margins=True).rename(
        columns={'workOrderId': 'is_past_due'})
    is_past_due_by_customer_30 = pd.pivot_table(work_orders[work_orders['is_past_due_30'] == 1], values='workOrderId',
                                                index=['Customer_Name'], columns=[], aggfunc='count',
                                                margins=True).rename(
        columns={'workOrderId': 'is_past_due'})
    # is_billable_by_customer = pd.pivot_table(work_orders[work_orders['is_billable'] == 1], values='workOrderId', index=['Customer_Name'], columns=['is_billable'], aggfunc='count', margins=True).drop(axis=1, columns='All').rename(columns={1: 'is_billable'})
    capacity_offline_by_customer = pd.pivot_table(work_orders[work_orders['is_active'] == 1],
                                                  values=['capacityOffline'], index=['Customer_Name'], columns=[],
                                                  aggfunc='sum', margins=True)
    capacity_offline_by_customer_30 = pd.pivot_table(work_orders[work_orders['is_active_30'] == 1],
                                                     values=['capacityOffline'], index=['Customer_Name'], columns=[],
                                                     aggfunc='sum', margins=True)
    dfs = [is_active_by_customer, is_past_due_by_customer, capacity_offline_by_customer]
    dfs_30 = [is_active_by_customer_30, is_past_due_by_customer_30, capacity_offline_by_customer_30]
    customer_summary_int = ft.reduce(
        lambda left, right: pd.merge(left, right, how='outer', left_index=True, right_index=True), dfs)
    customer_summary_int_30 = ft.reduce(
        lambda left, right: pd.merge(left, right, how='outer', left_index=True, right_index=True),
        dfs_30)
    customer_summary = customer_summary_int[customer_summary_int.index != 'All'].sort_values(by='capacityOffline',
                                                                                             ascending=False)
    customer_summary_30 = customer_summary_int_30[customer_summary_int_30.index != 'All'].sort_values(
        by='capacityOffline',
        ascending=False)
    customer_summary = pd.concat(
        [customer_summary, customer_summary_int[customer_summary_int.index == 'All']])
    customer_summary_30 = pd.concat(
        [customer_summary_30, customer_summary_int_30[customer_summary_int_30.index == 'All']])

    # TODO: these deltas do not display correctly if there are no active work orders thirty days ago
    customer_summary_delta = customer_summary.div(customer_summary_30).sub(1)
    customer_summary_with_values_and_deltas = pd.merge(customer_summary, customer_summary_delta, how='left',
                                                       left_index=True,
                                                       right_index=True, suffixes=('', '_delta'))
    cols = sorted(customer_summary_with_values_and_deltas.columns.tolist())
    customer_summary_with_values_and_deltas = customer_summary_with_values_and_deltas[cols].replace(
        {np.nan: 0, np.inf: 2}).reset_index()
    customer_summary_with_values_and_deltas.rename(
        columns={'is_active': 'active work orders', 'is_active_delta': 'active work orders_delta',
                 'is_past_due': 'past due work orders', 'is_past_due_delta': 'past due work orders_delta'},
        inplace=True)

    # by supervisor
    is_active_by_supervisor = pd.pivot_table(work_orders[work_orders['is_active'] == 1], values='workOrderId',
                                             index=['Site Supervisor'], columns=[], aggfunc='count',
                                             margins=True).rename(columns={'workOrderId': 'is_active'})
    is_active_by_supervisor_30 = pd.pivot_table(work_orders[work_orders['is_active_30'] == 1], values='workOrderId',
                                                index=['Site Supervisor'], columns=[], aggfunc='count',
                                                margins=True).rename(columns={'workOrderId': 'is_active'})
    is_past_due_by_supervisor = pd.pivot_table(work_orders[work_orders['is_past_due'] == 1], values='workOrderId',
                                               index=['Site Supervisor'], columns=[], aggfunc='count',
                                               margins=True).rename(columns={'workOrderId': 'is_past_due'})
    is_past_due_by_supervisor_30 = pd.pivot_table(work_orders[work_orders['is_past_due_30'] == 1], values='workOrderId',
                                                  index=['Site Supervisor'], columns=[], aggfunc='count',
                                                  margins=True).rename(columns={'workOrderId': 'is_past_due'})
    # is_billable_by_supervisor = pd.pivot_table(work_orders[work_orders['is_billable'] == 1], values='workOrderId', index=['Site_Supervisor'], columns=['is_billable'], aggfunc='count', margins=True).drop(axis=1, columns='All').rename(columns={1: 'is_billable'})
    capacity_offline_by_supervisor = pd.pivot_table(work_orders[work_orders['is_active'] == 1],
                                                    values=['capacityOffline'], index=['Site Supervisor'], columns=[],
                                                    aggfunc='sum',
                                                    margins=True)
    capacity_offline_by_supervisor_30 = pd.pivot_table(work_orders[work_orders['is_active_30'] == 1],
                                                       values=['capacityOffline'], index=['Site Supervisor'],
                                                       columns=[],
                                                       aggfunc='sum',
                                                       margins=True)
    dfs = [is_active_by_supervisor, is_past_due_by_supervisor, capacity_offline_by_supervisor]
    dfs_30 = [is_active_by_supervisor_30, is_past_due_by_supervisor_30, capacity_offline_by_supervisor_30]
    supervisor_summary_int = ft.reduce(
        lambda left, right: pd.merge(left, right, how='outer', left_index=True, right_index=True),
        dfs)
    supervisor_summary_int_30 = ft.reduce(
        lambda left, right: pd.merge(left, right, how='outer', left_index=True, right_index=True),
        dfs_30)
    supervisor_summary = supervisor_summary_int[supervisor_summary_int.index != 'All'].sort_values(by='capacityOffline',
                                                                                                   ascending=False)
    supervisor_summary_30 = supervisor_summary_int_30[supervisor_summary_int_30.index != 'All'].sort_values(
        by='capacityOffline',
        ascending=False)
    supervisor_summary = pd.concat(
        [supervisor_summary, supervisor_summary_int[supervisor_summary_int.index == 'All']])
    supervisor_summary_30 = pd.concat(
        [supervisor_summary_30, supervisor_summary_int_30[supervisor_summary_int_30.index == 'All']])
    supervisor_summary_delta = supervisor_summary.div(supervisor_summary_30).sub(1)
    supervisor_summary_with_values_and_deltas = pd.merge(supervisor_summary, supervisor_summary_delta, how='left',
                                                         left_index=True,
                                                         right_index=True, suffixes=('', '_delta'))
    cols = sorted(supervisor_summary_with_values_and_deltas.columns.tolist())
    supervisor_summary_with_values_and_deltas = supervisor_summary_with_values_and_deltas[cols].replace(
        {np.nan: 0, np.inf: 2}).reset_index()
    supervisor_summary_with_values_and_deltas.rename(
        columns={'is_active': 'active work orders', 'is_active_delta': 'active work orders_delta',
                 'is_past_due': 'past due work orders', 'is_past_due_delta': 'past due work orders_delta'},
        inplace=True)

    # prepare the table of work orders for the report
    details = work_orders[work_orders['is_active'] == 1][
        ['plantName', 'workOrderNumber', 'status', 'description', 'assetDescription', 'PIC', 'workTypeCategory',
         'workType', 'wopriority', 'capacityOffline', 'lost_MWh', 'createdDate', 'schedStart', 'target_complete_date',
         'days_in_current_status', 'escalation_name', 'PR_numbers', 'PR_statuses', 'purchase_order_numbers',
         'warranty_numbers', 'Region Manager',
         'Area Manager', 'Site Supervisor', 'Customer_Name']].rename(columns={'capacityOffline': 'offline_kW'})
    details['createdDate'] = details.apply(lambda x: x['createdDate'][:10], axis=1)
    details['schedStart'] = details.apply(lambda x: x['schedStart'][:10] if x['schedStart'] is not None else None,
                                          axis=1)
    details['target_complete_date'] = details.apply(lambda x: x['target_complete_date'].strftime('%Y-%m-%d'), axis=1)
    # TODO: add "person", "Bill Type", and PO values
    # TODO: add work order sub status

    ## create excel file
    # setup directory to receive results
    directory = DATA_DIRECTORY / 'prioritization_report' / f'prioritization_report {start_process.strftime("%Y-%m-%d")}'

    # make directory if it does not exist
    if not os.path.exists(directory):
        os.makedirs(directory)

    title = f'UPP Prioritization Report for {email.replace("@novasourcepower.com", "").replace(".", "_")} on {start_process.strftime("%Y-%m-%d")} {position}'
    sub_title = f'Prepared for {email.replace("@novasourcepower.com", "").replace(".", "_")} on {start_process.strftime("%Y-%m-%d")}'
    xlsx_name = title + '.xlsx'
    symbol_description = 'Note: The "_delta" columns show a comparison of the value compared to 30 days ago'

    logger.info(f'Generating {title}')

    xlsx_filepath = directory / xlsx_name
    writer = pd.ExcelWriter(xlsx_filepath, engine='xlsxwriter')
    workbook = writer.book

    pd.DataFrame().to_excel(writer, sheet_name='Summary', startrow=0, header=False,
                            index=False)  # create the sheet (blank dataframe
    worksheet = writer.sheets['Summary']
    worksheet.set_column(1, 19, 19)
    worksheet.set_column('A:A', 30)
    worksheet.set_row(0, 40)
    worksheet.insert_image('A1',
                           DATA_DIRECTORY / 'novasource_logo.png',
                           {"x_offset": 5, "y_offset": 5})
    cell_format_title = workbook.add_format({'bold': True, 'font_size': 20})
    cell_format_sub_title = workbook.add_format({'bold': True, 'font_size': 12})
    worksheet.write('D1', 'UPP Prioritization Report', cell_format_title)
    worksheet.write('D2', sub_title, cell_format_sub_title)
    worksheet.write('A4', symbol_description, cell_format_sub_title)

    r = 4  # skipped rows

    try:

        r = add_next_dataframe(status_summary_with_values_and_deltas, writer, sheet_name='Summary', startrow=r,
                               index=False,
                               summary_rows=1, use_number_format=True, delta_icons=True)
        r = add_next_dataframe(pic_summary_with_values_and_deltas, writer, sheet_name='Summary', startrow=r,
                               index=False,
                               summary_rows=1, use_number_format=True, delta_icons=True)
        r = add_next_dataframe(customer_summary_with_values_and_deltas, writer, sheet_name='Summary', startrow=r,
                               index=False,
                               summary_rows=1, use_number_format=True, delta_icons=True)
        if position not in ['OnM_Site_Manager']:
            r = add_next_dataframe(supervisor_summary_with_values_and_deltas, writer, sheet_name='Summary', startrow=r,
                                   index=False,
                                   summary_rows=1, use_number_format=True, delta_icons=True)

        # Add the detailed work order info to 'Work Orders' tab
        pd.DataFrame().to_excel(writer, sheet_name='Work Orders', startrow=0, header=False,
                                index=False)  # create the sheet (blank dataframe
        r = 0
        r = add_next_dataframe(details, writer, sheet_name='Work Orders', startrow=r, index=False,
                               summary_rows=0, use_number_format=False)
        writer.sheets['Work Orders'].freeze_panes(1, 2)
        writer.sheets['Work Orders'].set_column(0, 30, 25)

        writer.save()

    except Exception as e:
        logger.exception(f'There was an error generating {title}: {e}')

    # Generate email details
    if cc_list is None:
        cc_list = []
    if isinstance(cc_list, str):
        cc_list = [cc_list]

    if environment == Environment.PRODUCTION.value:
        email_list = [email]
        cc_list.append('BusinessAnalytics@novasourcepower.com')
        subject = title
    else:
        email_list = DEVELOPER_EMAIL_LIST
        cc_list = []
        subject = title + '*EMAIL TEST*'
    body = f'Please contact BusinessAnalytics@novasourcepower.com for questions regarding the report'

    smtp_response = send_via_smtp(email_list, from_str=from_email, cc_list=cc_list,
                                  subject=subject, body_html=body, attachment_filepath_list=[xlsx_filepath])
    logger.info(f'Email sent: {title}')

    return smtp_response


def run_escalation_report(email, position, environment=FPP_ENVIRONMENT.value, cc_list=None):
    """
    Construct and send escalation report. SMTP response will be a blank dict if there are no errors
    position can be 'OnM_Site_Manager', 'OnM_Area_Manager', 'OnM_Regional_Manager', 'Planning_Scheduling',
    'Account_Management', 'President', or 'CEO'
    """
    # define the escalation levels to display for each position
    escalation_thresholds = {'OnM_Site_Manager': 1, 'OnM_Area_Manager': 2, 'OnM_Regional_Manager': 3,
                             'President': 4, 'CEO': 5, 'Planning_Scheduling': 3, 'Account_Management': 3}

    start_process = datetime.now().astimezone(timezone.utc)

    logger.info(f'Initiating escalation report at {start_process}. ({email}: {position})')

    text_to_save = logger.info(f'Initiating escalation report at {start_process}. ({email}: {position})')

    with open(f'{LOGS_DIRECTORY}/{start_process}_log.txt', 'a') as file:
        file.write(text_to_save+'\n')


    today = start_process.strftime('%Y-%m-%d')
    start_datetime = start_process - timedelta(days=30)
    start = start_datetime.strftime('%Y-%m-%d')

    # get relevant work orders for report scope
    if position in ['Planning_Scheduling', 'Account_Management', 'President', 'CEO']:
        fleet = Fleet('In Operation', 'OnM_Project_Status')
    else:
        fleet = Fleet(email, position)

    fleet_metadata = fleet.get_summary_dataframe(
        columns=['AssetTitle', 'Customer_Name', 'OnM_Regional_Manager', 'OnM_Area_Manager', 'OnM_Site_Manager',
                 'Site Region', 'Capacity_AC']).rename(
        columns={'OnM_Regional_Manager': 'Region Manager', 'OnM_Area_Manager': 'Area Manager',
                 'OnM_Site_Manager': 'Site Supervisor'})
    sites = fleet.site_asset_titles
    fleet_metadata.sort_values(by='Capacity_AC', ascending=False, inplace=True)

    if len(sites) == 0:
        logger.exception(f'There are no sites for {email} - {position}')
        return

    cmms = CMMS(superuser=True, environment=FPP_ENVIRONMENT.value)
    work_orders = attempt_function(cmms.get_all_work_orders_by_site_titles, start, today, sites)

    if len(work_orders) == 0:
        logger.exception(f'There are no work orders to escalate for {email} - {position}')
        return

    work_orders = process_work_orders_for_prioritization_reports(work_orders, cmms, fleet_metadata, start,
                                                                 start_process)

    # # filter processed work_orders
    work_orders = work_orders[(work_orders['escalation'] >= escalation_thresholds[position]) | (
            work_orders['escalation_30'] >= escalation_thresholds[position])]

    critical_customer = get_critical_customers()
    work_orders = work_orders[work_orders['Customer_Name'].isin(critical_customer)]

    if len(work_orders[work_orders['is_active'] == 1]) == 0:
        logger.exception(f'There are no active work orders to escalate for {email} - {position}')
        return

    ######################################
    # do this outside of the escalation function?
    ######################################
    # get relevant work orders for active fleet summary
    active_fleet = Fleet('In Operation', 'OnM_Project_Status')
    sites = active_fleet.site_asset_titles
    active_fleet_metadata = active_fleet.get_summary_dataframe(
        columns=['AssetTitle', 'Customer_Name', 'OnM_Regional_Manager', 'OnM_Area_Manager', 'OnM_Site_Manager',
                 'Site Region', 'Capacity_AC']).rename(
        columns={'OnM_Regional_Manager': 'Region Manager', 'OnM_Area_Manager': 'Area Manager',
                 'OnM_Site_Manager': 'Site Supervisor'})
    active_fleet_metadata.sort_values(by='Capacity_AC', ascending=False, inplace=True)

    active_fleet_work_orders = attempt_function(cmms.get_all_work_orders_by_site_titles, start, today, sites)
    active_fleet_work_orders = process_work_orders_for_prioritization_reports(active_fleet_work_orders,
                                                                              cmms, active_fleet_metadata,
                                                                              start, start_process)
    # filter active_fleet_work_orders for only the escalatable ones
    active_fleet_work_orders = active_fleet_work_orders[
        (active_fleet_work_orders['escalation'] >= escalation_thresholds[position]) | (
                active_fleet_work_orders['escalation_30'] >= escalation_thresholds[position])]
    ######################################

    # make the escalation summary
    escalation_summary_int = pd.pivot_table(
        work_orders[(work_orders['escalation'] >= escalation_thresholds[position]) & (work_orders['is_active'] == 1)],
        values='workOrderId',
        index=['plantName'],
        columns=['PIC'], aggfunc='count',
        margins=True).fillna(0)
    escalation_summary_percent = escalation_summary_int.div(escalation_summary_int.iloc[:, -1],
                                                            axis=0)  # calculate totals as percent of row
    escalation_summary_percent = escalation_summary_percent.drop(columns=['All'])
    escalation_summary_percent['All'] = escalation_summary_int['All']
    escalation_summary = escalation_summary_percent[escalation_summary_percent.index != 'All'].sort_values(
        by='All', ascending=False)
    escalation_summary = pd.concat(
        [escalation_summary, escalation_summary_percent[escalation_summary_percent.index == 'All'].fillna(0)]).rename(
        columns={'All': 'total'})

    # make the escalation summary for 30 days ago
    escalation_summary_int_30 = pd.pivot_table(
        work_orders[
            (work_orders['escalation_30'] >= escalation_thresholds[position]) & (work_orders['is_active_30'] == 1)],
        values='workOrderId',
        index=['plantName'],
        columns=['PIC'], aggfunc='count',
        margins=True).fillna(0)
    escalation_summary_percent_30 = escalation_summary_int_30.div(escalation_summary_int_30.iloc[:, -1],
                                                                  axis=0)  # calculate totals as percent of row
    escalation_summary_percent_30 = escalation_summary_percent_30.drop(columns=['All'])
    escalation_summary_percent_30['All'] = escalation_summary_int_30['All']
    escalation_summary_30 = escalation_summary_percent_30[escalation_summary_percent_30.index != 'All'].sort_values(
        by='All', ascending=False)
    escalation_summary_30 = pd.concat(
        [escalation_summary_30,
         escalation_summary_percent_30[escalation_summary_percent_30.index == 'All'].fillna(0)]).rename(
        columns={'All': 'total'})

    # compare the current escalation summary with the past escalation summary
    escalation_summary_delta = escalation_summary['total'].div(escalation_summary_30['total']).sub(1)
    escalation_summary_with_values_and_deltas = pd.merge(escalation_summary,
                                                         escalation_summary_delta.rename('total_delta'), how='left',
                                                         left_index=True,
                                                         right_index=True)
    cols = sorted(escalation_summary_with_values_and_deltas.columns.tolist())
    escalation_summary_with_values_and_deltas = escalation_summary_with_values_and_deltas[cols].replace(
        {np.nan: 0, np.inf: 2})

    # sort dataframe based on 'position' where relevant
    if position == 'Account_Management':
        escalation_summary_with_values_and_deltas = pd.concat([escalation_summary_with_values_and_deltas[
                                                                   escalation_summary_with_values_and_deltas.index != 'All'].sort_values(
            by='Account Management', ascending=False), escalation_summary_with_values_and_deltas[
                                                                   escalation_summary_with_values_and_deltas.index == 'All']])
    if position == 'Planning_Scheduling':
        escalation_summary_with_values_and_deltas = pd.concat([escalation_summary_with_values_and_deltas[
                                                                   escalation_summary_with_values_and_deltas.index != 'All'].sort_values(
            by='Planning/Scheduling', ascending=False), escalation_summary_with_values_and_deltas[
                                                                   escalation_summary_with_values_and_deltas.index == 'All']])

    # calculate the active_fleet total row
    active_fleet_escalation_summary_int = pd.pivot_table(
        active_fleet_work_orders[(active_fleet_work_orders['escalation'] >= escalation_thresholds[position]) & (
                    active_fleet_work_orders['is_active'] == 1)],
        values='workOrderId',
        index=['plantName'],
        columns=['PIC'], aggfunc='count',
        margins=True).fillna(0)
    active_fleet_escalation_summary_percent = active_fleet_escalation_summary_int.div(
        active_fleet_escalation_summary_int.iloc[:, -1],
        axis=0)  # calculate totals as percent of row
    active_fleet_escalation_summary_percent = active_fleet_escalation_summary_percent.drop(columns=['All'])
    active_fleet_escalation_summary_percent['total'] = active_fleet_escalation_summary_int['All']

    # calculate the active_fleet total row 30 days ago
    active_fleet_escalation_summary_int_30 = pd.pivot_table(
        active_fleet_work_orders[(active_fleet_work_orders['escalation_30'] >= escalation_thresholds[position]) & (
                    active_fleet_work_orders['is_active_30'] == 1)],
        values='workOrderId',
        index=['plantName'],
        columns=['PIC'], aggfunc='count',
        margins=True).fillna(0)
    active_fleet_escalation_summary_percent_30 = active_fleet_escalation_summary_int_30.div(
        active_fleet_escalation_summary_int_30.iloc[:, -1],
        axis=0)  # calculate totals as percent of row
    active_fleet_escalation_summary_percent_30 = active_fleet_escalation_summary_percent_30.drop(columns=['All'])
    active_fleet_escalation_summary_percent_30['total'] = active_fleet_escalation_summary_int_30['All']

    # compare the current escalation summary with the past escalation summary for 30 days ago
    active_fleet_escalation_summary_delta = active_fleet_escalation_summary_percent['total'].div(
        active_fleet_escalation_summary_percent_30['total']).sub(1)
    active_fleet_escalation_summary_with_values_and_deltas = pd.merge(active_fleet_escalation_summary_percent,
                                                                      active_fleet_escalation_summary_delta.rename(
                                                                          'total_delta'), how='left',
                                                                      left_index=True,
                                                                      right_index=True)
    cols = sorted(active_fleet_escalation_summary_with_values_and_deltas.columns.tolist())
    active_fleet_escalation_summary_with_values_and_deltas = active_fleet_escalation_summary_with_values_and_deltas[
        cols].replace(
        {np.nan: 0, np.inf: 2})

    # add the active_fleet total row to escalation_summary
    escalation_summary_with_values_and_deltas = pd.concat(
        [escalation_summary_with_values_and_deltas, active_fleet_escalation_summary_with_values_and_deltas[
            active_fleet_escalation_summary_with_values_and_deltas.index == 'All']]).fillna(0)

    cols = sorted(escalation_summary_with_values_and_deltas.columns.tolist())
    escalation_summary_with_values_and_deltas = escalation_summary_with_values_and_deltas[cols].replace(
        {np.nan: 0, np.inf: 2})
    escalation_summary_with_values_and_deltas.reset_index(inplace=True)
    escalation_summary_with_values_and_deltas.rename(columns={'plantName': 'Plant Name'}, inplace=True)
    escalation_summary_with_values_and_deltas.iloc[-1, 0] = 'Fleet Total'

    # add the region/area/plant manager metadata to the escalation summary
    escalation_summary_metadata = pd.merge(escalation_summary_with_values_and_deltas['Plant Name'], fleet_metadata,
                                           how='left',
                                           left_on=escalation_summary_with_values_and_deltas['Plant Name'].str.lower(),
                                           right_on=fleet_metadata['AssetTitle'].str.lower()).fillna('')
    escalation_summary_with_values_and_deltas.insert(1, 'Region Manager', escalation_summary_metadata['Region Manager'])
    escalation_summary_with_values_and_deltas.insert(2, 'Area Manager', escalation_summary_metadata['Area Manager'])
    escalation_summary_with_values_and_deltas.insert(3, 'Site Supervisor ',
                                                     escalation_summary_metadata['Site Supervisor'])
    escalation_summary_with_values_and_deltas.insert(4, 'Customer',
                                                     escalation_summary_metadata['Customer_Name'])

    # prepare the work order table for report
    details = work_orders[work_orders['is_active'] == 1][
        ['plantName', 'workOrderNumber', 'status', 'description', 'assetDescription', 'PIC', 'workTypeCategory',
         'workType', 'wopriority', 'capacityOffline', 'lost_MWh', 'createdDate', 'schedStart', 'target_complete_date',
         'days_in_current_status', 'escalation_name', 'PR_numbers', 'PR_statuses', 'purchase_order_numbers',
         'warranty_numbers', 'Region Manager',
         'Area Manager', 'Site Supervisor', 'Customer_Name']].rename(columns={'capacityOffline': 'offline_kW'})
    details['createdDate'] = details.apply(lambda x: x['createdDate'][:10], axis=1)
    details['schedStart'] = details.apply(lambda x: x['schedStart'][:10] if x['schedStart'] is not None else None,
                                          axis=1)
    details['target_complete_date'] = details.apply(lambda x: x['target_complete_date'].strftime('%Y-%m-%d'), axis=1)

    ## create excel file
    # setup directory to receive results
    directory = DATA_DIRECTORY / 'escalation_report' / f'escalation_report {start_process.strftime("%Y-%m-%d")}'

    # make directory if it does not exist
    if not os.path.exists(directory):
        os.makedirs(directory)

    title = f'UPP Escalation Report for {email.replace("@novasourcepower.com", "").replace(".", "_")} on {start_process.strftime("%Y-%m-%d")} {position}'
    sub_title = f'Prepared for {email.replace("@novasourcepower.com", "").replace(".", "_")} on {start_process.strftime("%Y-%m-%d")}'
    xlsx_name = title + '.xlsx'
    report_description = 'The tables below show escalated work orders only'
    symbol_description = 'Note: The "_delta" columns show a comparison of the value compared to 30 days ago'

    logger.info(f'Generating {title}')

    xlsx_filepath = directory / xlsx_name
    writer = pd.ExcelWriter(xlsx_filepath, engine='xlsxwriter')
    workbook = writer.book

    pd.DataFrame().to_excel(writer, sheet_name='Data', startrow=0, header=False,
                            index=False)  # create the sheet (blank dataframe
    worksheet = writer.sheets['Data']
    worksheet.set_column(1, 30, 25)
    worksheet.set_column('A:A', 30)
    worksheet.set_row(0, 40)
    worksheet.insert_image('A1',
                           DATA_DIRECTORY / 'novasource_logo.png',
                           {"x_offset": 5, "y_offset": 5})
    cell_format_title = workbook.add_format({'bold': True, 'font_size': 20})
    cell_format_sub_title = workbook.add_format({'bold': True, 'font_size': 12})
    worksheet.write('D1', 'UPP Escalation Report', cell_format_title)
    worksheet.write('D2', sub_title, cell_format_sub_title)
    worksheet.write('A3', report_description, cell_format_sub_title)
    worksheet.write('A4', symbol_description, cell_format_sub_title)

    r = 4  # skipped rows

    try:

        r = add_next_dataframe(escalation_summary_with_values_and_deltas, writer, sheet_name='Data', startrow=r,
                               index=False,
                               summary_rows=2, use_percentage_format=True, delta_icons=True)
        r = add_next_dataframe(details, writer, sheet_name='Data', startrow=r, index=False,
                               summary_rows=0)

        writer.save()

    except Exception as e:
        logger.exception(f'There was an error generating {title}: {e}')

    # Generate email details
    if cc_list is None:
        cc_list = []
    if isinstance(cc_list, str):
        cc_list = [cc_list]

    if environment == Environment.PRODUCTION.value:
        email_list = [email]
        cc_list.append('BusinessAnalytics@novasourcepower.com')
        subject = title
    else:
        email_list = DEVELOPER_EMAIL_LIST
        cc_list = []
        subject = title + '*EMAIL TEST*'
    body = f'Please contact BusinessAnalytics@novasourcepower.com for questions regarding the report'

    try:
        # Send email
        smtp_response = send_via_smtp(email_list, from_str=from_email, cc_list=cc_list,
                                      subject=subject, body_html=body, attachment_filepath_list=[xlsx_filepath])
        logger.info(f'Email sent: {title}')
    except:
        smtp_response = None
        logger.exception(f'There was an error sending {title}')

    return smtp_response


def run_work_order_progress_report(email, position, fleet_future_cms, environment=FPP_ENVIRONMENT.value, cc_list=None):
    """
    Construct and send work order progress report. SMTP response will be a blank dict if there are no errors
    position can be 'OnM_Site_Manager', 'OnM_Area_Manager', 'OnM_Regional_Manager', 'OnM_Account_Manager', 'fleet', or 'north_america'
    """

    start_process = datetime.now().astimezone(timezone.utc)
    logger.info(f'Initiating work order progress report at {start_process}. ({email}: {position})')

    today = start_process.strftime('%Y-%m-%d')
    start_datetime_int = start_process - timedelta(days=180)  # ~6 months ago
    idx = start_datetime_int.weekday() + 1 % 7
    start_datetime = start_datetime_int - timedelta(days=idx)  # start on Sunday
    start = start_datetime.strftime('%Y-%m-%d')
    end_datetime = start_datetime + timedelta(days=7 * 4 * 12)
    end = end_datetime.strftime('%Y-%m-%d')

    # get relevant work orders for report scope
    if position in ['fleet', 'north_america']:
        fleet = Fleet('In Operation', 'OnM_Project_Status')
    else:
        fleet = Fleet(email, position)

    fleet_metadata = fleet.get_summary_dataframe(
        columns=['AssetTitle', 'Customer_Name', 'OnM_Regional_Manager', 'OnM_Area_Manager', 'OnM_Site_Manager',
                 'Site Region', 'Capacity_DC', 'Commission_Date', 'Capacity_AC']).rename(
        columns={'OnM_Regional_Manager': 'Region Manager', 'OnM_Area_Manager': 'Area Manager',
                 'OnM_Site_Manager': 'Site Supervisor', 'Capacity_DC': 'mwdc', 'Commission_Date': 'commission_date'})
    sites = fleet.site_asset_titles
    fleet_metadata.sort_values(by='Capacity_AC', ascending=False, inplace=True)

    if len(sites) == 0:
        logger.exception(f'There are no sites for {email} - {position}')
        return

    cmms = CMMS(superuser=True, environment=FPP_ENVIRONMENT.value)
    work_orders = attempt_function(cmms.get_all_work_orders_by_site_titles, start, today, sites)

    if len(work_orders) == 0:
        logger.exception(f'There are no work orders for progress report for {email} - {position}')
        return

    work_orders = process_work_orders_for_prioritization_reports(work_orders, cmms, fleet_metadata, start,
                                                                 start_process)

    # filter processed work_orders
    if position in ['north_america']:
        work_orders = work_orders[(work_orders['Site Region'] == 'NA') | (work_orders['Site Region'].isna())]

    critical_customer = get_critical_customers()
    work_orders = work_orders[work_orders['Customer_Name'].isin(critical_customer)]

    if len(work_orders[work_orders['is_active'] == 1]) == 0:
        logger.exception(f'There are no active work orders for progress report for {email} - {position}')
        return

    # calculate cm forecast by site
    future_cms_by_site = pd.DataFrame()
    future_cms_by_site['work_orders'] = \
    work_orders[(work_orders['category'] != '3') & (work_orders['target_complete_date'] >= start)].groupby(
        ['plantName', 'category'])['workOrderNumber'].count()
    future_cms_by_site['start_sample_date'] = start
    future_cms_by_site['end_sample_date'] = today
    future_cms_by_site['days'] = (datetime.strptime(today, '%Y-%m-%d') - datetime.strptime(start, '%Y-%m-%d')).days
    future_cms_by_site.reset_index(inplace=True)
    future_cms_by_site = pd.merge(future_cms_by_site, fleet_metadata[['AssetTitle', 'mwdc', 'commission_date']],
                                  how='left', left_on='plantName', right_on='AssetTitle')

    # use fleet future cms for sites that are less than 12 months old
    future_cms_by_site = pd.merge(future_cms_by_site, fleet_future_cms, on='category', how='left',
                                  suffixes=('', '_fleet'))
    future_cms_by_site['work_orders_per_day'] = future_cms_by_site.apply(
        lambda row: row['work_orders'] / row['days'] if (start_process.replace(tzinfo=None) - row[
            'commission_date']).days > 365 else row['work_orders_fleet'] / row['days_fleet'] / row['mwdc_fleet'] * row[
            'mwdc'], axis=1)
    future_cms_by_category = future_cms_by_site.groupby('category')['work_orders_per_day'].sum().reset_index()

    escalations_dict = get_escalations_dict()

    columns = ['level_1', 'level_2', 'level_3', 'level_4', 'level_5', 'category_name']
    escalations = pd.DataFrame.from_dict(escalations_dict, orient='index', columns=columns).reset_index().rename(
        columns={'index': 'category'})

    future_cms_by_category = pd.merge(future_cms_by_category, escalations[['category', 'level_3']], how='left',
                                      on='category')
    future_cms_by_category['forecast_start_date'] = future_cms_by_category.apply(
        lambda row: (start_process + timedelta(days=row['level_3'])).strftime('%Y-%m-%d'), axis=1)

    # calculate cumulative sum data
    def calculate_cumulative_counts_data(work_orders_df, start: str, end: str, start_process: datetime, futures,
                                         category_filter=None):
        if category_filter is not None:
            work_orders_df = work_orders_df[work_orders_df['category'].isin(category_filter)]
            futures = futures[futures['category'].isin(category_filter)]

        cumulative_counts = pd.DataFrame({'date_start': pd.date_range(start, end, freq='W', tz='UTC')})
        cumulative_counts['date_end'] = cumulative_counts['date_start'] + timedelta(days=7)
        cumulative_counts['target_without_future_cms'] = cumulative_counts.apply(
            lambda row: work_orders_df[(work_orders_df['target_complete_date'] < row['date_end'])][
                'workOrderId'].count(), axis=1)
        cumulative_counts['complete'] = cumulative_counts.apply(lambda row: work_orders_df[
            (work_orders_df['completedDate'] < row['date_end'])][
            'workOrderId'].count(), axis=1)
        cumulative_counts['complete'].mask(cumulative_counts['date_start'] > start_process, inplace=True)

        cumulative_counts['future_cms'] = 0
        # add future CMs to cumulative_counts. for each category, the cm forecast starts on today plus level_3 days (forecast_start_date)
        for index, row in futures.iterrows():
            cumulative_counts['future_cms'] += cumulative_counts.apply(lambda x: max(row['work_orders_per_day'] * (
                        x['date_end'] - (
                    datetime.strptime(row['forecast_start_date'], '%Y-%m-%d').astimezone(timezone.utc))).days, 0),
                                                                       axis=1)

        cumulative_counts['target'] = cumulative_counts['target_without_future_cms'] + cumulative_counts['future_cms']

        # calculate a forecast (based on the rate work orders are completed)
        # use approximately the last 120 days for forecast
        forecast_slope = (max(cumulative_counts['complete']) - cumulative_counts['complete'][9]) / (
                start_process - cumulative_counts['date_start'][9]).days
        cumulative_counts['forecast'] = forecast_slope * (
                cumulative_counts['date_start'] - start_process).dt.days + max(cumulative_counts['complete'])
        cumulative_counts['forecast'].mask(cumulative_counts['date_start'] < start_process, inplace=True)
        cumulative_counts['forecast'].mask(
            (cumulative_counts['date_start'] <= start_process) & (cumulative_counts['date_end'] > start_process),
            other=max(cumulative_counts['complete']), inplace=True)
        catch_up_df = cumulative_counts[cumulative_counts['forecast'].gt(cumulative_counts['target'])]
        if catch_up_df.empty:
            # diverging lines or catch up is not within reporting period
            cumulative_counts['catch_up'] = -1
        elif catch_up_df.iloc[0]['complete'] >= catch_up_df.iloc[0]['target_without_future_cms']:
            # no catch up because complete is already greater that or equal to target
            cumulative_counts['catch_up'] = -2
        else:  # len(catch_up_df.index) > 0:
            # calculate catch up if one exists
            catch_up_index = catch_up_df.index[0]
            cumulative_counts.at[catch_up_index, 'catch_up'] = 1

        return cumulative_counts

    major_cm_categories = ['1a', '2a']
    cumulative_counts = calculate_cumulative_counts_data(work_orders, start, end, start_process, future_cms_by_category)
    cumulative_counts_pms = calculate_cumulative_counts_data(work_orders, start, end, start_process,
                                                             future_cms_by_category, category_filter=['3'])
    cumulative_counts_major_cms = calculate_cumulative_counts_data(work_orders, start, end, start_process,
                                                                   future_cms_by_category,
                                                                   category_filter=major_cm_categories)

    # make the progress summary
    progress_summary_int = pd.pivot_table(work_orders[work_orders['is_active'] == 1], values='workOrderId',
                                          index=['plantName'],
                                          columns=['is_past_due'], aggfunc='count',
                                          margins=True).fillna(0).drop(columns=0, errors='ignore').rename(
        columns={1: 'Past Due WOs', 'All': 'Active WOs'})

    progress_summary_int['Past Due %'] = progress_summary_int['Past Due WOs'].div(progress_summary_int['Active WOs'])

    progress_summary_pm = pd.pivot_table(work_orders[(work_orders['is_active'] == 1) & (
            work_orders['workTypeCategory'] == 'UPP Preventative Maintenance')], values='workOrderId',
                                         index=['plantName'],
                                         columns=['is_past_due'], aggfunc='count',
                                         margins=True).fillna(0).drop(columns=0, errors='ignore').rename(
        columns={1: 'Past Due WOs', 'All': 'Active WOs'})
    progress_summary_pm['Past Due PM %'] = progress_summary_pm['Past Due WOs'].div(progress_summary_pm['Active WOs'])

    progress_summary = progress_summary_int[progress_summary_int.index != 'All'].sort_values(by='Past Due WOs',
                                                                                             ascending=False)
    progress_summary = pd.concat(
        [progress_summary, progress_summary_int[progress_summary_int.index == 'All'].fillna(0)]).rename(
        columns={'All': 'total'})
    progress_summary = pd.merge(progress_summary, progress_summary_pm['Past Due PM %'], left_index=True,
                                right_index=True, how='left').reset_index()

    if position not in ['OnM_Account_Manager']:
        # summarize work orders by region/area/plant manager, plant name
        progress_summary = pd.merge(progress_summary,
                                    fleet_metadata[['AssetTitle', 'Region Manager', 'Area Manager', 'Site Supervisor',
                                                    'Customer_Name']],
                                    how='left', left_on=progress_summary['plantName'].str.lower(),
                                    right_on=fleet_metadata['AssetTitle'].str.lower()).fillna(0)
        progress_summary = progress_summary[
            ['Region Manager', 'Area Manager', 'Site Supervisor', 'Customer_Name', 'plantName', 'Active WOs',
             'Past Due WOs',
             'Past Due %', 'Past Due PM %']]
        progress_summary.loc[progress_summary.index[-1], 'Region Manager'] = ''
        progress_summary.loc[progress_summary.index[-1], 'Area Manager'] = ''
        progress_summary.loc[progress_summary.index[-1], 'Site Supervisor'] = ''

        col_types = {'Region Manager': 'str', 'Area Manager': 'str', 'Site Supervisor': 'str'}
        progress_summary = progress_summary.astype(col_types)

    if position == 'OnM_Account_Manager':
        # summarize work orders by customer, plant name
        progress_summary = pd.merge(progress_summary,
                                    fleet_metadata[['AssetTitle', 'Customer_Name']], how='left',
                                    left_on=progress_summary['plantName'].str.lower(),
                                    right_on=fleet_metadata['AssetTitle'].str.lower()).fillna(0)
        progress_summary = progress_summary[
            ['Customer_Name', 'plantName', 'Active WOs', 'Past Due WOs', 'Past Due %', 'Past Due PM %']]

        progress_summary.loc[progress_summary.index[-1], 'Customer_Name'] = ''

        col_types = {'Customer_Name': 'str'}
        progress_summary = progress_summary.astype(col_types)

    ####################################
    # make "recently closed work orders"
    reporting_columns = ['plantName', 'workOrderNumber', 'status', 'description', 'assetDescription', 'PIC',
                         'workTypeCategory', 'workType',
                         'wopriority', 'capacityOffline', 'dateReceived', 'completedDate', 'target_complete_date',
                         'escalation_name', 'PR_numbers', 'PR_statuses', 'purchase_order_numbers', 'warranty_numbers']
    recently_closed_work_orders = work_orders[
        pd.to_datetime(work_orders['completedDate'],
                       format='%Y-%m-%dT%H:%M:%S').dt.date >= start_process.date() - timedelta(days=7)][
        reporting_columns].rename(columns={'capacityOffline': 'offline_kW'})
    recently_closed_work_orders['target_complete_date'] = recently_closed_work_orders[
        'target_complete_date'].dt.strftime('%Y-%m-%d')
    recently_closed_work_orders['dateReceived'] = recently_closed_work_orders['dateReceived'].str[:10]
    recently_closed_work_orders['completedDate'] = recently_closed_work_orders['completedDate'].dt.strftime('%Y-%m-%d')

    # make "active work orders"
    active_work_orders = work_orders[work_orders['is_active'] == 1][reporting_columns].rename(
        columns={'capacityOffline': 'offline_kW'})
    active_work_orders['target_complete_date'] = active_work_orders[
        'target_complete_date'].dt.strftime('%Y-%m-%d')
    active_work_orders['dateReceived'] = active_work_orders['dateReceived'].str[:10]
    active_work_orders['completedDate'] = active_work_orders['completedDate'].dt.strftime('%Y-%m-%d')

    ## create excel file
    # setup directory to receive results
    directory = DATA_DIRECTORY / 'work_order_progress_report' / f'work_order_progress_report {start_process.strftime("%Y-%m-%d")}'

    # make directory if it does not exist
    if not os.path.exists(directory):
        os.makedirs(directory)
    if not os.path.exists(directory / 'images'):
        os.makedirs(directory / 'images')

    title = f'Work Order Progress Report for {email.replace("@novasourcepower.com", "").replace(".", "_")} on {start_process.strftime("%Y-%m-%d")} {position}'
    sub_title = f'Prepared for {email.replace("@novasourcepower.com", "").replace(".", "_")} on {start_process.strftime("%Y-%m-%d")}'
    xlsx_name = title + '.xlsx'

    # Generate the work order progress chart
    ns_colors = ["#004D55", "#FF7279", "#82D9F0", "#0099A9", "#DDCECD", "#A9D7D3", "#BEA998",
                 "#7279FF", "#000000"]

    def generate_progress_chart(counts, title, n=0):
        plt.figure(n + 1, figsize=(15, 4))  # matplotlib numbering starts at 1
        plt.plot(counts['date_start'], counts['target'], label='Target', color=ns_colors[8])
        plt.plot(counts['date_start'], counts['complete'], label='Actual', color=ns_colors[n])
        plt.plot(counts['date_start'], counts['forecast'], label='Forecast', color=ns_colors[n], linestyle='dashed')

        # add catch up date
        if counts['catch_up'][0] == -1:
            plt.suptitle('Warning: Actual and Target do not converge!', y=.89, horizontalalignment='center',
                         verticalalignment='top', color='red')
        elif counts['catch_up'][0] == -2:
            # do nothing, no catch up date needed
            x = 1
        else:
            catch_up_date = counts[counts['catch_up'] == 1]['date_start'].reset_index(drop=True)
            plt.axvline(x=catch_up_date, color='#ffc22a', linestyle='-', alpha=0.5, linewidth=20)
            plt.text(catch_up_date, min(counts['complete']), f'Catch Up: {catch_up_date[0].strftime("%Y-%m-%d")}',
                     horizontalalignment='center', verticalalignment='bottom')

        plt.ylabel("Work Orders")
        plt.title(title)
        plt.legend()
        n += 1
        return plt, n

    plt.close('all')
    m = 0
    plot_1, m = generate_progress_chart(cumulative_counts, "Work Order Progress Chart", m)
    # plot_1.show()
    plot_1.savefig(directory / 'images' / f'{title} WOPC1.png')
    plot_2, m = generate_progress_chart(cumulative_counts_pms, "PM Work Order Progress Chart", m)
    # plot_2.show()
    plot_2.savefig(directory / 'images' / f'{title} WOPC2.png')
    plot_3, m = generate_progress_chart(cumulative_counts_major_cms, "Major CM Work Order Progress Chart", m)
    # plot_3.show()
    plot_3.savefig(directory / 'images' / f'{title} WOPC3.png')

    logger.info(f'Generating {title}')

    xlsx_filepath = directory / xlsx_name
    writer = pd.ExcelWriter(xlsx_filepath, engine='xlsxwriter')
    workbook = writer.book

    pd.DataFrame().to_excel(writer, sheet_name='Data', startrow=0, header=False,
                            index=False)  # create the sheet (blank dataframe
    worksheet = writer.sheets['Data']
    worksheet.set_column(1, 30, 25)
    worksheet.set_column('A:A', 30)
    worksheet.set_row(0, 40)
    worksheet.insert_image('A1',
                           DATA_DIRECTORY / 'novasource_logo.png',
                           {"x_offset": 5, "y_offset": 5})
    cell_format_title = workbook.add_format({'bold': True, 'font_size': 20})
    cell_format_sub_title = workbook.add_format({'bold': True, 'font_size': 12})
    worksheet.write('D1', 'Work Order Progress Report', cell_format_title)
    worksheet.write('D2', sub_title, cell_format_sub_title)

    r = 4  # skipped rows

    worksheet.insert_image(r, 0, directory / 'images' / f'{title} WOPC1.png')
    r += 21
    worksheet.insert_image(r, 0, directory / 'images' / f'{title} WOPC2.png')
    r += 21
    worksheet.insert_image(r, 0, directory / 'images' / f'{title} WOPC3.png')
    r += 21
    r = add_next_dataframe(progress_summary, writer, sheet_name='Data', startrow=r, index=False,
                           summary_rows=1, use_number_format=True, delta_icons=False, progress_summary_format=True)
    r = add_next_dataframe(recently_closed_work_orders.fillna(0), writer, sheet_name='Data', startrow=r, index=False,
                           summary_rows=0, use_number_format=True, delta_icons=False, progress_summary_format=False,
                           title='Recently Closed Work Orders')
    r = add_next_dataframe(active_work_orders.fillna(0), writer, sheet_name='Data', startrow=r, index=False,
                           summary_rows=0, use_number_format=True, delta_icons=False, progress_summary_format=False,
                           title='Active Work Orders')

    writer.save()

    # Generate email details
    if cc_list is None:
        cc_list = []
    if isinstance(cc_list, str):
        cc_list = [cc_list]

    if environment == Environment.PRODUCTION.value:
        email_list = [email]
        cc_list.append('BusinessAnalytics@novasourcepower.com')
        subject = title
    else:
        email_list = DEVELOPER_EMAIL_LIST
        cc_list = []
        subject = title + '*EMAIL TEST*'
    body = f'Please contact BusinessAnalytics@novasourcepower.com for questions regarding the report'

    try:
        # Send email
        smtp_response = send_via_smtp(email_list, from_str=from_email, cc_list=cc_list,
                                      subject=subject, body_html=body, attachment_filepath_list=[xlsx_filepath])
        logger.info(f'Email sent: {title}')
    except Exception as e:
        smtp_response = None
        logger.exception(f'There was an error sending {title}: {e}')

    return smtp_response


def calculate_fleet_cm_forecast():
    '''
    calculate the rate of CM work orders opened by category for the active fleet
    '''
    start_process = datetime.now().astimezone(timezone.utc)
    logger.info(f'Calculating fleet CM forecast at {start_process}')

    today = start_process.strftime('%Y-%m-%d')
    start_datetime_int = start_process - timedelta(days=180)  # ~6 months ago
    idx = start_datetime_int.weekday() + 1 % 7
    start_datetime = start_datetime_int - timedelta(days=idx)  # start on Sunday
    start = start_datetime.strftime('%Y-%m-%d')

    fleet = Fleet('In Operation', 'OnM_Project_Status')
    sites = fleet.site_asset_titles
    fleet_metadata = fleet.get_summary_dataframe(
        columns=['AssetTitle', 'Customer_Name', 'OnM_Regional_Manager', 'OnM_Area_Manager', 'OnM_Site_Manager',
                 'Site Region', 'Capacity_DC', 'Capacity_AC']).rename(
        columns={'OnM_Regional_Manager': 'Region Manager', 'OnM_Area_Manager': 'Area Manager',
                 'OnM_Site_Manager': 'Site Supervisor', 'Capacity_DC': 'mwdc'})
    fleet_metadata.sort_values(by='Capacity_AC', ascending=False, inplace=True)

    cmms = CMMS(superuser=True, environment=FPP_ENVIRONMENT.value)
    work_orders = attempt_function(cmms.get_all_work_orders_by_site_titles, start, today, sites)

    if len(work_orders) == 0:
        logger.exception(f'There are no work orders for fleet CM forecast')
        return

    work_orders = process_work_orders_for_prioritization_reports(work_orders, cmms, fleet_metadata, start,
                                                                 start_process)

    future_cms = pd.DataFrame()
    future_cms['work_orders'] = \
    work_orders[(work_orders['category'] != '3') & (work_orders['target_complete_date'] >= start)].groupby('category')[
        'workOrderNumber'].count()
    future_cms['start_sample_date'] = start
    future_cms['end_sample_date'] = today
    future_cms['days'] = (datetime.strptime(today, '%Y-%m-%d') - datetime.strptime(start, '%Y-%m-%d')).days
    future_cms['mwdc'] = fleet_metadata['mwdc'].sum()

    return future_cms.reset_index()


def run_supply_chain_prioritization_report(email, environment=FPP_ENVIRONMENT.value, cc_list=None):
    """
    Construct and send supply chain prioritization report. SMTP response will be a blank dict if there are no errors
    """
    start_process = datetime.now().astimezone(timezone.utc)
    logger.info(f'Initiating supply chain prioritization report at {start_process}.')

    today = start_process.strftime('%Y-%m-%d')
    start_datetime = start_process - timedelta(days=30)
    start = start_datetime.strftime('%Y-%m-%d')

    fleet = Fleet('In Operation', 'OnM_Project_Status')

    fleet_metadata = fleet.get_summary_dataframe(
        columns=['AssetTitle', 'Customer_Name', 'OnM_Regional_Manager', 'OnM_Area_Manager', 'OnM_Site_Manager',
                 'Site Region', 'Capacity_AC']).rename(
        columns={'OnM_Regional_Manager': 'Region Manager', 'OnM_Area_Manager': 'Area Manager',
                 'OnM_Site_Manager': 'Site Supervisor'})
    sites = fleet.site_asset_titles
    fleet_metadata.sort_values(by='Capacity_AC', ascending=False, inplace=True)

    if len(sites) == 0:
        logger.exception(f'There are no sites')
        return

    cmms = CMMS(superuser=True, environment=FPP_ENVIRONMENT.value)
    work_orders = attempt_function(cmms.get_all_work_orders_by_site_titles, start, today, sites)

    if len(work_orders) == 0:
        logger.exception(f'There are no work orders to prioritize for supply chain prioritization report')
        return

    # get product request, warranty, and purchase order info
    max_rows = 2000
    iterations = math.ceil(len(work_orders) / max_rows)
    get_chunk = flow_from_df(work_orders, max_rows)
    product_requests = pd.DataFrame()
    for i in range(iterations):
        chunk = next(get_chunk)
        chunk_product_requests = attempt_function(cmms.get_product_requests_by_work_order_ids,
                                                  chunk['workOrderId'].tolist())
        product_requests = pd.concat([product_requests, chunk_product_requests])

    product_requests.reset_index(inplace=True)
    product_requests = product_requests[product_requests['status'] != 'Canceled']
    # TODO add Filter on requesttype != warranty labor

    non_supply_chain_product_request_statuses = get_non_supply_chain_product_request_statuses()
    vendor_product_request_statuses = ['Ordered', 'Arrived, Pending Receipt', 'Partially Received', 'Shipped',
                                       'Shipped to Vendor']

    product_requests['supply_chain_PR'] = product_requests.apply(
        lambda x: 1 if x['status'] not in non_supply_chain_product_request_statuses else 0, axis=1)

    work_orders_with_product_requests = pd.merge(product_requests, work_orders, how='left', on='workOrderId',
                                                 suffixes=('_pr', ''))

    work_orders_with_product_requests = process_work_orders_for_prioritization_reports(
        work_orders_with_product_requests, cmms, fleet_metadata, start,
        start_process)

    product_requests_missing_purchase_orders = work_orders_with_product_requests[
        (work_orders_with_product_requests['poNumber'].isna()) & (
                    work_orders_with_product_requests['supply_chain_PR'] == 1) & (
                    work_orders_with_product_requests['is_active'] == 1)]

    missing_purchase_orders_summary = product_requests_missing_purchase_orders.groupby(
        ['plantName', 'Customer_Name', 'Region Manager', 'Area Manager', 'Site Supervisor'], as_index=False).agg(
        Product_Requests=('productRequestNumber', 'count'), Lost_MWh=('approx_lost_mwh', 'sum')).sort_values(
        by='Lost_MWh', ascending=False).reset_index(drop=True)

    missing_purchase_orders_summary.loc['All'] = missing_purchase_orders_summary.sum(numeric_only=True).fillna(value='')
    missing_purchase_orders_summary.fillna(value='', inplace=True)
    missing_purchase_orders_summary.iloc[-1, 0] = 'All'

    product_request_reporting_columns = ['productRequestNumber', 'poNumber', 'recordTypeName', 'status_pr', 'vendor',
                                         'orderDescription',
                                         'workOrderNumber', 'plantName', 'workOrderNumber', 'target_complete_date',
                                         'status', 'description',
                                         'assetDescription', 'PIC',
                                         'workTypeCategory', 'workType', 'wopriority', 'capacityOffline', 'lost_MWh',
                                         'createdDate', 'schedStart',
                                         'days_in_current_status', 'escalation_name',
                                         'PR_numbers', 'PR_statuses',
                                         'warranty_numbers', 'Region Manager', 'Area Manager', 'Site Supervisor',
                                         'Customer_Name']
    product_requests_missing_purchase_orders = product_requests_missing_purchase_orders[
        product_request_reporting_columns].reset_index(drop=True)

    # TODO add flag on '[NSPS] EXPECTED DELIVERY DATE' for vendor table
    purchase_orders_by_vendor = work_orders_with_product_requests[
        (work_orders_with_product_requests['poNumber'].notna()) & (
                    work_orders_with_product_requests['is_active'] == 1) & (
            work_orders_with_product_requests['status_pr'].isin(vendor_product_request_statuses))]
    vendor_summary1 = purchase_orders_by_vendor.groupby(['vendor'], as_index=False).agg(
        Purchase_Orders=('poNumber', 'count'), Lost_MWh=('approx_lost_mwh', 'sum')).sort_values(by='Lost_MWh',
                                                                                                ascending=False)
    # vendor_summary2 = purchase_orders_by_vendor.groupby(['vendor', 'AssetTitle', 'Customer_Name', 'Region Manager', 'Area Manager', 'Site Supervisor'], as_index=False).agg(
    #     urchase_Orders=('poNumber', 'count'), Lost_MWh=('approx_lost_mwh', 'sum')).sort_values(
    #     by='Lost_MWh', ascending=False)

    vendor_summary1.loc['All'] = vendor_summary1.sum(numeric_only=True).fillna(value='')
    vendor_summary1.fillna(value='', inplace=True)
    vendor_summary1.iloc[-1, 0] = 'All'

    # vendor_summary2.loc['All'] = vendor_summary2.sum(numeric_only=True).fillna(value='')
    # vendor_summary2.fillna(value='', inplace=True)
    # vendor_summary2.iloc[-1, 0] = 'All'

    purchase_orders_by_vendor = purchase_orders_by_vendor[product_request_reporting_columns]

    # TODO make table: WO status in "Pending warranty status", grouped by asset manufacturer
    # TODO Highlight [NSPS] EXPECTED DELIVERY DATE when blank or in the past
    # work_orders_pending_warranty = work_orders_with_product_requests[work_orders_with_product_requests['status_wo'] == 'Pending Warranty Provider']

    ## create excel file
    # setup directory to receive results
    directory = DATA_DIRECTORY / 'supply_chain_prioritization_report' / f'supply_chain_prioritization_report {start_process.strftime("%Y-%m-%d")}'

    # make directory if it does not exist
    if not os.path.exists(directory):
        os.makedirs(directory)

    title = f'Supply Chain Prioritization Report on {start_process.strftime("%Y-%m-%d")}'
    sub_title = f'Prepared on {start_process.strftime("%Y-%m-%d")}'
    xlsx_name = title + '.xlsx'
    page_title1 = 'Product Requests Without Purchase Orders'
    page_title2 = 'Purchase Orders by Vendor'

    logger.info(f'Generating {title}')

    xlsx_filepath = directory / xlsx_name
    writer = pd.ExcelWriter(xlsx_filepath, engine='xlsxwriter')
    workbook = writer.book

    pd.DataFrame().to_excel(writer, sheet_name='PRs wo POs', startrow=0, header=False,
                            index=False)  # create the sheet (blank dataframe
    worksheet = writer.sheets['PRs wo POs']
    worksheet.set_column(1, 30, 19)
    worksheet.set_column('A:A', 30)
    worksheet.set_row(0, 40)
    worksheet.insert_image('A1',
                           DATA_DIRECTORY / 'novasource_logo.png',
                           {"x_offset": 5, "y_offset": 5})
    cell_format_title = workbook.add_format({'bold': True, 'font_size': 20})
    cell_format_sub_title = workbook.add_format({'bold': True, 'font_size': 12})
    worksheet.write('D1', 'Supply Chain Prioritization Report', cell_format_title)
    worksheet.write('D2', sub_title, cell_format_sub_title)
    worksheet.write('A4', page_title1, cell_format_sub_title)

    r = 4  # skipped rows

    try:

        r = add_next_dataframe(missing_purchase_orders_summary, writer, sheet_name='PRs wo POs', startrow=r,
                               index=False,
                               summary_rows=1, use_number_format=True, delta_icons=False)

        r = add_next_dataframe(product_requests_missing_purchase_orders, writer, sheet_name='PRs wo POs', startrow=r,
                               index=False,
                               summary_rows=0, use_number_format=False, delta_icons=False)

    except Exception as e:
        logger.exception(f'There was an error generating {title}: {e}')

    # Add the 'POs by vendor' tab
    pd.DataFrame().to_excel(writer, sheet_name='POs by Vendor', startrow=0, header=False,
                            index=False)  # create the sheet (blank dataframe

    worksheet = writer.sheets['POs by Vendor']
    worksheet.set_column(1, 30, 19)
    worksheet.set_column('A:A', 30)
    worksheet.set_row(0, 40)
    worksheet.insert_image('A1',
                           DATA_DIRECTORY / 'novasource_logo.png',
                           {"x_offset": 5, "y_offset": 5})
    cell_format_title = workbook.add_format({'bold': True, 'font_size': 20})
    cell_format_sub_title = workbook.add_format({'bold': True, 'font_size': 12})
    worksheet.write('D1', 'Supply Chain Prioritization Report', cell_format_title)
    worksheet.write('D2', sub_title, cell_format_sub_title)
    worksheet.write('A4', page_title2, cell_format_sub_title)

    r = 4  # skipped rows

    try:
        r = add_next_dataframe(vendor_summary1, writer, sheet_name='POs by Vendor', startrow=r,
                               index=False,
                               summary_rows=1, use_number_format=True, delta_icons=False)
        # r = 4
        # r = add_next_dataframe(vendor_summary2, writer, sheet_name='POs by Vendor', startrow=r, startcol=4,
        #                        index=False,
        #                        summary_rows=1, use_number_format=True, delta_icons=False)
        r = add_next_dataframe(purchase_orders_by_vendor, writer, sheet_name='POs by Vendor',
                               startrow=r, index=False, summary_rows=0, use_number_format=False, delta_icons=False)

    except Exception as e:
        logger.exception(f'There was an error generating {title}: {e}')

    writer.save()

    # Generate email details
    if cc_list is None:
        cc_list = []
    if isinstance(cc_list, str):
        cc_list = [cc_list]

    if environment == Environment.PRODUCTION.value:
        email_list = [email]
        cc_list.append('BusinessAnalytics@novasourcepower.com')
        subject = title
    else:
        email_list = DEVELOPER_EMAIL_LIST
        cc_list = []
        subject = title + '*EMAIL TEST*'
    body = f'Please contact BusinessAnalytics@novasourcepower.com for questions regarding the report'

    smtp_response = send_via_smtp(email_list, from_str=from_email, cc_list=cc_list,
                                  subject=subject, body_html=body, attachment_filepath_list=[xlsx_filepath])
    logger.info(f'Email sent: {title}')

    return smtp_response


if __name__ == '__main__':
    # process subscriptions using multiprocessing.pool
    environment = 'qa' #Environment.PRODUCTION.value
    pools = 20
    active_fleet = fleet = Fleet(
        filter_="('In Operation', 'Signed')",
        filter_column='OnM_Project_Status',
        filter_operator='IN'
    )
    active_fleet_metadata = active_fleet.get_summary_dataframe(
        columns=[
            'AssetTitle', 'Customer_Name', 'OnM_Regional_Manager', 'OnM_Area_Manager', 'OnM_Site_Manager',
            'OnM_Planner_Scheduler', 'OnM_Account_Manager'
        ]
    )

    # run prioritization subscriptions
    data_driven_reports = [
        'OnM_Regional_Manager', 'OnM_Area_Manager', 'OnM_Site_Manager', 'OnM_Planner_Scheduler',
        'OnM_Account_Manager'
    ]

    emails, positions, environments, ccs = [], [], [], []
    for position in data_driven_reports:
        unique_emails = active_fleet_metadata[f'{position}'].str.lower().dropna().unique().tolist()
        emails.extend(unique_emails)
        positions.extend([position] * len(unique_emails))
        environments.extend([environment] * len(unique_emails))
        ccs.extend([''] * len(unique_emails))

    emails.append('akshay.sagar@novasourcepower.com'), positions.append('CEO'), environments.append(
        environment), ccs.append([])
    emails.append('frank.kelly@novasourcepower.com'), positions.append('fleet'), environments.append(
        environment), ccs.append([])
    emails.append('andrew.kerns@novasourcepower.com'), positions.append('north_america'), environments.append(
        environment), ccs.append([])
    tasks = [*zip(emails, positions, environments, ccs)]

    ctx = mp.get_context("spawn")
    with ctx.Pool(pools) as pool:
        try:
            smtp_response = pool.starmap(run_prioritization_report, tasks)
        except:
            logger.exception(f'Prioritization report failed for {tasks}')
            pass

    # run work order progress subscriptions (mondays only)
    if datetime.today().weekday() == datetime.today().weekday():  # 0:
        fleet_cm_forecast = calculate_fleet_cm_forecast()
        data_driven_reports = ['OnM_Regional_Manager', 'OnM_Area_Manager', 'OnM_Site_Manager', 'OnM_Account_Manager']

        emails, positions, fleet_future_cms_list, environments, ccs = [], [], [], [], []
        (emails.append('frank.kelly@novasourcepower.com'), positions.append('fleet'),
         fleet_future_cms_list.append(fleet_cm_forecast), environments.append(environment), ccs.append([]))

        tasks = [*zip(emails, positions, fleet_future_cms_list, environments, ccs)]

        ctx3 = mp.get_context("spawn")
        with ctx3.Pool(pools) as pool:
            try:
                smtp_response = pool.starmap(run_work_order_progress_report, tasks)
            except:
                logger.exception(f'work order progress report failed for {tasks}')
                pass

    #################################

    # parameters for testing

    # email = 'Stephen.Lynch@novasourcepower.com'
    # email = 'fred.visser@novasourcepower.com'
    # position = 'OnM_Regional_Manager'
    #
    # email = 'timothy.frost@novasourcepower.com'
    # email = 'jeremy.anelli@novasourcepower.com'
    # email = 'cristian.ardiles@novasourcepower.com'
    # email = 'Jesus.Ramirez@novasourcepower.com'  # ['Midway Solar 2', 'Sol Orchard']
    # email = 'cristian.ardiles@novasourcepower.com'
    # email = 'brendon.wykes@novasourcepower.com'
    # email = 'ian.hayward@novasourcepower.com'
    # email = 'jason.sutherland@novasourcepower.com'
    # email = 'robert.meyers@novasourcepower.com'
    # email = 'scott.liston@novasourcepower.com'
    # email = 'calum.mcdermott@novasourcepower.com'
    # email = 'samuel.arevalo@novasourcepower.com'
    # position = 'OnM_Site_Manager'

    # email = 'yash.bingi@novasourcepower.com'
    # email = 'felipe.silva@novasourcepower.com'
    # email = 'alberto.gerardo@novasourcepower.com'
    # email = 'kyle.rabe@novasourcepower.com'
    # email = 'donald.sibley@novasourcepower.com'
    # position = 'OnM_Area_Manager'
    # position = 'north_america'

    # email = 'sol.delapena@novasourcepower.com'
    # email = 'joshua.darley@novasourcepower.com'
    # email = 'mandy.hall@novasourcepower.com'
    # position = 'OnM_Planner_Scheduler'

    # email = 'connor.rebhorn@novasourcepower.com'
    # email = 'marcelle.jordaan@novasourcepower.com'
    # position = 'OnM_Account_Manager'

    # email = 'James.Baker@novasourcepower.com'
    # position = 'Supply_Chain'
    # email = 'UPP-Planning_Scheduling@novasourcepower.com'
    # position = 'Account_Management'
    # position = 'OnM_Planner_Scheduler'

    # email = 'fleet@novasourcepower.com'
    # email = 'frank.kelly@novasourcepower.com'
    # position = 'fleet'

    # email = 'brad.burmaster@novasourcepower.com'
    # position = 'CEO'

    # email = 'andrew.kerns@novasourcepower.com'
    # position = 'north_america'

    # smtp_response = run_prioritization_report(email, position, environment='qa')
    # smtp_response = run_escalation_report(email, position, environment='qa')

    # fleet_cm_forecast = calculate_fleet_cm_forecast()
    # smtp_response = run_work_order_progress_report(email, position, fleet_cm_forecast, environment='qa', cc_list=['tony.blekicki@novasourcepower.com'])

    # smtp_response = run_supply_chain_prioritization_report(environment='qa')