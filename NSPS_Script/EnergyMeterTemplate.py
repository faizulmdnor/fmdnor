import pandas as pd
directory = 'C:/Users/FaizulBinMdNor/Documents/fleet_performance_Sales_Force_1.4.2/Python_Script/'
templatefile = f'{directory}COT5_Energy MeterTemplate.csv'
datafile = f'{directory}COT5_Data_2024-03-15_13-00-00-000.csv'

template = pd.read_csv(templatefile)
data = pd.read_csv(datafile)

def WW_TAGNAME_kWh_Received(data, template):
    try:
        kWh_received = data[data['TagName'].str.contains('Meters') & data['TagName'].str.contains('PDel_kWh')]
        kWh_received.reset_index(drop=True, inplace=True)
        for i, r in kWh_received.iterrows():
            template.at[i+1, 'WW_TAGNAME_kWh_Received'] = kWh_received.at[i, 'TagName']
        return kWh_received
    except:
        print(f'WW_TAGNAME_kWh_Received not found')

def WW_TAGNAME_kWh_Delivered(data, template):
    try:
        kWh_delivered = data[data['TagName'].str.contains('Meters') & data['TagName'].str.contains('PRec_kWh')]
        kWh_delivered.reset_index(drop=True, inplace=True)
        for i, r in kWh_delivered.iterrows():
            template.at[i+1, 'WW_TAGNAME_kWh_Delivered'] = kWh_delivered.at[i, 'TagName']
        return kWh_delivered
    except:
        print(f'WW_TAGNAME_kWh_Delivered not found')

def WW_TAGNAME_kW_Total(data, template):
    try:
        kWh_total = data[data['TagName'].str.contains('Meters') & data['TagName'].str.contains('P_kW')]
        kWh_total.reset_index(drop=True, inplace=True)
        for i, r in kWh_total.iterrows():
            template.at[i+1, 'WW_TAGNAME_kW_Total'] = kWh_total.at[i, 'TagName']
        return kWh_total
    except:
        print(f'WW_TAGNAME_kW_Total not found')

kWh_received_meter = WW_TAGNAME_kWh_Received(data, template)
kWh_delivered_meter = WW_TAGNAME_kWh_Delivered(data, template)
kW_total_meter = WW_TAGNAME_kW_Total(data, template)

template.to_csv(templatefile, index=False)
