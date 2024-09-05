import pandas as pd
from datetime import datetime
from pathlib import Path

now = datetime.now()
today = now.strftime('%Y%m%d_%H%M')
year_month = now.strftime('%b-%Y')
directory = 'C:/Users/FaizulBinMdNor/Documents/fleet_performance_Sales_Force_1.4.2/Python_Script/new site setup/'
scada_guid = input('Scada Guid: ')
foldername = f'{scada_guid}_{year_month}/'
fullpathname = f'{directory}{foldername}'
Path(fullpathname).mkdir(parents=True, exist_ok=True)

templatefile = f'{directory}InverterTemplate.csv'
datafile = f'{directory}COT5_Data_01.csv'

template = pd.read_csv(templatefile)
data = pd.read_csv(datafile)

def inverter_split_assettitle(df):
    # Split the 'TagName' column by underscores (_) and slashes (/)
    split_values = df['TagName'].str.split('[/_]', expand=True)

    # Rename the columns if there are enough parts
    expected_columns = ['scada_guid', 'PVArray', 'block', 'pcs', 'inv', 'inv_n', 'etc', 'etc2']
    if len(split_values.columns) == len(expected_columns):
        split_values.columns = expected_columns

    # Replace 'I001' with 'Inv001' in the 'inv_n' column
    if 'inv_n' in split_values.columns:
        split_values['inv_n'] = split_values['inv'].str.replace('I', 'Inv')

    # Create the 'assetTitle' column based on the split parts
    df['assetTitle'] = (split_values['scada_guid'] + '_' + split_values['block'] + '_' + split_values['pcs'] + '.' +
                        split_values['inv_n']).copy()

    return df

def WW_TAGNAME_ACOutputkW(data, template):
    try:
        inverter_acoutput = data[data['TagName'].str.contains('I0') & data['TagName'].str.contains('Sts/P_kW')]
        inverter_acoutput.reset_index(drop=True, inplace=True)
        inverter_acoutput_assetTitle = inverter_split_assettitle(inverter_acoutput)

        df_merge = pd.merge(template, inverter_acoutput_assetTitle[['TagName', 'assetTitle']], how='left', left_on='AssetTitle', right_on='assetTitle')
        acoutput_kW = df_merge['TagName']
        acoutput_kW.loc[0, 0] = 'string'

        return acoutput_kW
    except:
        print('ACOutputkW not found.')

def WW_TAGNAME_DCInputkW(data, template):
    inverter_dcInputkW = data[data['TagName'].str.contains('I0') & data['TagName'].str.contains('Sts/PDC_kW')]
    inverter_dcInputkW.reset_index(drop=True, inplace=True)
    inverter_dc_input_assetTitle = inverter_split_assettitle(inverter_dcInputkW)

    df_merge = pd.merge(template, inverter_dc_input_assetTitle[['TagName', 'assetTitle']], how='left',
                        left_on='AssetTitle', right_on='assetTitle')
    dc_input_kW = df_merge['TagName']
    dc_input_kW.loc[0, 0] = 'string'

    return dc_input_kW

def WW_TAGNAME_Matrix_Temperature_A(data, template):
    tempretureA = data[data['TagName'].str.contains('I0') & data['TagName'].str.contains('intTemp')]
    tempretureA.reset_index(drop=True, inplace=True)
    tempretureA_assetTitle = inverter_split_assettitle(tempretureA)

    df_merge = pd.merge(template, tempretureA_assetTitle[['TagName', 'assetTitle']], how='left',
                        left_on='AssetTitle', right_on='assetTitle')
    temp_A = df_merge['TagName']
    temp_A.loc[0, 0] = 'string'

    return temp_A

def WW_TAGNAME_AC_Output_KWh(data, template):
    ac_output_kWh = data[data['TagName'].str.contains('I0') & data['TagName'].str.contains('Pdel_kWh')]
    ac_output_kWh.reset_index(drop=True, inplace=True)
    ac_output_kWh_assetTitle = inverter_split_assettitle(ac_output_kWh)

    df_merge = pd.merge(template, ac_output_kWh_assetTitle[['TagName', 'assetTitle']], how='left',
                        left_on='AssetTitle', right_on='assetTitle')
    AC_Output_Kwh = df_merge['TagName']
    AC_Output_Kwh.loc[0, 0] = 'string'

    return AC_Output_Kwh

def WW_TAGNAME_DC_Input_Amperes(data, template):
    DC_Input_Amperes = data[data['TagName'].str.contains('I0') & data['TagName'].str.contains('Sts/IDC_A')]
    DC_Input_Amperes.reset_index(drop=True, inplace=True)
    DC_Input_Amperes_assetTitle = inverter_split_assettitle(ac_output_kWh)

    df_merge = pd.merge(template, DC_Input_Amperes_assetTitle[['TagName', 'assetTitle']], how='left',
                        left_on='AssetTitle', right_on='assetTitle')
    dc_input_amperes = df_merge['TagName']
    dc_input_amperes.loc[0, 0] = 'string'

    return dc_input_amperes

def WW_TAGNAME_DC_Input_Voltage(data, template):
    DC_Input_Voltage = data[data['TagName'].str.contains('I0') & data['TagName'].str.contains('Sts/VDC_V')]
    DC_Input_Voltage.reset_index(drop=True, inplace=True)
    DC_Input_Voltage_assetTitle = inverter_split_assettitle(ac_output_kWh)

    df_merge = pd.merge(template, DC_Input_Voltage_assetTitle[['TagName', 'assetTitle']], how='left',
                        left_on='AssetTitle', right_on='assetTitle')
    dc_input_voltage = df_merge['TagName']
    dc_input_voltage.loc[0, 0] = 'string'

    return dc_input_voltage

def WW_TAGNAME_ActivePowerSetPoint(data, template):
    DC_Input_Voltage = data[data['TagName'].str.contains('I0') & data['TagName'].str.contains('Sts/PSp_Pct')]
    DC_Input_Voltage.reset_index(drop=True, inplace=True)
    DC_Input_Voltage_assetTitle = inverter_split_assettitle(ac_output_kWh)

    df_merge = pd.merge(template, DC_Input_Voltage_assetTitle[['TagName', 'assetTitle']], how='left',
                        left_on='AssetTitle', right_on='assetTitle')
    dc_input_voltage = df_merge['TagName']
    dc_input_voltage.loc[0, 0] = 'string'

    return dc_input_voltage

def WW_TAGNAME_Phase_A_Voltage(data, template):
    Phase_A_Voltage = data[data['TagName'].str.contains('I0') & data['TagName'].str.contains('Sts/VAB')]
    Phase_A_Voltage.reset_index(drop=True, inplace=True)
    Phase_A_Voltage_assetTitle = inverter_split_assettitle(ac_output_kWh)

    df_merge = pd.merge(template, Phase_A_Voltage_assetTitle[['TagName', 'assetTitle']], how='left',
                        left_on='AssetTitle', right_on='assetTitle')
    phase_a_voltage = df_merge['TagName']
    phase_a_voltage.loc[0, 0] = 'string'

    return phase_a_voltage

def WW_TAGNAME_Phase_B_Voltage(data, template):
    Phase_B_Voltage = data[data['TagName'].str.contains('I0') & data['TagName'].str.contains('Sts/VBC')]
    Phase_B_Voltage.reset_index(drop=True, inplace=True)
    Phase_B_Voltage_assetTitle = inverter_split_assettitle(ac_output_kWh)

    df_merge = pd.merge(template, Phase_B_Voltage_assetTitle[['TagName', 'assetTitle']], how='left',
                        left_on='AssetTitle', right_on='assetTitle')
    phase_b_voltage = df_merge['TagName']
    phase_b_voltage.loc[0, 0] = 'string'

    return phase_b_voltage

def WW_TAGNAME_Phase_C_Voltage(data, template):
    Phase_C_Voltage = data[data['TagName'].str.contains('I0') & data['TagName'].str.contains('Sts/VCA')]
    Phase_C_Voltage.reset_index(drop=True, inplace=True)
    Phase_C_Voltage_assetTitle = inverter_split_assettitle(ac_output_kWh)

    df_merge = pd.merge(template, Phase_C_Voltage_assetTitle[['TagName', 'assetTitle']], how='left',
                        left_on='AssetTitle', right_on='assetTitle')
    phase_c_voltage = df_merge['TagName']
    phase_c_voltage.loc[0, 0] = 'string'

    return phase_c_voltage

def IsLineToNeutralVoltage(data, template):
    IsLineToNeutralVoltage = data[data['TagName'].str.contains('I0') & data['TagName'].str.contains('False')]
    IsLineToNeutralVoltage.reset_index(drop=True, inplace=True)
    IsLineToNeutralVoltage_assetTitle = inverter_split_assettitle(ac_output_kWh)

    df_merge = pd.merge(template, IsLineToNeutralVoltage_assetTitle[['TagName', 'assetTitle']], how='left',
                        left_on='AssetTitle', right_on='assetTitle')
    islinetoneutralvoltage = df_merge['TagName']
    islinetoneutralvoltage.loc[0, 0] = 'string'

    return islinetoneutralvoltage

def WW_TAGNAME_Phase_A_Current(data, template):
    Phase_A_Current = data[data['TagName'].str.contains('I0') & data['TagName'].str.contains('False')]
    Phase_A_Current.reset_index(drop=True, inplace=True)
    Phase_A_Current_assetTitle = inverter_split_assettitle(ac_output_kWh)

    df_merge = pd.merge(template, Phase_A_Current_assetTitle[['TagName', 'assetTitle']], how='left',
                        left_on='AssetTitle', right_on='assetTitle')
    phase_a_current = df_merge['TagName']
    phase_a_current.loc[0, 0] = 'string'

    return phase_a_current

try:
    ACOutputkW = WW_TAGNAME_ACOutputkW(data, template)
    template['WW_TAGNAME_ACOutputkW'] = ACOutputkW

except:
    print('No data')

try:
    DCInputkW = WW_TAGNAME_DCInputkW(data, template)
    template['WW_TAGNAME_DCInputkW'] = DCInputkW

except:
    print('No data')

try:
    Temperature_A = WW_TAGNAME_Matrix_Temperature_A(data, template)
    template['WW_TAGNAME_Matrix_Temperature_A'] = Temperature_A

except:
    print('No data')

try:
    ac_output_kWh = WW_TAGNAME_AC_Output_KWh(data, template)
    template['WW_TAGNAME_AC_Output_KWh'] = ac_output_kWh

except:
    print(f'WW_TAGNAME_AC_Output_KWh - No data')

try:
    ac_output_kWh = WW_TAGNAME_DC_Input_Amperes(data, template)
    template['WW_TAGNAME_DC_Input_Amperes'] = ac_output_kWh

except:
    print(f'WW_TAGNAME_DC_Input_Amperes - No data')

try:
    ac_output_kWh = WW_TAGNAME_DC_Input_Voltage(data, template)
    template['WW_TAGNAME_DC_Input_Voltage'] = ac_output_kWh

except:
    print(f'WW_TAGNAME_DC_Input_Voltage - No data')

try:
    ac_output_kWh = WW_TAGNAME_ActivePowerSetPoint(data, template)
    template['WW_TAGNAME_ActivePowerSetPoint'] = ac_output_kWh

except:
    print(f'WW_TAGNAME_ActivePowerSetPoint - No data')

try:
    ac_output_kWh = WW_TAGNAME_Phase_A_Voltage(data, template)
    template['WW_TAGNAME_Phase_A_Voltage'] = ac_output_kWh

except:
    print(f'WW_TAGNAME_Phase_A_Voltage - No data')

try:
    ac_output_kWh = WW_TAGNAME_Phase_B_Voltage(data, template)
    template['WW_TAGNAME_Phase_B_Voltage'] = ac_output_kWh

except:
    print(f'WW_TAGNAME_Phase_B_Voltage - No data')

try:
    ac_output_kWh = WW_TAGNAME_Phase_C_Voltage(data, template)
    template['WW_TAGNAME_Phase_C_Voltage'] = ac_output_kWh

except:
    print(f'WW_TAGNAME_Phase_C_Voltage - No data')

try:
    ac_output_kWh = IsLineToNeutralVoltage(data, template)
    template['IsLineToNeutralVoltage'] = ac_output_kWh

except:
    print(f'IsLineToNeutralVoltage - No data')

try:
    ac_output_kWh = WW_TAGNAME_Phase_A_Current(data, template)
    template['WW_TAGNAME_Phase_A_Current'] = ac_output_kWh

except:
    print(f'WW_TAGNAME_Phase_A_Current - No data')

template.to_csv(f'{fullpathname}new_Template{today}.csv', index=False)
