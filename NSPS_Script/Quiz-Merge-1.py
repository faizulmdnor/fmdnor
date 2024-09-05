from fleet_performance import Site, CMMS
import pandas as pd
import numpy as np
cmms = CMMS(superuser=True, environment='production')
site = Site('OSF1')

df_weather_meta = site.weather_station_metadata
assetlist = df_weather_meta['AssetTitle'].to_list()
sf_weatherstations = pd.DataFrame()

for i in range(len(assetlist)):
    try:
        sf_weatherstation = cmms.get_asset_by_title(assetlist[i])
        sf_assetTitle = sf_weatherstation.globalFedAssetTitle
        sf_assetId = sf_weatherstation.assetId
        sf_assetNum = sf_weatherstation.assetNum

        sf_weatherstations.loc[i, 'AssetId'] = sf_assetId
        sf_weatherstations.loc[i, 'AssetNum'] = sf_assetNum
        sf_weatherstations.loc[i, 'GlobalFEDAssetTitle'] = sf_assetTitle
    except Exception as err:
        print(err)

df_merge_weatherStation = pd.merge(df_weather_meta, sf_weatherstations, how='left', left_on='AssetTitle', right_on='GlobalFEDAssetTitle')

