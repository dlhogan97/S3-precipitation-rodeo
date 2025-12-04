
# %%
import geopandas as gpd
import pandas as pd
import numpy as np
from metloom.pointdata import SnotelPointData
from metloom.variables import SnotelVariables
from datetime import datetime

# %%
points = {
    "380:CO:SNTL": "Butte",
    "737:CO:SNTL": "Schofield Pass"}

vrs_sntl = [
    SnotelVariables.PRECIPITATION,
]

start_date = datetime(1999,10,1)
end_date = datetime(2025,9,30)

# Loop through the files I downloaded and get a dataframe of all the stations associated with each basin. 
df_list = []
for key in points.keys(): 
    # Get SNOTEL and MESOWEST locations in the basin
    snotel_point = SnotelPointData(key, points[key])
    df_sntl = snotel_point.get_daily_data(start_date, end_date, vrs_sntl)
    print(f"Downloaded {points[key]} data...")
    df_list.append(df_sntl)
df = pd.concat(df_list)
print("Successfully got SNOTEL data!")
df.to_csv('/storage/dlhogan/precipitation-rodeo/data/processed/final/east_river_snotel.csv')



