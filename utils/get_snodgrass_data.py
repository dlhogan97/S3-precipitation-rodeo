# %%
import pandas as pd
import numpy as np
import datetime as dt
import geopandas as gpd

def get_metadata_and_cols(filename):
    with open(filename) as f:
        site_meta = f.readlines()
    site_meta = [x.strip() for x in site_meta]
    site_meta = [x.split('=') for x in site_meta]
    # save the first entry as the site_name
    site_name = site_meta[0][0]
    # remove the first entry
    site_meta = site_meta[1:]
    # remove exra spaces
    site_meta = [[x[0].strip(), x[1].strip()] for x in site_meta]
    # convert first 3 entries to a dictionary
    site_loc = dict(site_meta[:3])
    # convert the rest to another dictionary
    site_cols = dict(site_meta[3:])
    # rename the keys to just values of 0, 1, 2, ...
    site_cols = dict(zip(range(len(site_cols)), site_cols.values()))
    return site_cols, site_loc, site_name

def get_snodgrass_data(filename, filemeta, meta_dict=None, send_meta=False):
    # read in the data
    site_data = pd.read_csv(filename, header=None)
    # get metadate, columns, and site name
    site_cols, site_loc, site_name = get_metadata_and_cols(filemeta)
    
    # rename the columns
    site_data.columns = site_cols.values()
    # renmae the hour (MST) column to hour
    site_data = site_data.rename(columns={'hour (MST)': 'hour'})
    # convert the year month day hour minute columns to a datetime 
    site_data['datetime'] = pd.to_datetime(site_data[['year', 'month', 'day', 'hour', 'minute']])
    # make timezone aware and set to MST
    site_data['datetime'] = site_data['datetime'].dt.tz_localize('MST')
    # convert to UTC
    site_data['datetime'] = site_data['datetime'].dt.tz_convert('UTC')
    # remove the year month day hour minute columns
    site_data = site_data.drop(['year', 'month', 'day', 'hour', 'minute'], axis=1)
    # remove the units from the column names (if they exist) inside the parentheses
    units= [x.split(' (')[1][:-1] if '(' in x else '' for x in site_data.columns]
    site_data.columns = [x.split(' (')[0] for x in site_data.columns]
    if meta_dict is not None:
        meta_dict[site_name] = site_loc
        # add the units to the meta_dict
        meta_dict[site_name].update(dict(zip(site_data.columns, units)))
        if send_meta:
            return meta_dict
    else:
        return site_data
# turn metadict into geodataframe
def meta_to_gdf(meta_dict):
    # convert the meta_dict to a dataframe
    meta_df = pd.DataFrame(meta_dict).T
    # convert to geodataframe
    meta_gdf = gpd.GeoDataFrame(meta_df, geometry=gpd.points_from_xy(meta_df['lon'], meta_df['lat']))
    # set the crs
    meta_gdf.crs = 'EPSG:4326'
    return meta_gdf

def get_snodgrass_metadata():
    meta_dict = {}
    # filenames for the open and forest sites
    open_site_datafile = '.././01_data/raw_data/station_data/SND_opn_AWS_data_001hr.csv'
    open_site_metafile = '.././01_data/raw_data/station_data/SND_opn_AWS_data_meta.txt'
    forest_site_datafile = '.././01_data/raw_data/station_data/SND_for_AWS_data_001hr.csv'
    forest_site_metafile = '.././01_data/raw_data/station_data/SND_for_AWS_data_meta.txt'
    # get metadate, columns, and site name
    for name,meta in zip([open_site_datafile, forest_site_datafile], [open_site_metafile, forest_site_metafile]):
        # get the meta dict from get_snodgrass_data
        meta_dict.update(get_snodgrass_data(name, meta, meta_dict=meta_dict,send_meta=True))
    # convert the meta_dict to a geodataframe
    meta_gdf = meta_to_gdf(meta_dict)
    return meta_gdf

# %%
if __name__ == '__main__':
    # Read in the opn site data
    open_site_datafile = '.././01_data/raw_data/station_data/SND_opn_AWS_data_001hr.csv'
    open_site_metafile = '.././01_data/raw_data/station_data/SND_opn_AWS_data_meta.txt'
    open_site_data = get_snodgrass_data(open_site_datafile,open_site_metafile)

    forest_site_datafile = '.././01_data/raw_data/station_data/SND_for_AWS_data_001hr.csv'
    forest_site_metafile = '.././01_data/raw_data/station_data/SND_for_AWS_data_meta.txt'
    forest_site_data = get_snodgrass_data(forest_site_datafile,forest_site_metafile)
    metadata_gdf = get_snodgrass_metadata()

    # save the data
    open_site_data.to_csv('.././01_data/processed_data/snodgrass_open_site_data_processed.csv')
    forest_site_data.to_csv('.././01_data/processed_data/snodgrass_forest_site_data_processed.csv')
    metadata_gdf.to_file('.././01_data/processed_data/snodgrass_metadata_processed.geojson', driver='GeoJSON')
# %%
