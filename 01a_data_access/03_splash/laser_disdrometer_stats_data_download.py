import glob as glob
import os
import zipfile# %%
# set the filepath
filepath = '/storage/dlhogan/precipitation-rodeo/data/raw/SPLASH/'
if not os.path.exists(filepath+'laser_disdrometer_stats_KP'):
    print('Data not downloaded yet. Would you like to download it?')
    download = input('y/n: ')
    if download == 'y':
        # create laser_disdrometer_stats_KP directory
        os.makedirs(filepath+'laser_disdrometer_stats_KP', exist_ok=True)
        # downlaod the data from https://zenodo.org/records/10372121/files/NOAA_PSL_OttDisdrometerStats_KettlePonds.zip using wget to the filepath
        os.system('wget https://zenodo.org/records/10372121/files/NOAA_PSL_OttDisdrometerStats_KettlePonds.zip -P '+filepath)
        # unzip the file
        with zipfile.ZipFile(filepath+'NOAA_PSL_OttDisdrometerStats_KettlePonds.zip', 'r') as zip_ref:
            zip_ref.extractall(filepath+'laser_disdrometer_stats_KP')
        # remove the zip file
        os.system('rm '+filepath+'NOAA_PSL_OttDisdrometerStats_KettlePonds.zip')
    else:
        print('Download the data from https://zenodo.org/records/10372121/files/NOAA_PSL_OttDisdrometerStats_KettlePonds.zip')
else:
    print('Data already downloaded')
    # we'll start by loading in one file and looking at the data
    filepath = '/storage/dlhogan/precipitation-rodeo/data/raw/SPLASH/laser_disdrometer_stats_KP/*'
    files = glob.glob(filepath)