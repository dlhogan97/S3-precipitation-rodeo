import glob as glob
import os
import zipfile# %%
# set the filepath
filepath = '/storage/dlhogan/precipitation-rodeo/data/raw/SPLASH/'
if not os.path.exists(filepath+'snow_level_radar_KP'):
    print('Data not downloaded yet. Would you like to download it?')
    download = input('y/n: ')
    if download == 'y':
        # create snow_level_radar_KP directory
        os.makedirs(filepath+'snow_level_radar_KP', exist_ok=True)
        # downlaod the data from https://zenodo.org/records/10552780/files/KpsFmcwRaw.zip using wget to the filepath
        os.system('wget https://zenodo.org/records/10552780/files/KpsFmcwRaw.zip -P '+filepath)
        # unzip the file
        with zipfile.ZipFile(filepath+'KpsFmcwRaw.zip', 'r') as zip_ref:
            zip_ref.extractall(filepath+'snow_level_radar_KP')
        # remove the zip file
        os.system('rm '+filepath+'KpsFmcwRaw.zip')
    else:
        print('Download the data from https://zenodo.org/records/10552780/files/KpsFmcwRaw.zip')
else:
    print('Data already downloaded')
    # we'll start by loading in one file and looking at the data
    filepath = '/storage/dlhogan/precipitation-rodeo/data/raw/SPLASH/snow_level_radar_KP/*'
    files = glob.glob(filepath)