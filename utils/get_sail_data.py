import numpy as np
import pandas as pd
import xarray as xr
import datetime as dt
import requests
import pytz
import ftplib
import nctoolkit as nc
import argparse
import json
import os
import sys
from datetime import timedelta

try:
    from urllib.request import urlopen
except ImportError:
    from urllib import urlopen

def date_parser(date_string, output_format='%Y%m%d', return_datetime=False):
    """Converts one datetime string to another or to
    a datetime object.
    Parameters
    ----------
    date_string : str
        datetime string to be parsed. Accepted formats are
        YYYY-MM-DD, DD.MM.YYYY, DD/MM/YYYY or YYYYMMDD.
    output_format : str
        Format for datetime.strftime to output datetime string.
    return_datetime : bool
        If true, returns str as a datetime object.
        Default is False.
    returns
    -------
    datetime_str : str
        A valid datetime string.
    datetime_obj : datetime.datetime
        A datetime object.
    """
    date_fmts = [
        '%Y-%m-%d',
        '%d.%m.%Y',
        '%d/%m/%Y',
        '%Y%m%d',
        '%Y/%m/%d',
        '%Y-%m-%dT%H:%M:%S',
        '%d.%m.%YT%H:%M:%S',
        '%d/%m/%YT%H:%M:%S',
        '%Y%m%dT%%H:%M:%S',
        '%Y/%m/%dT%H:%M:%S',
    ]
    for fmt in date_fmts:
        try:
            datetime_obj = dt.datetime.strptime(date_string, fmt)
            if return_datetime:
                return datetime_obj
            else:
                return datetime_obj.strftime(output_format)
        except ValueError:
            pass
    fmt_strings = ', '.join(date_fmts)
    raise ValueError('Invalid Date format, please use one of these formats ' + fmt_strings)


def get_sail_data(username, token, datastream, startdate, enddate, time=None, resample=None):
    """
    *** This tool was adapted from the ARM Atmospheric Data Community Toolkit ***

    This tool will help users utilize the ARM Live Data Webservice to download
    ARM data.
    Parameters
    ----------
    username : str
        The username to use for logging into the ADC archive.
    token : str
        The access token for accessing the ADC archive.
    datastream : str
        The name of the datastream to acquire.
    startdate : str
        The start date of the data to acquire. Formats accepted are
        YYYY-MM-DD, DD.MM.YYYY, DD/MM/YYYY, YYYYMMDD, YYYY/MM/DD or
        any of the previous formats with THH:MM:SS added onto the end
        (ex. 2020-09-15T12:00:00).
    enddate : str
        The end date of the data to acquire. Formats accepted are
        YYYY-MM-DD, DD.MM.YYYY, DD/MM/YYYY, YYYYMMDD or YYYY/MM/DD, or
        any of the previous formats with THH:MM:SS added onto the end
        (ex. 2020-09-15T13:00:00).
    time: str or None
        The specific time. Format is HHMMSS. Set to None to download all files
        in the given date interval.
    output : str
        The output directory for the data. Set to None to make a folder in the
        current working directory with the same name as *datastream* to place
        the files in.
    Returns
    -------
    files : list
        Returns list of files retrieved
    Notes
    -----
    This programmatic interface allows users to query and automate
    machine-to-machine downloads of ARM data. This tool uses a REST URL and
    specific parameters (saveData, query), user ID and access token, a
    datastream name, a start date, and an end date, and data files matching
    the criteria will be returned to the user and downloaded.
    By using this web service, users can setup cron jobs and automatically
    download data from /data/archive into their workspace. This will also
    eliminate the manual step of following a link in an email to download data.
    All other data files, which are not on the spinning
    disk (on HPSS), will have to go through the regular ordering process.
    More information about this REST API and tools can be found on `ARM Live
    <https://adc.arm.gov/armlive/#scripts>`_.
    To login/register for an access token click `here
    <https://adc.arm.gov/armlive/livedata/home>`_.
    Author: Michael Giansiracusa
    Email: giansiracumt@ornl.gov
    Web Tools Contact: Ranjeet Devarakonda zzr@ornl.gov
    Examples
    --------
    This code will download the netCDF files from the sgpmetE13.b1 datastream
    and place them in a directory named sgpmetE13.b1. The data from 14 Jan to
    20 Jan 2017 will be downloaded. Replace *userName* and *XXXXXXXXXXXXXXXX*
    with your username and token for ARM Data Discovery. See the Notes for
    information on how to obtain a username and token.
    .. code-block:: python
        act.discovery.download_data(
            "userName", "XXXXXXXXXXXXXXXX", "sgpmetE13.b1", "2017-01-14", "2017-01-20"
        )
    """
    # default start and end are empty
    start, end = '', ''
    # start and end strings for query_url are constructed
    # if the arguments were provided
    if startdate:
        start_datetime = date_parser(startdate, return_datetime=True)
        start = start_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        start = f'&start={start}'
    if enddate:
        end_datetime = date_parser(enddate, return_datetime=True)
        # If the start and end date are the same, and a day to the end date
        if start_datetime == end_datetime:
            end_datetime += timedelta(hours=23, minutes=59, seconds=59)
        end = end_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        end = f'&end={end}'
    # build the url to query the web service using the arguments provided
    query_url = (
        'https://adc.arm.gov/armlive/livedata/query?' + 'user={0}&ds={1}{2}{3}&wt=json'
    ).format(':'.join([username, token]), datastream, start, end)
    
    # get url response, read the body of the message,
    # and decode from bytes type to utf-8 string
    response_body = urlopen(query_url).read().decode('utf-8')
    # if the response is an html doc, then there was an error with the user
    if response_body[1:14] == '!DOCTYPE html':
        raise ConnectionRefusedError('Error with user. Check username or token.')

    # parse into json object
    response_body_json = json.loads(response_body)

    # not testing, response is successful and files were returned
    if response_body_json is None:
        print('ARM Data Live Webservice does not appear to be functioning')
        return []

    num_files = len(response_body_json['files'])
    file_names = []
    if response_body_json['status'] == 'success' and num_files > 0:
        for i,fname in enumerate(response_body_json['files']):
            if time is not None:
                if time not in fname:
                    continue
            print(f'[DOWNLOADING] {fname}')
            # construct link to web service saveData function
            save_data_url = (
                'https://adc.arm.gov/armlive/livedata/' + 'saveData?user={0}&file={1}'
            ).format(':'.join([username, token]), fname)
            if i == 0:
                ds = nc.open_data(save_data_url).to_xarray()
                if resample:
                    ds = ds.resample(time=resample).mean()
                    print("Resampling data...")
            else: 
                try:
                    tmp = nc.open_data(save_data_url).to_xarray()
                    if resample:
                        tmp = tmp.resample(time=resample).mean()
                    ds = xr.concat([ds,tmp], dim='time').sortby('time')
                except:
                    print(f'Error downloading {fname}')
                    continue
        return ds
    else:
        print(
            f'No files returned or url status error for {datastream}.\n' 'Check datastream name, start, and end date.'
        )
        return
