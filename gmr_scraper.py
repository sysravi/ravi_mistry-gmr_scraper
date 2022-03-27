import os
import datetime
import time

import requests
import urllib.request
from bs4 import BeautifulSoup
import re
import json

import pandas as pd
import zipfile as zp

def get_google_link():
    '''Get link of Google Community Mobility report file
    
       Returns:
           link (str): link of Google Community report file
    '''
    # get webpage source
    url = 'https://www.google.com/covid19/mobility/'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    csv_tag = soup.find('a', {"class": "icon-link"})
    link = csv_tag['href']
    return link

def download_google_report(directory="google_reports"):
    '''Download Google Community Mobility report in CSV format

        Args:
            directory: directory to which CSV report will be downloaded

        Returns:
            new_files (bool): flag indicating whether or not new files have been downloaded
    '''
    new_files = False

    # create directory if it don't exist
    if not os.path.exists(directory) and directory!='':
        os.makedirs(directory)

    # download CSV file
    link = get_google_link()
    file_name = "Global_Mobility_Report.csv"
    path = os.path.join(directory, file_name)
    old_size = os.path.getsize(path) if os.path.isfile(path) else 0
    urllib.request.urlretrieve(link, path)
    new_size = os.path.getsize(path)
    if old_size!=new_size:
        new_files = True

    if not new_files:
        print('Google: No updates')
    else:
        print('Google: Update available')
    
    return new_files

def build_google_report(
        source=os.path.join("google_reports", "Global_Mobility_Report.csv"),
        report_type="regions",
        countries=None,
        world_regions=None,
        country_regions_file = os.path.join("auxiliary_data", "country_worldregions.csv")):
    '''Build cleaned Google report for the worldwide

        Args:
            source: location of the raw Google CSV report
            report_type: available options: 
                            1) "regions" - basic report for the worldwide
                            2) "US" - report for the US
                            3) "regions_detailed" - detailed report for selected countries
                            4) "world_regions_detailed" - detailed report for selected regions (Europe, Asia etc)
            countries: list of countries for "regions_detailed" option. If None - all countries selected
            world_regions: list of regions for "world_regions_detailed option. If None - all regions selected
            country_regions_file: path of the CSV file with matching table of countries and regions

        Returns:
           google (DataFrame): generated Google report
    '''
    # read the raw report
    google = pd.read_csv(source, low_memory=False)
    # shorten value column names
    google.columns = google.columns.str.replace(
        r'_percent_change_from_baseline', '')
    # remove underscores from column names
    google.columns = google.columns.str.replace(r'_', ' ')
    # rename country column
    google = google.rename(columns={'country region': 'country'})
    if report_type == "regions":
        # remove data of subregions of the second level
        google = google[google['sub region 2'].isnull()]
        # remove metropolitan data
        google = google[google['metro area'].isnull()]
        # rename region column
        google = google.rename(columns={'sub region 1': 'region'})
        google = google.loc[:,
                            ['country',
                             'region',
                             'date',
                             'retail and recreation',
                             'grocery and pharmacy',
                             'parks',
                             'transit stations',
                             'workplaces',
                             'residential']]
        google['region'].fillna('Total', inplace=True)
    elif report_type == "US":
        google = google[(google['country'] == "United States")]
        google = google.rename(
            columns={
                'sub region 1': 'state',
                'sub region 2': 'county'})
        google = google.loc[:,
                            ['state',
                             'county',
                             'date',
                             'retail and recreation',
                             'grocery and pharmacy',
                             'parks',
                             'transit stations',
                             'workplaces',
                             'residential']]
        google['state'].fillna('Total', inplace=True)
        google['county'].fillna('Total', inplace=True)
    elif report_type == "regions_detailed" or report_type == "world_regions_detailed":
        if countries is not None and report_type == "regions_detailed":
            google = google[google.country.isin(countries)]
        if report_type == "world_regions_detailed":
            if os.path.isfile(country_regions_file):
                country_regions = pd.read_csv(country_regions_file)
            google = pd.merge(google, country_regions, on='country')
            if world_regions is not None:
                google = google[google.world_region.isin(world_regions)]
        # metro area -> sub region 1    
        google['sub region 1'] = google.apply(lambda x: x['sub region 1'] if isinstance(x['sub region 1'],str)
                                                  else x['metro area'], axis=1)
        column_list = ['world_region'] if report_type == "world_regions_detailed" else []
        column_list = column_list + ['country',
                         'sub region 1',
                         'sub region 2',
                         'date',
                         'retail and recreation',
                         'grocery and pharmacy',
                         'parks',
                         'transit stations',
                         'workplaces',
                         'residential']
        google = google.loc[:, column_list]
        google['sub region 1'].fillna('Total', inplace=True)
        google['sub region 2'].fillna('Total', inplace=True)
    return google

# process Google reports
GOOGLE_ZIP_PATH = os.path.join("google_reports", "Global_Mobility_Report.zip")
GOOGLE_CSV_PATH = os.path.join("google_reports", "Global_Mobility_Report.csv")
# unzip existing report
if os.path.exists(GOOGLE_ZIP_PATH):
    with zp.ZipFile(GOOGLE_ZIP_PATH, 'r') as zf:
        zf.extract('Global_Mobility_Report.csv', "google_reports")
# download new report
new_files_status_google = download_google_report()
if new_files_status_google:
    # build reports
    # build basic report for the worldwide
    google_world = build_google_report()
    # build report for the US
    google_US = build_google_report(report_type="US")
    # build report for Brazil
    google_brazil = build_google_report( report_type="regions_detailed", countries=["Brazil"])
    # build detailed reports for world regions
    google_world_regions = build_google_report(report_type="world_regions_detailed")
    google_europe = google_world_regions[google_world_regions.world_region.isin(['Europe'])]
    google_asia_africa = google_world_regions[google_world_regions.world_region.isin(['Asia', 'Africa'])]
    google_america_oceania = google_world_regions[
        google_world_regions.world_region.isin(['South America', 'North America', 'Oceania'])]
    # write reports to CSV and Excel
    google_world.to_csv(os.path.join("google_reports", "mobility_report_countries.csv"), index=False)
    google_world.to_excel(os.path.join("google_reports", "mobility_report_countries.xlsx"), 
                          index=False, sheet_name='Data', engine = 'xlsxwriter')
    google_US.to_csv(os.path.join("google_reports", "mobility_report_US.csv"), index=False)
    google_US.to_excel(os.path.join("google_reports", "mobility_report_US.xlsx"), 
                          index=False, sheet_name='Data', engine = 'xlsxwriter')
    google_brazil.to_csv(os.path.join("google_reports", "mobility_report_brazil.csv"), index=False)
    google_brazil.to_excel(os.path.join("google_reports", "mobility_report_brazil.xlsx"), 
                          index=False, sheet_name='Data', engine = 'xlsxwriter')
    google_europe.to_csv(os.path.join("google_reports", "mobility_report_europe.csv"), index=False)
    google_europe.to_excel(os.path.join("google_reports", "mobility_report_europe.xlsx"), 
                          index=False, sheet_name='Data', engine = 'xlsxwriter')
    google_asia_africa.to_csv(os.path.join("google_reports", "mobility_report_asia_africa.csv"), index=False)
    google_asia_africa.to_excel(os.path.join("google_reports", "mobility_report_asia_africa.xlsx"), 
                          index=False, sheet_name='Data', engine = 'xlsxwriter')
    google_america_oceania.to_csv(os.path.join("google_reports", "mobility_report_america_oceania.csv"), index=False)
    google_america_oceania.to_excel(os.path.join("google_reports", "mobility_report_america_oceania.xlsx"), 
                          index=False, sheet_name='Data', engine = 'xlsxwriter')
    # zip raw report
    with zp.ZipFile(GOOGLE_ZIP_PATH, 'w', zp.ZIP_DEFLATED) as zf:
        zf.write(GOOGLE_CSV_PATH,"Global_Mobility_Report.csv")
# delete raw CSV report
os.remove(GOOGLE_CSV_PATH)
