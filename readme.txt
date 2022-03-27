Task objective is to scrape details from Google Mobility Index URL and generate a CSV file
containing details of the various regions in Google Mobility Index (last checked 9249841 lines)

Each region reports the data for one country (or state if the U.S.) 
The movement data is reported relative to a normal baseline for the following categories:

1) Retail & recreation
2) Grocery & pharmacy
3) Parks
4) Transit stations
5) Workplaces
6) Residential

Procedure:
1) Get link of Google Community Mobility report file
    
       Returns:
           link (str): link of Google Community report file

2) Downloading Google report:
    Download Google Community Mobility report in CSV format

        Args:
            directory: directory to which CSV report will be downloaded

        Returns:
            new_files (bool): flag indicating whether or not new files have been downloaded

3) Build cleaned Google report for worldwide

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

4) Processing google report:
    Writing report from google object to CSV and XLSX formats