"""
Ingest excel file from examination step, isolate records needing revision, and patch the revisions to Socrata.

The first step involved requesting metadata for all of our datasets, checking the description text for the values
of interest, and outputing an excel file of the results. This script ingests the excel file from step one, isolates
those rows with values that need attention, revises the values, and then updates the description text in the metadata
online in the Socrata open data portal. The patching portion of this script was adopted from the original design
by william.connelly.

Author: CJuice, 20180319
Revisions:
"""


def main():

    # IMPORTS
    import configparser
    import json
    import os
    import pandas as pd
    import re
    import requests

    # VARIABLES
    _root_file_path = os.path.dirname(__file__)
    config_file_name = "credentials.cfg"
    # df_columns = ["FourByFour", "data_url", "opendata_url", "opendata_str", "Description", "DatasetURL"]
    # md_metadata_url = r"https://opendata.maryland.gov/api/views/metadata/v1"
    new_url_string = r"/opendata.maryland.gov"
    old_url_string = r"/data.maryland.gov"
    request_headers = {'Content-Type': 'application/json'}
    input_excel_file = "Socrata_Datasets_Metadata_Analysis.xlsx"

    # FUNCTIONS

    # FUNCTIONALITY
    # Setup config parser and get credentials
    parser = configparser.ConfigParser()
    parser.read(filenames=os.path.join(_root_file_path, config_file_name))
    password = parser.get("DEFAULT", "password")
    username = parser.get("DEFAULT", "username")

    # Need a pandas dataframe from the excel file, and need to revise the description text.
    master_df = pd.read_excel(io=input_excel_file, sheet_name=0, header=0)
    master_df = master_df[(0 < master_df["data_url"])]
    master_df["Description"] = master_df["Description"].apply(func=(lambda x: re.sub(pattern=old_url_string,
                                                                                     repl=new_url_string,
                                                                                     string=x,
                                                                                     count=0,
                                                                                     flags=re.IGNORECASE)))
    # print(master_df["Description"].values) # to visually inspect the revised values

    # Make the patch request to the metadata api endpoint
    row_gen = master_df.iterrows()
    for index, row_series in row_gen:
        row_dict = row_series.to_dict()
        data = {"description": row_dict.get("Description")}
        response = requests.patch(url=row_dict.get("DatasetURL"),
                                  auth=(username, password),
                                  headers=request_headers,
                                  data=json.dumps(data))
        print(row_dict.get("DatasetURL"), response)


if __name__ == "__main__":
    main()

