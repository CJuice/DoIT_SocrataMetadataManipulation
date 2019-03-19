"""
Request metadata for all of our Socrata datasets, store json objects of metadata, and output select fields to excel.

When the domain and url changed from https://data.maryland.gov to https://opendata.maryland.gov we needed to inspect the
description text in the metadata for each dataset on our Socrata open data portal and update the url.

Original Design: Created on Wed Mar 13 09:25:49 2019, @author: william.connelly (he is a developer at Socrata)
Revisions: 20190318, CJuice: Met with Pat, overhauling and revising for MD. Outputs an excel spreadsheet for
Pat to inspect. This is meant to be an inspection script, not a manipulation script like the original version. A
second script exists for manipulation.
"""


def main():

    # IMPORTS
    import configparser
    import numpy as np
    import os
    import pandas as pd
    import re
    import requests

    # VARIABLES
    _root_file_path = os.path.dirname(__file__)
    all_metadata_json_objs = []
    config_file_name = "credentials.cfg"
    df_columns = ["FourByFour", "data_url", "opendata_url", "opendata_str", "Description", "DatasetURL"]
    dicts_for_df_list = []
    fourbyfour_ids_list = []
    md_metadata_url = r"https://opendata.maryland.gov/api/views/metadata/v1"
    new_url_string = r"/opendata.maryland.gov"
    old_url_string = r"/data.maryland.gov"
    open_data_string = "opendata.maryland.gov"
    page = 1
    record_limit = 500
    request_headers = {'Content-Type': 'application/json'}

    # FUNCTIONS
    def get_metadata(page):
        request_params = {"page": page, "limit": record_limit}
        return requests.get(md_metadata_url, params=request_params, headers=request_headers)

    # FUNCTIONALITY
    # Setup config parser and get credentials
    parser = configparser.ConfigParser()
    parser.read(filenames=os.path.join(_root_file_path, config_file_name))
    password = parser.get("DEFAULT", "password")
    username = parser.get("DEFAULT", "username")

    # Page through all metadata by limit amount, and store results as json objects until have metadata for all datasets
    # RESOURCE: https://socratametadataapi.docs.apiary.io/#reference/0/paging-through-metadata-for-all-assets-on-a-domain/example:-paging-through-metadata
    print(f"Limit: {record_limit}")
    metadata_json_objs = get_metadata(page).json()

    while len(metadata_json_objs) > 0:
        print(f"Page: {page}")
        all_metadata_json_objs.extend(metadata_json_objs)
        page += 1
        metadata_json_objs = get_metadata(page).json()
        try:
            print(metadata_json_objs[0])
        except IndexError:
            # No objects
            pass

    # For loop that will cycle through all assets on the platform and replace strings
    for json_obj in all_metadata_json_objs:

        # Need to collect the 4x4 ids from the full json response object for each dataset
        four_by_four = json_obj['id']
        fourbyfour_ids_list.append(four_by_four)

        try:
            # Need to setup and make the requests, then convert to json, and extract the description value for testing
            dataset_url = f"{md_metadata_url}/{json_obj['id']}"
            print(dataset_url)
            dataset_metadata_response = requests.get(dataset_url, auth=(username, password), headers=request_headers)
            dataset_metadata_json = dataset_metadata_response.json()
            description_string = dataset_metadata_json['description']

            # Need to check description text for values of interest
            old_string_results_list = re.findall(pattern=old_url_string, string=description_string, flags=re.IGNORECASE)
            new_string_results_list = re.findall(pattern=new_url_string, string=description_string, flags=re.IGNORECASE)
            opendata_results_list = re.findall(pattern=open_data_string, string=description_string, flags=re.IGNORECASE)

            # Want the number of occurrences of value of interest in description text
            old_string_url_count = len(old_string_results_list)
            new_string_url_count = len(new_string_results_list)
            opendata_url_count = len(opendata_results_list)

            # Need a dictionary of values of interest for use in building a master pandas dataframe for output to excel
            dict_for_df = {"FourByFour": four_by_four,
                           "data_url": old_string_url_count,
                           "opendata_url": new_string_url_count,
                           "opendata_str": opendata_url_count,
                           "Description": description_string,
                           "DatasetURL": dataset_url}
            dicts_for_df_list.append(dict_for_df)

        except Exception as e:
            print(f"Error: {e}")

    master = pd.DataFrame(data=dicts_for_df_list, columns=df_columns)
    with pd.ExcelWriter(path="Metadata_Examination_Socrata.xlsx") as excel_writer:
        master.to_excel(excel_writer=excel_writer, sheet_name="Results", na_rep=np.NaN, index=False)

    print(master.info())


if __name__ == "__main__":
    main()

