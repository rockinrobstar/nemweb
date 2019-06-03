"""Module for downloading the Current WA participants list from AEMO"""

from io import BytesIO
from collections import namedtuple
import requests
import os
import pkg_resources
from nemweb import nemfile_reader, nemweb_sqlite, nemweb_filehandler

class ParticipantFileHandler:
    """class for handling AEMO participant list"""

    def __init__(self):
        self.base_url = "/data/"

    def update_data(
            self,
            dataset,
            print_progress=False,
            db_name='nemweb_wa_extra.db'
    ):
        """Main method to process the nemweb participant list
        - opens the file
        - inserts the sheet into the database"""
        localfile = pkg_resources.resource_filename('nemweb', "data/{0}".format(dataset.file))
        with open(localfile, 'rb') as csv_bytes:
            if print_progress:
                print(dataset.dataset_name)
            for table in dataset.tables:
                nemfile = nemfile_reader.nemfile_reader(csv_bytes, table=table)
                dataframe = nemfile[table].drop_duplicates().copy()
                nemweb_sqlite.insert(dataframe, table, db_name)

# class factory for sheets within NEM participants file
WAParticipants = namedtuple("ParticipantsExtra",
                            ["dataset_name",
                             "tables",
                             "file"])

DATASETS = {
    "facilities_extra": WAParticipants(
        dataset_name="facilities_extra",
        tables=['FACILITIES_EXTRA'],
        file="facilities_wa_extra.csv")
}

def update_datasets(datasets, print_progress=False):
    """function that updates a subset of participants (as a list) contained in
    DATASETS"""
    filehandler = ParticipantFileHandler()
    for dataset_name in datasets:
        filehandler.update_data(DATASETS[dataset_name], print_progress=print_progress)
    print("Done")
