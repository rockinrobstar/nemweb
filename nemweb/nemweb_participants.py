"""Module for downloading the Current participants list from AEMO"""

from io import BytesIO
from collections import namedtuple
import requests
from nemweb import nemfile_reader, nemweb_sqlite

class ParticipantFileHandler:
    """class for handling AEMO participant list"""

    def __init__(self):
        self.base_url = "https://www.aemo.com.au/"
        self.section = "-/media/Files/Electricity/NEM/Participant_Information/"
        self.file = "NEM-Registration-and-Exemption-List.xls"

    def update_data(
            self,
            dataset,
            print_progress=False,
            db_name='nemweb_live.db'
    ):
        """Main method to process the nemweb participant list
        - downloads the file
        - inserts the sheet into the database"""
        page = requests.get("{0}/{1}/{2}".format(self.base_url,
                                                 self.section,
                                                 self.file))

        xls_bytes = BytesIO(page.content)
        if print_progress:
            print(dataset.dataset_name)
        for table in dataset.tables:
            nemxls = nemfile_reader.nemxls_reader(xls_bytes, table, dataset.sheet_name)
            dataframe = nemxls[table].drop_duplicates().copy()
            nemweb_sqlite.insert(dataframe, table, db_name)

# class factory for sheets within NEM participants file
NEMParticipants = namedtuple("ParticipantSheet",
                             ["dataset_name",
                              "tables",
                              "sheet_name"])

DATASETS = {
    "generators_&_loads": NEMParticipants(
        dataset_name="Generators_&_Loads",
        tables=['GENERATORS_&_LOADS'],
        sheet_name="Generators and Scheduled Loads")
}

def update_datasets(datasets, print_progress=False):
    """function that updates a subset of participants (as a list) contained in
    DATASETS"""
    filehandler = ParticipantFileHandler()
    for dataset_name in datasets:
        filehandler.update_data(DATASETS[dataset_name], print_progress=print_progress)
    print("Done updating datasets")
