"""Module for downloading the Current WA participants list from AEMO"""

from io import BytesIO
from collections import namedtuple
import requests
from nemweb import nemfile_reader, nemweb_sqlite, nemweb_filehandler

class ParticipantFileHandler:
    """class for handling AEMO participant list"""

    def __init__(self):
        self.base_url = "http://data.wa.aemo.com.au"
        self.section = "public/public-data/datafiles"
        self.local_cache_id = "AEMO_WA"

    def update_data(
            self,
            dataset,
            print_progress=False,
            db_name='nemweb_wa.db'
    ):
        """Main method to process the nemweb participant list
        - downloads the file
        - inserts the sheet into the database"""
        #page = requests.get("{0}/{1}/{2}".format(self.base_url,
        #                                         dataset.section,
        #                                         dataset.file))

        #csv_bytes = BytesIO(page.content)
        file=("/{0}/{1}/{2}".format(self.section, dataset.dataset_name, dataset.file))
        csv_bytes = nemweb_filehandler.get_file(self.base_url, self.local_cache_id, file)
        if print_progress:
            print(dataset.dataset_name)
        for table in dataset.tables:
            nemfile = nemfile_reader.nemfile_reader(csv_bytes, table=table)
            dataframe = nemfile[table].drop_duplicates().copy()
            nemweb_sqlite.insert(dataframe, table, db_name)

# class factory for sheets within NEM participants file
WAParticipants = namedtuple("ParticipantSheet",
                            ["dataset_name",
                             "tables",
                             "file"])

DATASETS = {
    "participants": WAParticipants(
        dataset_name="participants",
        tables=['PARTICIPANTS'],
        file="participants.csv"),

    "facilities": WAParticipants(
        dataset_name="facilities",
        tables=['FACILITIES'],
        file="facilities.csv")
}

def update_datasets(datasets, print_progress=False):
    """function that updates a subset of participants (as a list) contained in
    DATASETS"""
    filehandler = ParticipantFileHandler()
    for dataset_name in datasets:
        filehandler.update_data(DATASETS[dataset_name], print_progress=print_progress)
    print("Done")
