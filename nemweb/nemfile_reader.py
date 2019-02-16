""" reading nemfiles and zipped nemfiles into pandas dataframes """

from io import BytesIO
from zipfile import ZipFile
import pandas as pd


class ZipFileStreamer(ZipFile):
    """ZipFile subclass, with method to extract ZipFile as byte stream to memory"""

    def __init__(self, filename):
        """Initialises ZipFile object, and adds member_count attribute"""
        ZipFile.__init__(self, filename)
        self.member_count = len(self.filelist)

    def extract_stream(self, member):
        """Extract a member from the archive as a byte stream or string steam, using
        its full name. 'member' may be a filename or a ZipInfo object. """
        return BytesIO(self.read(member))


def zip_streams(fileobject):
    """
    Generator that yields each memeber of a zipfile as a BytesIO stream.
    Can take a filename or file-like object (BytesIO object) as an argument.
    """
    with ZipFileStreamer(fileobject) as zipfile:
        for filename in zipfile.namelist():
            yield filename, zipfile.extract_stream(filename)


def nemfile_reader(nemfile_object, table=None, dtype=None):
    """
    Returns a dict containing a pandas dataframe each table in a nemfile.
    The fileobject needs to be unzipped csv (nemfile), and can be either a file or an
    an in stream fileobject. This method processes line by line to allow for csv files
    that contain multiple tables.
    """
    table_dict = {}
    tablesource = None
    for line in nemfile_object.readlines():
        rows = line.decode().split(',')
        if rows[0] == "C": #nem csv file, first line
            tablesource = "nem"
            table = "{0}_{1}".format(rows[1], rows[2])
            print("!", end='')
        elif tablesource == "nem": #nem csv file, other lines
            if rows[0] == "I": #  new table
                table = "{0}_{1}".format(rows[1], rows[2])
                table_dict[table] = line
                print("!", end='')
            elif rows[0] == "D": #  append data to each table
                table_dict[table] += line
                print(".", end='')
        elif rows[0] == "Trading Date" or rows[0] == "Participant Code": #wa csv file, first line
            tablesource = "wa"
            table = table
            table_dict[table] = line
            print("!", end='')
        elif tablesource == "wa": #wa csv subsequent lines
            table_dict[table] += line
            print(".", end='')
    return {table: pd.read_csv(BytesIO(table_dict[table]), dtype=dtype)
            for table in table_dict}


def nemzip_reader(nemzip_object):
    """
    Returns a dict containing a pandas dataframe each table in a zipped nemfile.
    The fileobject is needs to be a zipped csv (nemzip), and can be either a file or an
    in stream fileobject.
    Function checks there is only one file to unzip, unzips to a nemfile (csv) in memory,
    and passes nemfile_object to nemfile reader.
    """
    with ZipFileStreamer(nemzip_object) as zipfile:
        if zipfile.member_count == 1:
            filename = zipfile.namelist()[0]
            nemfile_object = zipfile.extract_stream(filename)
            return nemfile_reader(nemfile_object)
        else:
            raise Exception('More than one file in zipfile')

def nemxls_reader(nemfile_object, table=None, sheet_name="Test"):
    """
    Returns a dict containing a pandas dataframe for a sheet within an Excel file.
    If sheet_name is not specified it will just return the first sheet in the file.
    """
    with pd.ExcelFile(nemfile_object) as xls:
        try:
            sheet_index = xls.sheet_names.index(sheet_name)
        except ValueError:
            sheet_index = 0
    return {table: pd.read_excel(xls, sheet_name=sheet_index)}
            