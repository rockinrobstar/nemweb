"""Initialises nemweb package, loads config"""
import configparser
import os

LOCAL_DIR = os.path.expanduser("~")
CONFIG = configparser.RawConfigParser()
fileName = os.path.join(LOCAL_DIR, '.nemweb_config.ini')
try:
    CONFIG.read(fileName)
except (OSError, configparser.ParsingError):
    print("failed to read existing config file: %s" % fileName)
try:
    CONFIG.get("local_settings","sqlite_dir")
except configparser.NoOptionError:
    print("No sqlite_dir set in config")
try:
    CONFIG.get("local_settings","start_date")
except configparser.NoOptionsError:
    print("No start_date set in conffg")