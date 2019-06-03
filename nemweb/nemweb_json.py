"""Module for production of json files"""

import json
import os
from collections import OrderedDict
from nemweb import nemweb_sqlite, CONFIG

#weekNumber = date.today().isocalendar()[1]
#print 'Week number:', weekNumber

class Station:
    """class to represent a OpenNEM station"""
    def __init__(self, station_id=None, display_name=None, state=None, postcode=None,
                 latitude=None, longitude=None, region_id=None, status_state=None):
        self.station_id = station_id
        self.display_name = display_name
        self.state = state
        self.postcode = postcode
        self.latitude = latitude
        self.longitude = longitude
        self.region_id = region_id
        self.status_state = status_state


class Unit:
    """class to represent an OpenNEM unit. Each station has at least 1 unit"""
    def __init__(self, unit_id=None, fuel_tech=None, sec_fuel=None, registered_capacity=None):
        self.unit_id = unit_id
        self.fuel_tech = fuel_tech
        self.sec_fuel = sec_fuel
        self.registered_capacity = registered_capacity

def create_station_json():
    """funtion to generate the facility_registry.json"""
    wa_facilities = nemweb_sqlite.read_rows(
        "SELECT \"Facility Code\",\"Facility Type\" from \"FACILITIES\"\
                WHERE \"Facility Type\" LIKE '%Generator%'",
        db_name='nemweb_wa.db'
    ) #only look at generators
    wa_facilities_extra = nemweb_sqlite.read_rows(
        "SELECT \"Facility Code\",\"Station Name\",\"Display Name\",\"Postcode\",\
            \"Postcode.1\",\"Latitude\",\"Longitude\",\"Maximum Capacity (MW)\",\
            \"Fuel Tech\",\"2nd Fuel\",\"State\" from \"FACILITIES_EXTRA\" ",
        db_name='nemweb_wa_extra.db'
    )

    stations_array = [Station()] * (len(wa_facilities))
    unit_array = [[]*10 for k in range(len(wa_facilities))]
    done_list = []
    i = j = 0 #outer and inner iterator index
    for generator in wa_facilities:
        if generator[0] in done_list:
            #print("found gen {0} in done_list".format(generator[0]))
            continue
        for extra in wa_facilities_extra:
            #print("{0}:{1},{2}:gen[0]: {3}".format(i, generator[0], j, extra[1]))
            if extra[0] in done_list:
                #print("found ext {0} in done_list".format(extra[0]))
                continue
            if generator[0] == extra[0]: # first unit for station
                stations_array[i] = Station(
                    station_id=extra[1],
                    display_name=extra[2],
                    state=extra[3],
                    postcode=extra[4],
                    latitude=extra[5],
                    longitude=extra[6],
                    region_id="WA1", # hardcode for now
                    status_state=extra[10]
                )

            if stations_array[i].station_id == extra[1]: # one of our sation units
                #print("station_id: {0} == extra[1]: {1}".format(station.station_id,extra[1]))
                unit_array[i].append(Unit(
                    unit_id=extra[0],
                    fuel_tech=extra[8],
                    sec_fuel=extra[9],
                    registered_capacity=extra[7]
                ))
                done_list.append(extra[0])
                #print("added {0} to the done list!".format(extra[0]))
                #print(done_list)
            j += 1
        else: #last iteration
            j = 0
            #print(stations_array)
        i += 1

    #put the Ordered Dict together using the arrays as variable inputs
    station_dict = OrderedDict()
    for station, units in zip(stations_array, unit_array):
        if station.station_id is None:
            #print("no more")
            break
        station_dict[station.station_id] = {
            "station_id": station.station_id,
            "display_name": station.display_name,
            "location":
                {"state": station.state,
                 "postcode": station.postcode,
                 "latitude": "{0:.6f}".format(station.latitude),
                 "longitude": "{0:.6f}".format(station.longitude)
                },
            "duid_data" : {}, # placeholder for pos in Dict
            "region_id" : station.region_id,
            "status" : {
                "state": station.status_state
                }
            }

        for uid in units:
            station_dict[station.station_id]["duid_data"].update(
                {uid.unit_id : {
                    "fuel_tech" : uid.fuel_tech,
                    "2nd_fuel" : uid.sec_fuel,
                    "registered_capacity" : "{0}".format(int(uid.registered_capacity))
                    }
                }
                )


    stations_array_json = json.dumps(station_dict, indent=4)
    print(stations_array_json)
    file_cache = CONFIG['local_settings']['file_cache']
    jsonfile = os.path.join(file_cache, "facility_registry_wa.json")
    print("Saved to {0}".format(jsonfile))
    try:
        with open(jsonfile, 'w') as jfile:
            jfile.write(stations_array_json)
    except OSError:
        print("Failed to write Local file: " + jsonfile)
