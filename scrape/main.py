#!/usr/bin/python
import json
import urllib
import timeit
import os
import boto
import attr
import ast
import calendar
import decimal
from boto import dynamodb2
from boto.dynamodb2.table import Table
import requests

#using local mongo for temp store dynamo for final store
from pymongo import MongoClient

from utils.util import get_coords

from utils.scraper import BrowserScrape

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

TABLE_NAME = "GooglePopularTimes"
REGION = "us-east-1"

def compute_average(week):
    week = week.replace("'", "\"")
    jobj = json.loads(week)
    dayAvg = 0
    eveAvg = 0
    d = 0
    while(d < 7):
        tobj = json.loads(json.dumps(jobj[d]["data"]))
        d += 1
#        print len(tobj)
        i = 0
        while(i < len(tobj)):
#            print(tobj[i]["popularity"])
            if i < 13:
                dayAvg += tobj[i]["popularity"]
            if i > 13:
                eveAvg += tobj[i]["popularity"]
                if tobj[i] > 90:
                    eveAvg += 20 #weight the night
            i += 1
    return ((dayAvg/9)/7), ((eveAvg/7)/7)

def map_hour_to_range_string(hour):
    range_map={
        0 : "06-07",
        1 : "07-08",
        2 : "08-09",
        3 : "09-10",
        4 : "10-11",
        5 : "11-12",
        6 : "12-13",
        7 : "13-14",
        8 : "14-15",
        9 : "15-16",
        10 : "16-17",
        11 : "17-18",
        12 : "18-19",
        13 : "19-20",
        14 : "20-21",
        15 : "21-22",
        16 : "22-23",
        17 : "23-24",
        }
    return range_map[hour]

def map_days(val):
    day_map = {
        0 : "Sunday",
        1 : "Monday",
        2 : "Tuesday",
        3 : "Wednesday",
        4 : "Thursday",
        5 : "Friday",
        6 : "Saturday"
        }
    return day_map[val]


def build_popular_times_map(week):
    calendar.setfirstweekday(calendar.SUNDAY)
    popular_map = {}
    popular_time = {}
    week = week.replace("'", "\"")
    jobj = json.loads(week)
    d = 0
    while(d < 7):
        tobj = json.loads(json.dumps(jobj[d]["data"]))
        #day = calendar.day_name[d]
        day = map_days(d)
        d += 1
        i = 0
        while(i < len(tobj)):
           # popular_time[str(i)] = tobj[i]["popularity"]
            #print tobj[i]["popularity"]
            if tobj[i]["popularity"] == 0:
                #print "continuing!!!!!!!!!!!!!!"
                i += 1
                continue
            popular_time[map_hour_to_range_string(i)] = tobj[i]["popularity"]
            i += 1
        popular_map[day] = popular_time
   # print popular_map
    return popular_map

def build_wait_times_map(week):
    wait_map = {}
    popular_time = {}
    week = week.replace("'", "\"")
    jobj = json.loads(week)

    d = 0
    while(d < 7):
        tobj = json.loads(json.dumps(jobj[d]["data"]))
        #day = calendar.day_name[d]
        day = map_days(d)
        d += 1
        i = 0
        ret = 0
        while(i < len(tobj)):
            # popular_time[str(i)] = tobj[i]["popularity"]
            #print tobj[i]["popularity"]
            if tobj[i]["popularity"] == 0:
                #print "continuing!!!!!!!!!!!!!!"
                i += 1
                continue
            if tobj[i]["popularity"] < 50:
                ret = 5
            if tobj[i]["popularity"] > 50 and tobj[i]["popularity"] < 80:
                ret = 10
            if tobj[i]["popularity"] > 80:
                ret = 20
            popular_time[map_hour_to_range_string(i)] = ret
            i += 1
        wait_map[day] = popular_time
    print wait_map
    return wait_map
def build_open_time(times):
    open_map = {}
    for a in times:
        #print "close"
        #print(a["close"])
        #print "open"
        #print(a["open"])
    
        open_map.update({map_days(a["open"]["day"]):[{"open" : int(a["open"]["time"]),"close" : int(a["close"]["time"])}]})

    print "open_map"
    print open_map
    return open_map
    
    
def load_data():
    n_available = 0
    n_unavailable = 0
    locmap = {}
    bacmap = {}
    lacmap = {}
    facmap = {}
    conn = dynamodb2.connect_to_region(
    REGION,
    aws_access_key_id=os.environ['AWS_AC_KEY'],
    aws_secret_access_key=os.environ['AWS_ACS_KEY'],
    )

    table = Table(
    TABLE_NAME,
    connection=conn
)

    for lat, lng in get_coords(params["bounds"][0], params["bounds"][1], params["radius"]):

        radar = radarSearchUrl.format(lat, lng, params["radius"], "|".join(params["types"]), params["API_key"])

        try:
            response = requests.get(radar, auth=('user', 'pass')).text
            results = json.loads(response)["results"]

            if len(results) > 200:
                print("-> more than 200 places in search radius")

            # iterate over places which are not already in database
            for place in (p for p in results
                          if locations.find_one({"place_id": p["place_id"]}) is None):

                # places api - detail search
                detail = json.loads(requests.get(placeRequestUrl.format(place["place_id"], params["API_key"]),
                                                 auth=('user', 'pass')).text)["result"]
                #print detail
                searchterm = "{} {}".format(detail["name"], detail["formatted_address"])
                try:
                    openHour =  detail["opening_hours"]["periods"]
                    hour_map = build_open_time(openHour)
                    print openHour
                except:
                    print "No Opening Hours Listed"

                try:
                    pTimes, weekTot = crawler.get_popular_times(searchterm)
                   
                    locit = detail["geometry"]
                    
                    c = 0
                    for k, v in locit.items():
                        locit[k] = str(v)
                        if c == 0:
                            locmap[k] = v
                            
                            locmap[k]["lat"] = decimal.Decimal(str(locmap[k]["lat"]))
                            locmap[k]["lng"] = decimal.Decimal(str(locmap[k]["lng"]))
                        if c > 0:
                            bacmap[k] = v
                            bacmap[k]["northeast"]["lat"] = decimal.Decimal(str((bacmap[k]["northeast"]["lat"])))
                            bacmap[k]["northeast"]["lng"] = decimal.Decimal(str((bacmap[k]["northeast"]["lng"])))
                            bacmap[k]["southwest"]["lat"] = decimal.Decimal(str((bacmap[k]["southwest"]["lat"])))
                            bacmap[k]["southwest"]["lng"] = decimal.Decimal(str((bacmap[k]["southwest"]["lng"])))
                            print bacmap
                        c += 1

                    tacmap = dict(locmap.items() + bacmap.items())
                    #print locmap
                    waitTime = build_wait_times_map(str(weekTot))

                    d, e = compute_average(str(weekTot))
                    poptime =  build_popular_times_map(str(weekTot))

                    try:
                        table.put_item(data={"placeID": str(detail["place_id"]),
                            "address": str(detail["formatted_address"]),
                            "location" : tacmap,
                            "name": detail["name"],
                            "types": detail["types"],
                            "rating": str(detail["rating"]) if "rating" in detail else -1,
                            "waitTimes": waitTime,
                            "popularTimes": poptime,
                            "openHours": hour_map})

                    except:
                        print("Put Item Failed")
                    print("+ {}".format(searchterm))
                    #print ret
                    #print(crawler.get_popular_times(searchterm))
                    n_available += 1

                except BrowserScrape.NoPopularTimesAvailable:
                    print("no popular times!!!!!!!!!!!!!!!!!!!!")
                    print("- {}".format(searchterm))
                    continue
                    try:
                        table.put_item(data={"placeID": str(detail["place_id"]),
                                          "name": detail["name"],
                                          "address": str(detail["formatted_address"]),
                                          "location": str(detail["geometry"]),
                                          "types": str(detail["types"]),
                                          "rating": "NoPopularTime",
                                          "noonToSix": "Estimate 15<",
                                          "sixToMid": "Estimate 30<",
                                          "popularTimes": "NoPopularTime"})
                    except:
                        print("Duplicate")
                    n_unavailable += 1
                except KeyError:
                    print("key error")
                    pass

        except requests.exceptions.RequestException as e:
            print(e)

    print("executionTime={}; nAvailable={}; nUnavailable={}"
          .format(timeit.default_timer() - start_time, n_available, n_unavailable))


if __name__ == "__main__":
    start_time = timeit.default_timer()

    radarSearchUrl = "https://maps.googleapis.com/maps/api/place/radarsearch/json?location={},{}&radius={}&types={}&key={}"
    placeRequestUrl = "https://maps.googleapis.com/maps/api/place/details/json?placeid={}&key={}"

    params = json.loads(open("params.json", "r").read())
    crawler = BrowserScrape()

    client = MongoClient('localhost', params["dbPort"])
    database = client[params["dbName"]]
    locations = database[params["collectionName"]]

    load_data()
