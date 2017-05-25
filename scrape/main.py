#!/usr/bin/python
import json
import urllib
import timeit
import os
import boto
import attr
import ast
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

def load_data():
    n_available = 0
    n_unavailable = 0

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

                searchterm = "{} {}".format(detail["name"], detail["formatted_address"])

                try:
                    pTimes, weekTot = crawler.get_popular_times(searchterm)
                   
                    locit = detail["geometry"]
                    for k, v in locit.items():
                        locit[k] = str(v)
                    
                    d, e = compute_average(str(weekTot))
                    if d < 50:
                        day = "15<"
                    else:
                        day = "30<"
                    if e < 50:
                        eve = "15<"
                    else:
                        eve = "30<"
                    try:
                        table.put_item(data={"placeID": str(detail["place_id"]),
                                          "name": detail["name"],
                                          "address": str(detail["formatted_address"]),
                                          #"location": str(detail["geometry"]),
                                           "location" : locit,
                                          "types": str(detail["types"]),
                                          "rating": str(detail["rating"]) if "rating" in detail else -1,
                                          "noonToSix": day,
                                          "sixToMid": eve,
                                   #       "popularTimes": str(json.loads(pTimes))})
                                          "popularTimes": json.loads(pTimes)[2]})

                    except:
                        print("Put Item Failed")
                    print("+ {}".format(searchterm))
                    #print(crawler.get_popular_times(searchterm))
                    n_available += 1

                except BrowserScrape.NoPopularTimesAvailable:
                    print("no popular times!!!!!!!!!!!!!!!!!!!!")
                    print("- {}".format(searchterm))
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
