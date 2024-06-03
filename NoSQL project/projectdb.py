#! /usr/bin/env python

import datetime, time
import pprint
import os
import sys

from pymongo import MongoClient

portNum = 27017
try:
    portNum = int(os.environ["PORT"])
except KeyError:
    print("Please set the environment variables $PORT")
    sys.exit(1)

client = MongoClient("localhost", portNum)
flights = client["flying"]["flights"]
collection = client["flying"]["flights"]

# Gets the Day of week with the most delays, this is special because of the compound index
#Day of the week with minimum Arrival delay
def getCompoundId(aggregateValue="arrDelay", agg="$avg", n=10):
    return flights.aggregate([
        {"$group" : {"_id":
            {"w" : {"$dayOfWeek" : "$date"},
            "h" : {"$hour" : "$date"}},
            "delay" : {agg : "$"+aggregateValue}}}
        , {"$sort": {"delay" : 1}}
        , {"$limit" : n}
        ])
#Based on the input parameters this function returns Arrival , Departure DElays for  different States
# Arrival, Departure delays for the Day of the Year 
def getmostfrequentattr(aggregateId="origStateId", aggregateValue="depDelay", agg="$avg", n=10):
    return flights.aggregate([
        {"$group" : {"_id": "$"+aggregateId,
                     "delay" : {agg : "$"+aggregateValue}}}
        , { "$sort" : {"delay" : 1} }
        , { "$limit" : n}
    ])
 # finds the most delayed flights (during either departure or arrival or both)
 # sorts first by arrDelay since departure delays are the most common
def mostdelayed(n, withages=False):

    q = {"age" : {"$exists" : True}} if withages else {}
    return flights.find(q,
                        {"_id":1
                         ,"arrDelay":1
                         , "depDelay":1
                         , "carrier":1
                         , "origCity":1
                         , "destCity":1
                         , "age": 1}).sort([("arrDelay", -1)]).sort([("depDelay", -1)]).limit(n)
# Delays in connecting flights due to late arrival of first aircraft 

def findNumCascDelays(tailNum, crsArrTime):
    after = collection.find({"tailNum" : tailNum,
        "crsDepTime" : {"$gt" : crsArrTime}}).sort([("crsDepTime", 1)])
    cnt = collection.count_documents({"tailNum" : tailNum,
        "crsDepTime" : {"$gt" : crsArrTime}})
    if cnt == 0:
        return 0
    else:
        num = 0

        for trip in after:
            if ("depDelay" in trip and "arrDelay" in trip \
                    and "lateAircraftDelay" in trip ):
                if (trip["depDelay"] > 0 and trip["arrDelay"] > 0 \
                    and trip["lateAircraftDelay"] > 0):
                        num += 1
                        continue
            break
    return num

#Returns Airports and Carriers with least Delays
def getmostattr(attr="origAirport", sortBy="depDelay", agg="$avg", n=10):
    return flights.aggregate([
        {"$group" : {"_id": "$"+attr,
                     "delay" : {agg : "$"+sortBy}}}
        , { "$sort" : {"delay" : 1} }
        , { "$limit" : n}
    ])
#Returns States with Highest Flight Traffic
def getStatesByFlights(aggregateId="origStateId", n=10):
    return flights.aggregate([
        {"$group" : {"_id" : "$"+aggregateId,
                     "numFlights" : {"$sum" : 1}}}
        , {"$sort" : {"numFlights" : -1}}
        , {"$limit" : n}
        ])    

if __name__ == "__main__":

    #print("===TIME OF WEEK WITH LEAST ARRIVAL DELAYS================")
    #for doc in getCompoundId():
    #    pprint.pprint(doc)
    #print("===LEAST ARRIVAL DELAYS FOR  DAY OF YEAR=================")
    #for doc in getmostfrequentattr("date", "arrDelay"):
    #    pprint.pprint(doc)

    #d = mostdelayed(5, True)
    #print("======AGE FACTOR IN MOST DELAYED FLIGHTS====")
    #for doc in d:
    #    pprint.pprint(doc)
    
   
   # print("===MOST FLIGHTS LEAVING STATES=================")
   # for doc in getStatesByFlights():
   #      pprint.pprint(doc)
   # print("===MOST INCOMING FLIGHTS STATES================")
   # for doc in getStatesByFlights("destStateId"):
   #     pprint.pprint(doc)

   # print("===MOST DEPARTURE DELAY STATES====================")
   # for doc in getmostfrequentattr():
   #     pprint.pprint(doc)
   # print("===MOST ARRIVAL DELAY STATES======================")
   # for doc in getmostfrequentattr("destStateId", "arrDelay"):
   #     pprint.pprint(doc)
    
   # print("===DEPARTURE DELAYS IN AIRPORTS====================")
   # for doc in getmostattr():
   #     pprint.pprint(doc)
   # print("===ARRIVAL DELAYS IN AIRPORTS=================")
   # for doc in getmostattr("destAirport", "arrDelay"):
   #     pprint.pprint(doc)
   # print("===DEPARTURE DELAYS BASED ON CARRIERS====================")
   # for doc in getmostattr("carrier", "depDelay"):
   #     pprint.pprint(doc)
   # print("===ARRIVAL DELAYS BASED ON CARRIERS====================")
   # for doc in getmostattr("carrier", "arrDelay"):
   #     pprint.pprint(doc)
    print("===DELAYS FROM SUBSEQUENT CONNECTING FLIGHTS====================")    
    first = collection.find({"depDelay" : {"$lte":0}, "arrDelay" : {"$gt" : 0}})
    num = 0
    delays = 0
    for doc in first:
        delays += findNumCascDelays(doc["tailNum"], doc["crsArrTime"])
        num += 1
    
        if num % 10000 == 0:
            print(num, "late arrival flights have caused", delays, "delays")
    print("On average, per late flight causes", float(delays) / float(num), "cascading delays")
    
    