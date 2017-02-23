#!/usr/bin/python2.7
#
# Assignment3 Interface
# Name: Thamizh arasan Rajendran
#

from pymongo import MongoClient
import re
import codecs
import math

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    cursr=collection.find({"city" : re.compile(cityToSearch,re.IGNORECASE)})
    outputfptr =codecs.open(saveLocation1,"w",encoding='utf-8')

    for itr in cursr:
        output=itr['name']+"$"+itr['full_address'].replace("\n","")+"$"+itr['city']+"$"+itr['state']+"\r"
        outputfptr.write(output.upper())
    outputfptr.close()
    cursr.close()

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    cursr = collection.find({"categories": {"$in": categoriesToSearch}})
    outputfptr2 =codecs.open(saveLocation2,"w",encoding='utf-8')

    for itr in cursr:
        dist= DistanceFunction(float(itr['latitude']), float(itr['longitude']),float(myLocation[0]),float(myLocation[1]))
        #print dist
        if dist <=float(maxDistance):
            out2=itr['name'].upper()+"\n"
            outputfptr2.write(out2)
    outputfptr2.close()
    cursr.close()


def DistanceFunction(lat2, lon2, lat1, lon1):
    R=3959;
    del1=math.radians(lat1);
    del2=math.radians(lat2);
    dt1=math.radians(lat2-lat1);
    dl1=math.radians(lon2-lon1);
    a = math.sin(dt1 / 2) * math.sin(dt1 / 2) + math.cos(del1) * math.cos(del2) * math.sin(dl1/ 2) * math.sin(dl1/ 2);
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a));
    d = R * c;
    return d
