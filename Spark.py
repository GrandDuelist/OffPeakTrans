 #coding=utf-8
from __future__ import print_function
import os
from hdfs import Config
from hdfs import InsecureClient
import sys
from operator import add
from pyspark import SparkConf
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext
from Transportation import *
import json
import re
class Spark():
    def __init__(self):
        self.sc = None
        self.sorted_user_record_list = None
        self.file_path = None
        self.input_data = None
    def connect(self):
        conf=SparkConf()

        # conf.setMaster("spark://namenode:7077")
        # conf.setAppName("off-peak")
        # conf.set("spark.executor.memory", "128g")
        # conf.set("spark.executor.cores", "16")
        # conf.set("spark.scheduler.mode", "FAIR")
        # sc = SparkContext(conf=conf)
        # sc.addPyFile("Module/Shapely-1.6b4.zip")
        # sc.addFile("exploring/Spark.py")
        # sc.addFile("exploring/Transportation.py")
        # sc.addFile("exploring/Record.py")
        # sc.addFile("exploring/TransRegions.py")
        # sc.addFile("exploring/__init__.py")
        # sc.addFile('exploring/SparkLocalMain.py')
        #
        conf.setMaster("spark://namenode2:7077")
        conf.setAppName("off-peak")
        conf.set("spark.executor.memory", "24g")
        conf.set("spark.executor.cores", "12")
        conf.set("spark.scheduler.mode", "FAIR")
        sc = SparkContext(conf=conf)
        sc.addFile("Spark.py")
        sc.addFile("Transportation.py")
        sc.addFile("Record.py")
        sc.addFile("TransRegions.py")
        sc.addFile("__init__.py")
        sc.addFile('SparkLocalMain.py')
        sc.addFile('../data/shenzhen_tran_simple_gps.json')
        self.sc = sc
    def setLocalInputFile(self,inputFile):
        self.input_data = self.sc.parallelize(inputFile)
    def setInputData(self,input_data):
        self.input_data = input_data
    def setLocalFilePath(self,file_path):
        self.file_path = file_path
        self.setLocalInputFile(open(self.file_path))
    def setHDFSFilePath(self,file_path):
        self.file_path = file_path
        self.setInputData(self.sc.textFile(self.file_path,use_unicode=False).cache())


class SubwaySpark(Spark):
    def __init__(self):
        self.subway = Subway()
        self.sorted_record_list = None
        self.user_trip_pair = None
        self.trip_average_time  = None
        self.in_vehicle_time = None
        self.connect()

    def buildRecordList(self):
        '''
        build the record list for each user and sort it by the time
        :return:
        '''
        one_file = self.input_data
        subway_record_list = one_file.map(self.subway.subwayOneRow).filter(lambda x: x is not None)#.map(lambda record: print(record.station_id))
        user_record_list = subway_record_list.map(lambda x: (x.user_id,x)).groupByKey()
        self.sorted_user_record_list = user_record_list.mapValues(list).filter(lambda (k,v): len(v) > 1).map(lambda (k,v): (k, sorted(v, key=lambda record: record.time)))

    def buildDistrictFilter(self,target_district):
        '''
        filter the record station by district
        :return:
        '''
        station_district_mapping = self.sc.textFile("/zf72/data/station_with_region.txt",use_unicode=False).cache()
        station_district_mapping = station_district_mapping.map(self.subway.stationDistrictMappingOneLine).filter(lambda (k,v): v==target_district)
        station_district_mapping = station_district_mapping.map(lambda (k,v): (v,k)).filter(lambda (k,v): '地铁站' in v)
        station_district_mapping = station_district_mapping.map(lambda (k,v): (v.replace('地铁站',''),k))
        self.station_district_mapping = station_district_mapping.reduceByKey(self.subway.removeDuplicateInMapping)
    def buildFilterRecordListByDistrict(self):
        one_file = self.input_data
        subway_record_list = one_file.map(self.subway.subwayOneRow).filter(lambda x: x is not None)#.map(lambda record: print(record.station_id))
        subway_record_list = subway_record_list.map(lambda record: (record.station_name.rstrip('站'),record))
        records_after_filter = subway_record_list.join(self.station_district_mapping).map(lambda (k,v): v[0])
        user_record_list = records_after_filter.map(lambda x: (x.user_id,x)).groupByKey()
        self.sorted_user_record_list = user_record_list.mapValues(list).filter(lambda (k,v): len(v) > 1).map(lambda (k,v): (k, sorted(v, key=lambda record: record.time)))

    def buildTripList(self):
        self.user_trip_pair = self.sorted_user_record_list.map(self.subway.sortedRecordsToTrip).filter(lambda (k,v): len(v)>0)

    def filterTripListByStartDistrict(self):
        user_trip_pair = self.user_trip_pair.flatMap(self.subway.user_trip_to_trip_user)
        user_trip_pair = user_trip_pair.map(lambda (user_id,one_trip): (one_trip.start.station_name.rstrip('站'),(one_trip,user_id)))
        filter_user_trip_pair = user_trip_pair.join(self.station_district_mapping)
        filter_user_trip_pair = filter_user_trip_pair.map(lambda (k,v): (v[0][1],v[0][0])).groupByKey().mapValues(list)
        self.user_trip_pair = filter_user_trip_pair

    def filterTripListByDestinationDistrict(self):
        user_trip_pair = self.user_trip_pair.flatMap(self.subway.user_trip_to_trip_user)
        user_trip_pair = user_trip_pair.map(lambda (user_id,one_trip): (one_trip.end.station_name.rstrip('站'),(one_trip,user_id)))
        filter_user_trip_pair = user_trip_pair.join(self.station_district_mapping)
        filter_user_trip_pair = filter_user_trip_pair.map(lambda (k,v): (v[0][1],v[0][0])).groupByKey().mapValues(list)
        self.user_trip_pair = filter_user_trip_pair

    def buildInVehicleTime(self):
        trip_time_pair = self.user_trip_pair.flatMap(self.subway.mapToStationTimeMapping)
        self.in_vehicle_time = trip_time_pair.reduceByKey(self.subway.minimumByKey)
        #self.in_vehicle_time.saveAsTextFile('/zf72/transportation_data/result/in_vehicle_time/0601')

    def buildWaitingTime(self,output_file_name):
        #read in_vehicle_time mapping
        user_trips_in_vehicle_time = self.user_trip_pair.flatMap(self.subway.mapToODTrip).join(self.in_vehicle_time)
        timeslot_waiting_time = user_trips_in_vehicle_time.map(self.subway.mapToTripWaitingTime).map(lambda (k,v1,v2): (k,v2))
        self.timeslot_ave_waiting_time = timeslot_waiting_time.combineByKey(
            lambda v: (v,1),
            lambda x,v: (x[0]+v,x[1]+1),
            lambda x,y: (x[0]+y[0],x[1]+y[1])
        ).map(
            lambda (k,v): (k,float(v[0])/float(v[1]))
        )
        self.timeslot_ave_waiting_time.saveAsTextFile("/zf72/transportation_data/result/waiting_time/"+output_file_name)
        # result_json = {}
        # for record in self.timeslot_ave_waiting_time.collect():
        #     result_json[record[0]] = record[1]
        # print(result_json)
        #json.dump(result_json,open('../../data/result/waiting_time.json'))
        # self.user_trip_pair.map(
        #     lambda (k,v):
        # )
        # self.user_trip_pair.join(self.in_vehicle_time)

    def buildTripAveTimeMatrix(self):
        trip_time_pair = self.user_trip_pair.flatMap(self.subway.mapToStationTimeMapping)
        trip_time_count = trip_time_pair.combineByKey(
            lambda v: (v,1),
            lambda x,v: (x[0]+v,x[1]+1),
            lambda x,y: (x[0]+y[0],x[1]+y[1])
        )
        self.trip_average_time = trip_time_count.map(
            lambda (k,v): (k,float(v[0])/float(v[1]))
        )

    def saveAverageTripTime(self,output_file_path,local=False):
        if not local:
            self.trip_average_time.saveAsTextFile(output_file_path)
        else:
            result = self.trip_average_time.collect()
            print(result)


    def buildWaitingTimeInDistricts(self):
        self.subway.buildStationNameDistrictMapping('/media/zf72/Seagate Backup Plus Drive/E/DATA/edges/subway station/station_with_region.txt')
        #trip_pair = self.user_trip_pair.flatMap(lambda (k,v): (k,v))
        local = self.user_trip_pair.collect()
        for (k,v) in local:
            print (k,v[0].start.district)


class BusSpark(Spark):
    def __init__(self):
        self.bus = Bus()
        self.bus_smart_card_data = None
        self.connect()

    def markSmartCardID(self,output_file_path):
        if self.bus_smart_card_data is None:
            self.filterSmartCardBusData()
        self.mask_bus_smart_card_data = self.bus_smart_card_data.map(self.bus.maskSmartCardID)
        self.mask_bus_smart_card_data.saveAsTextFile(output_file_path)

    def filterSmartCardBusData(self):
        self.bus_smart_card_data = self.input_data.filter(self.bus.filterBusSmartCardData)

class TaxiSpark(Spark):
    def __init__(self):
        self.taxi = Taxi()
        self.connect()

    def buildTravelTime(self):
        self.taxi.initPointRegionMapping('../data/shenzhen_tran_simple_gps.json')
        record_group_user = self.input_data.map(self.taxi.parseRecord).filter(lambda record: record.is_occupied)\
            .groupBy(lambda record: record.plate)
        self.record_group_user = record_group_user.mapValues(list).map(lambda (k,v): (k, sorted(v, key=lambda record: record.time)))

    def buildTripList(self):
        taxi_trip = self.record_group_user.map(self.taxi.parseRecordTotrip)
        record_group_user_collect = self.record_group_user.collect()
        for one_record_list in record_group_user_collect:
            print(one_record_list)

    def test(self):

        # mapping = self.sc.textFile("/zf72/data/edges/shenzhen_tran_simple_gps.json").cache()
        # t = mapping.map(json.loads).collect()
        test = json.load(open('../data/shenzhen_tran_simple_gps.json'))
        print(test.keys())
        
        # for ii in t:
        #     print(ii.keys())
