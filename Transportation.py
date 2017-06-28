#coding=utf-8
import csv
from Record import *
from datetime import datetime
from TransRegions import  *
from Route import *

class Point():
    def __init__(self,x,y,name=None):
        self.x  = x
        self.y = y 
        if name != None:
            self.name = name
class Trip():
    def __init__(self,start=None,end=None,start_time=None,end_time=None,route= None):
        self.start = start
        self.end = end
        self.start_time = start_time
        self.end_time = end_time
        self.all_locations = None #used for intermediate points 
        self.all_times = None #used for intermediate times
        self.in_vehicle_time = None
        self.trip_time = None
        self.waiting_time = None
        self.user_id = None
        self.route = route
    def setLocation(self,start,end):
        self.start = start
        self.end = end
    def setDistrictByStationName(self,subway):
        self.start.setDistrictByStationName(subway)
        self.end.setDistrictByStationName(subway)

    def setTime(self,start_time,end_time):
        self.start_time = start_time
        self.end_time = end_time
    def computeTripDistance(self):
        pass
    def computeTripTime(self):
        # print "start:" + str(self.start_time) + " end: " + str(self.end_time)
        self.trip_time = self.end_time - self.start_time
    def setInvehicleTime(self,in_vehicle_time):
        self.in_vehicle_time = in_vehicle_time
    def computeWaitingTime(self):
        self.waiting_time = self.trip_time - self.in_vehicle_time
    def timeToMin(self,t_hour=None,t_min=None,t_sec=None):
        total_min = 0
        if t_hour is not None:
          total_min = total_min + t_hour * 60
        if t_min is not None:
            total_min = total_min + t_min
        if t_sec is not None:
            total_min = float(total_min) + float(t_sec)/float(60)
        return total_min

    def timeSlot(self,t_hour = None, t_min=None,t_sec = None):
        divide_min = self.timeToMin(t_hour,t_min,t_sec)
        start_min = self.timeToMin(self.start_time.hour,self.start_time.minute,self.start_time.second)
        slot = int(start_min/divide_min)
        self.timeslot = slot
        return slot


    def isCircle(self):
        return self.start.lon == self.end.lon and self.start.lat == self.end.lat

    def arriveTimeSlot(self,t_hour=None,t_min=None, t_sec =None):
        divide_min = self.timeToMin(t_hour,t_min,t_sec)
        end_min = self.timeToMin(self.end_time.hour,self.end_time.minute,self.end_time.second)
        slot = int(end_min/divide_min)
        self.end_timeslot = slot
        return slot

    def waitingTime(self,in_vehicle_time):
        # print in_vehicle_time
        self.waiting_time = self.trip_time - in_vehicle_time
        self.waiting_time_to_min = float(self.waiting_time.total_seconds())/float(60)
        return self.waiting_time_to_min
    def originDestination(self):
        if self.start.station_id is None or self.end.station_id is None:
            return (self.start.trans_region,self.end.trans_region)
        return (self.start.station_id,self.end.station_id)



class Transportation(object):
    def __init__(self):
        self.dir_path = None
        self.file_path = None
        self.region_handler = None
        self.district_handler = None
    def setFileDirectoryPath(self,dir_path):
        self.dir_path = dir_path
    def setFilePath(self,file_path):
        self.file_path = file_path
    def setTransRegionFilePath(self,file_path):
        self.trans_region_file_path = file_path
    def setDistrictFilePaht(self,file_path):
        self.district_file_path = file_path

    def writeToLocal(self,data,file_path):
        file_handler = open(file_path,'wb')

    def minimumByKey(self,a,b):
        if a > b:
            return b
        else:
            return a
    def parseTime(self,timeStr):
        return datetime.strptime(timeStr,'%Y-%m-%d %H:%M:%S')
    def isInTranRegion(self,sc):
        simple_region_file_path = "hdfs://namenode:9000/zf72/data/shenzhen_tran_simple_gps.json"
        region_file = sc.textFile(simple_region_file_path)
        file_content = region_file.collect()
        file_content = json.loads(file_content[0])
        (polygons,polygon_ids) = buildPolygon(file_content)
        file_content['out_edge']
        print(file_content['out_edge'])
    
    def TimeSlotDensity(self,sc):
        dir_path = "/zf72/human_mobility/Tranporation"
        new_name = "tranportation"
        client = Config().get_client('dev')
        file_dates = client.list(dir_path)
        for file_date in file_dates:
            print(file_date)
            files = sc.textFile(dir_path+"/"+file_date+"/*").cache()
            lines = files.map(extract_user_slot_location_telecom).map(lambda (k,v):((v,k),1)).reduceByKey(lambda a,b: a+b)\
                .map(lambda ((k,userid),v):(k,1)).reduceByKey(lambda a,b: a+b).map(lambda (k,v): (tuple(k.split(',')),v))\
                .map(lambda (k,v): ((int(k[0]), float(k[1]), float(k[2])), v))\
                .sortByKey(True,keyfunc=lambda k: int(k[0]))
            lines.saveAsTextFile(dir_path+'_'+new_name+'/'+file_date)

    def initPointRegionMapping(self,file_path):
        self.region_handler = RegionHandler()
        self.region_handler.initializeGridRegion(file_path)

    def initPointDistrictMapping(self,file_path):
        self.district_handler = DistrictHandler()
        self.district_handler.initDistritts(file_path)

    def findPointInTransRegion(self,point):
        lon = point[0]
        lat = point[1]
        target_region = self.region_handler.findPointTransRegion([lon,lat])
        if target_region is not None:
            return target_region.getGeoID()
        else:
            return -1

    def findPointInDistrict(self,point):
        lon = point[0]
        lat = point[1]
        target_district = self.district_handler.findPointInDistrict([lon,lat])
        if target_district is not None:
            return target_district.getGeoID()
        else:
            return "None"

    def filterByStartDistrict(self,user_trip_list):
        if self.start_district is None:
            return True
        (k,(one_trip,user_id)) = user_trip_list
        if one_trip.start.district is not None and one_trip.start.district == self.start_district:
            return True
        else:
            return False

    def filterByEndDistrict(self,user_trip_list):
        if self.end_district is None:
            return True
        (k,(one_trip,user_id)) =  user_trip_list
        if one_trip.end.district is not None and one_trip.end.district == self.end_district:
            return True
        else:
            return False


class Subway(Transportation):
    def __init__(self):
        self.file_path = None
        self.dir_path = None
        self.record_list = None
        self.all_trips = None
        self.time_matrix = None
        self.in_vehicle_time = None
        self.start_station = None
        self.end_station = None
        # self.buildStationNameDistrictMapping('/zf72/data/station_with_region.txt')
    def initSubwayMapping(self,file_path):
        route = SubwayRouteHandler()
        route.buildStationLineMap(file_path)
        route.buildStationLineMap(file_path)

    def subwayOneRow(self,row):
        attrs = row.split(",")
        if attrs[4] == '22':
            in_station = False
        elif attrs[4] == '21':
            in_station = True
        else:
            return None
        recordTime = self.parseTime(attrs[8])
        record = SubwayRecord(user_id=attrs[1],time=recordTime,in_station=in_station,station_id=attrs[13],station_name=attrs[13],route_name=attrs[12],train_id=attrs[14])
        return record
    def generateUniqueSubwayMap(self):
        if self.file_path is None:
            print "ERROR: set file path first"
            return None
        with open(self.file_path) as csvFile:
            for one_row in csvFile:
                record = self.subwayOneRow(one_row)
                if record is not None:
                    print record.in_station
    def inSubwayTime(self):
        if self.file_path is None:
            print "ERROR: set file path first"
            return None
        with open(self.file_path) as csvFile:
            for one_row in csvFile:
                pass

    def buildRecordList(self):
        self.record_list = {}
        if self.file_path is None:
            print "ERROR: set file path first"
            return None
        with open(self.file_path) as csvFile:
            for one_row in csvFile:
                record = self.subwayOneRow(one_row)
                if record is not None:
                    if record.user_id not in self.record_list.keys():
                        self.record_list[record.user_id] = [record]
                    else:
                        self.record_list[record.user_id].append(record)
        return self.record_list

    def buildTripList(self):
        if self.record_list is None:
            print "INFO: build station list first, call buildRecordList"
            return None
        self.all_trips = []
        for one_user in self.record_list.keys():
            start = None
            end = None
            one_user_records = self.record_list[one_user]
            sorted_records = sorted(one_user_records, key=lambda record: record.time)
            for record in sorted_records:
                if record.in_station:
                    start = record
                else:
                    end = record
                    if start is not None:
                        temp_trip = Trip(start=start,end=end)
                        temp_trip.start_time = start.time
                        temp_trip.end_time = end.time
                        temp_trip.computeTripTime()
                        self.all_trips.append(temp_trip)
                        start =None
    def buildTripTimeMatrix(self):
        if self.all_trips is None:
            print "INFO: build trip list first, call buildTripList"
            return None
        self.time_matrix = {}
        for one_trip in self.all_trips:
            start_end = one_trip.start.station_id+","+one_trip.end.station_id
            timeslot = one_trip.timeSlot(t_min=60)
            if start_end not in self.time_matrix.keys():
                self.time_matrix[start_end] = [one_trip] 
            else:
                self.time_matrix[start_end].append(one_trip)
    def inVehicleTimeEstiamte(self):
        if self.time_matrix is None:
            print "INFO: build time matrix first, call buildTripTimeMatrix"
            return None
        self.in_vehicle_time = {}
        for one_key in self.time_matrix.keys():
            min_trip_time =  -1
            trip_list = self.time_matrix[one_key]
            for one_trip in trip_list:
                if min_trip_time == -1:
                    min_trip_time = one_trip.trip_time
                elif min_trip_time > one_trip.trip_time:
                    min_trip_time = one_trip.trip_time
            self.in_vehicle_time[one_key] = min_trip_time
            
    def filterWaitingTimeByStartEndStation(self,one_trip):
        ((start_station,end_station),(trip,in_vechile_time)) = one_trip
        return(self.filterByStartEndStation(trip))

    def waitingTimeOneDay(self):
        self.buildRecordList()
        self.buildTripList()
        self.buildTripTimeMatrix()
        self.inVehicleTimeEstiamte()
        self.one_day_waiting_time = {} 
        self.one_day_average_waiting_time = {}
        for one_key in self.time_matrix.keys():
            trip_list = self.time_matrix[one_key]
            for one_trip in trip_list:
                one_trip.waitingTime(self.in_vehicle_time[one_key])
                if one_trip.timeslot not in self.one_day_waiting_time.keys():
                    self.one_day_waiting_time[one_trip.timeslot] = [one_trip.waiting_time_to_min]
                else:
                    self.one_day_waiting_time[one_trip.timeslot].append(one_trip.waiting_time_to_min)
        for one_key in self.one_day_waiting_time.keys():
            trip_list = self.one_day_waiting_time[one_key]
            self.one_day_average_waiting_time[str(one_key)] = float(sum(trip_list))/float(len(trip_list))
        with open('../data/average_waiting_time.json','w') as fh:
            json.dump(self.one_day_average_waiting_time,fh)

    def userTripToNoUserTripList(self,user_trip_list):
        (user,trip_list) = user_trip_list
        all_trips = []
        for one_trip in trip_list:
            one_trip.user_id = user
            all_trips.append(one_trip)
        return(all_trips)

    def filterByStartEndStation(self,one_trip):
        if self.start_station is None and self.end_station is None:
            return(True)
        elif self.start_station is not None and self.end_station is not None:
            return(self.start_station == one_trip.start.station_name.strip('站') and self.end_station == one_trip.end.station_name.strip('站'))
        elif self.end_station is not None:
            return(self.end_station == one_trip.end.station_name.strip('站'))
        elif self.start_station is not None:
            return(self.start_station == one_trip.start.station_name.strip('站'))



    def buildStationNameDistrictMapping(self,mapping_file_path):
        self.station_districts  = {}
        with open(mapping_file_path) as file_handler:
            content = file_handler.read()
            for one_line in  content:
                attrs = one_line.split(',')
                station_name = attrs[0]
                district_name = attrs[3]
                self.station_districts[station_name] = district_name
        return self.station_districts

    def stationDistrictMappingOneLine(self,one_line):
        attrs = one_line.split(',')
        station_name = attrs[0]
        district_name = attrs[3]
        return (station_name,district_name)

    def mapStationNameToDistrict(self,station_name):
        if self.station_districts is None:
            print "ERROR: %s" % "station mapping is none, call buildStationNameDistrictMapping"
            return None
        return self.station_districts[station_name]

    
#*********************************for spark use
    def sortedRecordsToTrip(self,input):
        start = None
        end = None
        (k,sorted_records) = input
        all_trips = []
        for record in sorted_records:
                if record.in_station:
                    start = record
                else:
                    end = record
                    if start is not None and start is not None:
                        temp_trip = Trip(start=start,end=end)
                        temp_trip.start_time = start.time
                        temp_trip.end_time = end.time
                        temp_trip.computeTripTime()
                        #temp_trip.setDistrictByStationName(self)
                        if start.station_id != end.station_id:
                            all_trips.append(temp_trip)
                        start =None
                        end = None
        return (k,all_trips)


    def mapToStationTimeMapping(self,user_trip_list):
        (user_id,trip_list) = user_trip_list
        od_time = []
        for one_trip in trip_list:
            od_time.append((one_trip.originDestination(),one_trip.trip_time.total_seconds()))
        return od_time
    def mapToODTrip(self,user_trip_list):
        (user_id,trip_list) = user_trip_list
        od_time = []
        for one_trip in trip_list:
            od_time.append((one_trip.originDestination(),one_trip))
        return od_time
    def mapToTripWaitingTime(self,user_trip_in_vehicle_time):
        (od,(one_trip,in_vehicle_time)) = user_trip_in_vehicle_time
        return (one_trip.timeSlot(t_sec=1), one_trip.start.time,one_trip.trip_time.total_seconds() - in_vehicle_time)

    def tripClusterWithRegion(self,id_trip_list):
        (user_id, trip_list) = id_trip_list
        for one_trip in trip_list:
            print one_trip.start

    def user_trip_to_trip_user(self,input):
        (user_id,trip_list) = input
        trip_userids = []
        for one_trip in trip_list:
            trip_userids.append((user_id,one_trip))
        return trip_userids

    def initWalkingTimeStationLine(self,file_path,target_station):
        self.routeHandler = SubwayRouteHandler()
        self.routeHandler.setTargetStation(target_station=target_station)
        self.routeHandler.buildRoutes(file_path=file_path)
        self.routeHandler.buildStationLineMap(file_path=file_path)
        self.routeHandler.buildTargetLatterStations()
        self.routeHandler.buildTargetPreviousStations()
        self.routeHandler.buildTargetStationPairs()

    def mapToLineTarget(self,one_trip):
        start_lines = self.routeHandler.station_line_mapping[one_trip.start.station_name]
        end_lines = self.routeHandler.station_line_mapping[one_trip.end.station_name]
        intersect_lines = list(set(start_lines).intersection(end_lines))
        output = []
        for one_line in intersect_lines:
            output.append(((one_line,self.routeHandler.target_station),one_trip))
        return(output)

    def minimumTripByKey(self,a,b):
        if a.trip_time.total_seconds() > b.trip_time.total_seconds():
            return(b)
        else:
            return(a)
    # def filterByWalkingTimeStart(self,one_trip_record):
    #     pass
#************************************************
class Bus(Transportation):
    def __init__(self):
        pass
    def parseOneRowToRecord(self,one_row):
        attrs = one_row.split(",")
        in_station = False
        if attrs[4] != '31':
            return None
        recordTime = self.parseTime(attrs[8])
        record = BusSmartCardRecord(user_id=attrs[1],time=recordTime,in_station=in_station,station_id=attrs[13],station_name=attrs[13],route_name=attrs[12],train_id=attrs[14])
        return record

    def filterBusSmartCardData(self,one_row):
        one_record = self.parseOneRowToRecord(one_row)
        if one_record is not None:
            return True
        else:
            return False

    def maskFunction(self,current_id_str):
        id_len = len(current_id_str)
        current_id = [v for v in current_id_str]
        temp = current_id[0]
        current_id[0] = current_id[id_len-2]
        current_id[id_len-2] = temp
        temp = current_id[1]
        current_id[1] = current_id[id_len-1]
        current_id[id_len-1] = temp
        return ''.join(current_id)

    def maskID(self,one_row,id_index,sep=","):
        attrs= one_row.split(sep)
        current_id = attrs[id_index]
        masked_id = self.maskFunction(current_id)
        attrs[id_index] = masked_id
        return sep.join(attrs)

    def maskSmartCardID(self,one_row):
        return self.maskID(one_row=one_row,id_index=1)

#*************************************************************
class PrivateVehicle(Transportation):
    def __init__(self):
        pass


#*************************************************************
class Taxi(Transportation):
    def __init__(self):
        pass

    # def initRegionHandler(self,trans_region_file_path=None):
    #     if trans_region_file_path is not trans_region_file_path:
    #         self.initPointRegionMapping(file_path=trans_region_file_path)

    def parseRecord(self,one_row):
        attrs = one_row.split(",")
        taxi_id = attrs[0]
        lon = float(attrs[1])
        lat = None
        try:
            lat = float(attrs[2])
        except:
            return None
        time_str = attrs[3]
        time_str = time_str.strip(".")
        time = self.parseTime(time_str)
        if attrs[len(attrs)-3] == '1':
            is_occupied = True
        else:
            is_occupied = False
        one_record = TaxiRecord(lon=lon,lat=lat,time = time,is_occupied=is_occupied,plate=taxi_id)
        if self.region_handler is not None:
            one_record.computeTargetRegion(self)
        if self.district_handler is not None:
            one_record.computeTargetDistrict(self)
        return one_record

    def parseTime(self,timeStr):
        return datetime.strptime(timeStr,'%Y-%m-%d %H:%M:%S')

    def parseRecordTotrip(self,record_list):
        (plate,record_list) = record_list
        n_records = len(record_list)
        all_trips = []
        route = []
        for ii in range(0,n_records):
            one_record = record_list[ii]
            route.append(one_record)
            if ii == n_records-1 or not record_list[ii+1].is_occupied: #(record_list[ii+1].time - record_list[ii].time).total_seconds() < 30:
                start = route[0]
                end = route[-1]
                one_trip = Trip(start=start,end=end,route=route,start_time=start.time,end_time=end.time)
                one_trip.computeTripTime()
                one_trip.timeSlot(t_min=5)
                one_trip.arriveTimeSlot(t_min=5)
                if (one_trip.start.trans_region != one_trip.end.trans_region and one_trip.trip_time.total_seconds()>30):
                    all_trips.append(one_trip)
                route = []
        return (plate,all_trips)

    def tripListToODTime(self,user_trip_list):
        (user_id,trip_list) = user_trip_list
        od_trip_time = []
        for one_trip in trip_list:
            od_trip_time.append((one_trip.originDestination(), one_trip.trip_time.total_seconds()))
        return od_trip_time
    
    def userTripToTripUser(self,user_trip_list):
        (user_id, trip_list) = user_trip_list
        trip_user_array = []
        for one_trip in trip_list:
            trip_user_array.append((one_trip.originDestination(),(one_trip,user_id)))
        return trip_user_array

    def filterByStartDistrict(self,user_trip_list):
        if self.start_district is None:
            return True
        (k,(one_trip,user_id)) = user_trip_list
        if one_trip.start.district is not None and one_trip.start.district == self.start_district:
            return True
        else:
            return False

class PV(Transportation):

    def parseRecord(self,one_row):
        attrs = one_row.split(",")
        if attrs is None or len(attrs)<4:
            return None
        pv_id = attrs[0]
        lon = float(attrs[1])
        lat = float(attrs[2])
        timeStr = attrs[3]
        time = None
        try:
            time = self.parseTime(timeStr)
        except:
            return None
        one_record = PVRecord(pv_id=pv_id,lon=lon,lat=lat,time=time)
        if self.region_handler is not None:
            one_record.computeTargetRegion(self)
        if self.district_handler is not None:
            one_record.computeTargetDistrict(self)
        return one_record

    def parseTime(self,timeStr):
        return datetime.strptime(timeStr,'%Y-%m-%d %H:%M:%S')

    def splitTripByTimeInterval(self,route):
        all_trip = []
        start_stop = route[0]
        end_start = route[0]
        is_end_start = False
        new_route = []
        n_records = len(route)
        for ii in range(0,n_records):
            one_stop = route[ii]

            if ii == n_records-1:
                end_start = one_stop
                new_route.append(one_stop)
                one_trip = Trip(start=start_stop,end=end_start,start_time=start_stop.time,end_time=end_start.time,route=new_route)
                all_trip.append(one_trip)
            else:
                next_stop = route[ii+1]
                if one_stop.lat == next_stop.lat and one_stop.lon == next_stop.lon:
                    if not is_end_start:
                        is_end_start = True
                        end_start = one_stop

                elif is_end_start:
                    one_trip = Trip(start=start_stop,end=end_start,route=new_route,start_time=start_stop.time,end_time=end_start.time)
                    start_stop = one_stop
                    is_end_start =False
                    all_trip.append(one_trip)
                    new_route = [start_stop]
                elif not is_end_start:
                    new_route.append(one_stop)

        new_all_trip = []
        n_trips =  len(all_trip)
        start_merge = False
        for ii in range(0,n_trips):
            if one_trip.isCircle():
                continue
            one_trip = all_trip[ii]
            if ii != n_trips-1:
                next_trip = all_trip[ii+1]
                if (next_trip.start.time - one_trip.end.time).total_seconds() < 20:  #if two trips interval is less than 1 min, merge two trips
                    if not start_merge:
                        new_trip = self.mergeTwoTrip(one_trip,next_trip)
                        start_merge = True
                    else:
                        new_trip = self.mergeTwoTrip(new_trip,next_trip)
                else:
                    if start_merge:
                        new_all_trip.append(new_trip)
                        start_merge = False
                        new_trip  = None
                    else:
                        new_all_trip.append(one_trip)
            else:
                if start_merge:
                    new_all_trip.append(new_trip)
                    start_merge = False

        return(new_all_trip)

    def mergeTwoTrip(self,first_trip,second_trip):
        start = first_trip.start
        end = second_trip.end
        route = []
        route.extend(first_trip.route)
        route.extend(second_trip.route)
        trip = Trip(start=start,end=end,start_time=start.time,end_time=end.time,route=route)
        return(trip)

    def parseRecordTotrip(self,record_list):
        (pv_id,record_list) = record_list
        n_records = len(record_list)
        all_trips = []
        route = []
        for ii in range(0,n_records):
            one_record = record_list[ii]
            route.append(one_record)
            if ii == n_records-1 or (record_list[ii+1].time - record_list[ii].time).total_seconds() > 30:
                start = route[0]
                end = route[-1]
                one_trip = Trip(start=start,end=end,route=route,start_time=start.time,end_time=end.time)
                one_trip.computeTripTime()
                one_trip.timeSlot(t_min=5)
                one_trip.arriveTimeSlot(t_min=5)
                if (one_trip.start.trans_region != one_trip.end.trans_region and one_trip.trip_time.total_seconds >60):
                # if  one_trip.trip_time.total_seconds() >60 and len(route) > 1:
                    all_trips.append(one_trip)
                route = []
        new_all_trips = []
        for one_trip in all_trips:
            new_all_trips.extend(self.splitTripByTimeInterval(one_trip.route))
        return (pv_id,all_trips)


    def userTripToTripUser(self,user_trip_list):
        (user_id, trip_list) = user_trip_list
        trip_user_array = []
        for one_trip in trip_list:
            trip_user_array.append((one_trip.originDestination(),(one_trip,user_id)))
        return trip_user_array




