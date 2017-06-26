#coding=utf-8
from Spark import *
import time
class SubwayMain():
    def __init__(self):
        self.subway = SubwaySpark()

    def subwayWaitingTime(self):
        subway = SubwaySpark()
        test_file = [('a',44),('a',10),('a',11),('a',12),('a',5),('c',3),('b',2),('a',1),('a',6),('a',3)]
        subway.setLocalInputFile(test_file)
        subway.buildRecordList()

    def waitingTimeInCity(self):
        subway = SubwaySpark()
        subway.setHDFSFilePath('/zf72/transportation_data/subway_bus/P_GJGD_SZT_20160601')
        #subway.setLocalFilePath("/home/zf72/Dropbox/projects/off-peak-trans/data/SZT/P_GJGD_SZT_20160601")
        #subway.setLocalFilePath("/home/zf72/Dropbox/projects/off-peak-trans/data/sample/public_sample.txt")
        subway.buildRecordList()
        subway.buildTripList()
        # subway.buildTripTimeMatrix()
        subway.buildInVehicleTime()
        subway.buildWaitingTime()
        #subway.saveAverageTripTime('/zf72/transportation_data/result/P_GJGD_SZT_20160601',local=False)
        #subway.saveAverageTripTime("file:/home/zf72/Dropbox/projects/off-peak-trans/data/SZT_RESULT/P_GJGD_SZT_20160601",local=True)


    def waitingTimeFilterByStartDistrict(self,start_district,subway=None):
        if subway is None:
            subway = SubwaySpark()
        subway.setHDFSFilePath('/zf72/transportation_data/subway_bus/P_GJGD_SZT_20160601')
        # subway.setLocalFilePath("/home/zf72/Dropbox/projects/off-peak-trans/data/sample/public_sample.txt")
        subway.buildRecordList()
        subway.buildTripList()
        subway.buildInVehicleTime()
        subway.buildDistrictFilter(start_district)
        subway.filterTripListByStartDistrict()
        subway.buildWaitingTime("from_"+start_district)


    def waitingTimeFilterByDestinationDistrict(self,end_district,subway=None):
        if subway is None:
            subway = SubwaySpark()
        subway.setHDFSFilePath('/zf72/transportation_data/subway_bus/P_GJGD_SZT_20160601')
        # subway.setLocalFilePath("/home/zf72/Dropbox/projects/off-peak-trans/data/sample/public_sample.txt")
        subway.buildRecordList()
        subway.buildTripList()
        subway.buildInVehicleTime()
        subway.buildDistrictFilter(end_district)
        subway.filterTripListByDestinationDistrict()
        subway.buildWaitingTime("to_"+end_district)

    def waitingTimeByDistricts(self):
        districts = ['Luohu','Futian','Nanshan','Longgang','Baoan','Yantian']
        subway = SubwaySpark()
        for one_district in districts:
            self.waitingTimeFilterByStartDistrict(one_district,subway)
            self.waitingTimeFilterByDestinationDistrict(one_district,subway)


    def inOneStationWalkingTime(self):
        pass

    def inStationsAverageWalkingTime(self):
        pass

    def travelTimeBetweenTwoStation(self,from_station,to_station):
        pass

class BusMain():
    def __init__(self):
        self.bus = BusSpark()
    def maskBusID(self):
        # self.bus.setLocalFilePath("/home/zf72/Dropbox/projects/off-peak-trans/data/sample/public_sample.txt")
        self.bus.setHDFSFilePath("/zf72/transportation_data/subway_bus/P_GJGD_SZT_20160601")
        self.bus.markSmartCardID(output_file_path="/zf72/transportation_data/result/mask/bus_data_0601")

class TaxiMain():
    def __init__(self):
        self.taxi = TaxiSpark()
    def travelDelayTimeOneDayByHours(self):
        #self.taxi.setLocalFilePath("/media/zf72/Seagate Backup Plus Drive/E/DATA/SmartCityRawData/sz/sample/taxi_gps_sample_2016_06_01")
        # self.taxi.setHDFSFilePath('/zf72/transportation_data/taxi_gps/taxi_gps_sample_2016_06_01')
        self.taxi.setHDFSFilePath('/zf72/transportation_data/taxi_gps/GPS_2016_06_02')
        #self.taxi.setHDFSFilePath('')
        self.taxi.buildRecordList()
        self.taxi.buildTripList()
        self.taxi.buildODTravelTime()
        self.taxi.buildDelayTimeDistribution()
        self.taxi.average_delay_time.saveAsTextFile('/zf72/transportation_data/result/taxi_delay_time')

    def odTravelTime(self):
        self.taxi.setHDFSFilePath('/zf72/transportation_data/taxi_gps/GPS_2016_06_02')
        self.taxi.buildRecordList()
        self.taxi.buildTripList()
        self.taxi.buildODTravelTime()
        self.taxi.od_minimum_time.saveAsTextFile('/zf72/transportation_data/result/od_minimum_time_taxi')

    def travelTimeOneDayByHoursFilterByStartDistrict(self,district_name):
        self.taxi.setStartFilterDistrictName(district_name)
        self.taxi.setHDFSFilePath('/zf72/transportation_data/taxi_gps/GPS_2016_06_02')
        self.taxi.buildRecordList()
        self.taxi.buildTripList()
        self.taxi.buildODTravelTime()
        self.taxi.filterTripByStartDistrict(district_name)
        self.taxi.buildDelayTimeDistribution()
        self.taxi.average_delay_time.saveAsTextFile('/zf72/transportation_data/result/taxi_delay_time_districts/'+district_name)

    def travelTimeOneDayByHoursFilterByStartDistricts(self):
        districts = ['Luohu','Futian','Nanshan','Longgang','Baoan','Yantian']
        for one_district in districts:
            self.travelTimeOneDayByHoursFilterByStartDistrict(one_district)

    def travelTimeOneDayByHoursFilterByStartEndDistrict(self,start_district_name=None,end_district_name=None):
        self.taxi.setStartFilterDistrictName(start_district_name)
        self.taxi.setHDFSFilePath('/zf72/transportation_data/taxi/input/GPS_2016_06_01')
        self.taxi.buildRecordList()
        self.taxi.buildTripList()
        self.taxi.buildODTravelTime()
        if start_district_name is not None:
            self.taxi.filterTripByStartDistrict(start_district_name)
        if end_district_name is not None:
            self.taxi.filterTripByEndDistrict(end_district_name)
        self.taxi.buildDelayTimeDistribution()
        self.taxi.average_delay_time.saveAsTextFile('/zf72/transportation_data/taxi/output/taxi_delay_time_districts/'+start_district_name+"_"+end_district_name)

    def travelTimeOneDayByHoursFilterByStartEndDistricts(self):
        #districts = ['Luohu','Futian','Nanshan','Longgang','Baoan','Yantian']
        districts = ['Luohu','Futian']
        for one_district in districts:
            self.travelTimeOneDayByHoursFilterByStartEndDistrict(one_district,one_district)

    def test(self):
        self.taxi.test()

class PVMain():
    def __init__(self):
        self.pv = PVSpark()

    def travelDelayTimeOneDayByHours(self):
        #self.pv.setHDFSFilePath('/zf72/transportation_data/pv/input/20160101_000026.gtd.txt')
        self.pv.setHDFSFilePath('/zf72/transportation_data/pv/input/20160101_*.gtd.txt')
        # self.pv.setLocalFilePath("/media/zf72/Seagate Backup Plus Drive/E/DATA/SmartCityRawData/sz/privateCar/2016-01-txt/pv_sample_01_31")
        self.pv.buildRecordList()
        self.pv.buildTripList()
        self.pv.buildODTravelTime()
        self.pv.buildDelayTimeDistribution()
        self.pv.average_delay_time.saveAsTextFile('/zf72/transportation_data/pv/output/pv_delay_time')

    def travelTimeOneDayByHoursFilterByStartEndDistrict(self,start_district_name=None,end_district_name=None):
        self.pv.setStartFilterDistrictName(start_district_name)
        self.pv.setHDFSFilePath('/zf72/transportation_data/pv/input/20160101_*.gtd.txt')
        self.pv.buildRecordList()
        self.pv.buildTripList()
        self.pv.buildODTravelTime()
        if start_district_name is not None:
            self.pv.filterTripByStartDistrict(start_district_name)
        if end_district_name is not None:
            self.pv.filterTripByEndDistrict(end_district_name)
        self.pv.buildDelayTimeDistribution()
        self.pv.average_delay_time.saveAsTextFile('/zf72/transportation_data/result/pv_delay_time_districts/'+start_district_name+"_"+end_district_name)

    def travelTimeOneDayByHoursFilterByStartEndDistricts(self):
        #districts = ['Luohu','Futian','Nanshan','Longgang','Baoan','Yantian']
        districts = ['Luohu','Futian']
        for one_district in districts:
            self.travelTimeOneDayByHoursFilterByStartEndDistrict(one_district,one_district)



if __name__ == "__main__":
    taxi = TaxiMain()
    # pv.travelDelayTimeOneDayByHours()
    taxi.travelTimeOneDayByHoursFilterByStartEndDistricts()
