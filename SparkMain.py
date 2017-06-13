 #coding=utf-8
from Spark import *
import time
class SubwayMain():
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

    def waitingTimeByDistricts(self,):
        districts = ['Luohu','Futian','Nanshan','Longgang','Baoan','Yantian']
        subway = SubwaySpark()
        for one_district in districts:
            self.waitingTimeFilterByStartDistrict(one_district,subway)
            self.waitingTimeFilterByDestinationDistrict(one_district,subway)

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
    def travelTimeOneDayByHours(self):
       # self.taxi.setLocalFilePath("/media/zf72/Seagate Backup Plus Drive/E/DATA/SmartCityRawData/sz/sample/taxi_gps_sample_2016_06_01")
        self.taxi.setHDFSFilePath('/zf72/transportation_data/taxi_gps/taxi_gps_sample_2016_06_01')
        self.taxi.buildRecordList()
        self.taxi.buildTripList()
    def test(self):
        self.taxi.test()

# if __name__ == "__main__":
#     #waitingTimeInDistricts()
#     taxi = TaxiMain()
#     # taxi.travelTimeOneDayByHours()
#     taxi.test()
