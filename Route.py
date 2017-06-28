#coding=utf-8
from Record import *

class RouteBean():
    def __init__(self,route_num=None,stations = None,station_names = None):
        self.route_num = route_num
        self.stations = stations
        self.station_names = station_names

class RouteHandler():
    def __init__(self):
        pass



class SubwayRouteHandler(RouteHandler):
    def __init__(self):
        self.routes = []
        self.existing_routes = {}
        self.station_line_mapping = None

    def parseOneLine(self,one_line):
        attrs = one_line.split(',')
        line_num = attrs[0]
        station_name = attrs[1]
        translate  = attrs[2]
        transfer = attrs[3]
        order = attrs[4]
        one_record = SubwayRouteRecord(line_num=line_num,station_name=station_name,translate=translate,transfer= transfer,order=order)
        return one_record

    def buildRoutes(self,file_path):
        with open(file_path) as file_handler:
            for one_line in file_handler:
                subwayRouteRecord = self.parseOneLine(one_line)
                if subwayRouteRecord.line_num in self.existing_routes.keys():
                    route_index = self.existing_routes[subwayRouteRecord.line_num]
                    self.routes[route_index].stations.append(subwayRouteRecord)
                    self.routes[route_index].station_names.append(subwayRouteRecord.station_name)
                else:
                    route_count = len(self.existing_routes.keys())
                    self.existing_routes[subwayRouteRecord.line_num] = route_count
                    route = RouteBean(route_num=subwayRouteRecord.line_num,stations=[subwayRouteRecord],station_names=[subwayRouteRecord.station_name])
                    self.routes.append(route)

    def buildStationLineMap(self,file_path):
        if self.station_line_mapping is not None:
            return self.station_line_mapping
        self.station_line_mapping = {}
        with open(file_path) as file_handler:
            for one_line in file_handler:
                subwayRouteRecord = self.parseOneLine(one_line)
                if subwayRouteRecord.station_name in self.station_line_mapping.keys():
                    self.station_line_mapping[subwayRouteRecord.station_name].append(subwayRouteRecord.line_num)
                else:
                    self.station_line_mapping[subwayRouteRecord.station_name] = [subwayRouteRecord.line_num]
        return(self.station_line_mapping)

    def findPreviousStationsInSameLine(self,station_name):
        target_lines = self.station_line_mapping[station_name]
        previous_stations = {}
        for one_line in target_lines:
            one_route = self.routes[self.existing_routes[one_line]]
            stations = one_route.station_names
            previous_stations[one_line] = stations[0:stations.index(station_name)]
        return(previous_stations)

    def findLatterStationsInSameLine(self,station_name):
        target_lines = self.station_line_mapping[station_name]
        latter_stations = {}
        print(target_lines)
        for one_line in target_lines:
            one_route = self.routes[self.existing_routes[one_line]]
            stations = one_route.station_names
            latter_stations[one_line] = stations[stations.index(station_name)+1:]
        return(latter_stations)

    def getTripsAroundStationForWalkingTime(self,target_station):
        previous_stations = self.findPreviousStationsInSameLine(station_name=target_station)
        latter_stations = self.findLatterStationsInSameLine(station_name=target_station)
        

#
# if __name__=='__main__':
#     subwayRoute= SubwayRouteHandler()
#     subwayRoute.buildRoutes("/media/zf72/Seagate Backup Plus Drive/E/DATA/edges/shenzhen_subway_station_line.csv")
#     subwayRoute.buildStationLineMap("/media/zf72/Seagate Backup Plus Drive/E/DATA/edges/shenzhen_subway_station_line.csv")
#     previous_stations = subwayRoute.findPreviousStationsInSameLine("车公庙")
#     print(previous_stations.keys())
