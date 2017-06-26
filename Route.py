from Record import *

class RouteBean():
    def __init__(self,route_num=None,stations = None):
        self.route_num = route_num
        self.stations = stations

class RouteHandler():
    def __init__(self):
        self.routes = []
        self.existing_routes = {}


class SubwayRouteHandler(RouteHandler):
    def __init__(self):
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
                else:
                    route_count = len(self.existing_routes.keys())
                    self.existing_routes[subwayRouteRecord.line_num] = route_count
                    route = RouteBean(route_num=subwayRouteRecord.line_num,stations=[subwayRouteRecord])
                    self.routes.append(route)

    def buildStationLineMap(self,file_path):
        if self.station_line_mapping is not None:
            return self.station_line_mapping
        self.station_line_mapping = {}
        with open(file_path) as file_handler:
            for one_line in file_handler:
                subwayRouteRecord = self.parseOneLine(one_line)
                self.station_line_mapping[subwayRouteRecord.station_name] = subwayRouteRecord.line_num
        return(self.station_line_mapping)

if __name__=='__main__':
    subwayRoute= SubwayRouteHandler()
    subwayRoute.buildRoutes("/media/zf72/Seagate Backup Plus Drive/E/DATA/edges/shenzhen_subway_station_line.csv")
