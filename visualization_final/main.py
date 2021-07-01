# -- coding: utf-8 --
# @Time : 2021/5/29 19:17
# @Author : LUO Ziqin
# @File : main.py
# @SoftWare: PyCharm

from flask import Flask, render_template\
    , request, url_for, send_from_directory\
    , Markup, session, redirect, abort, jsonify
import pandas as pd
zones = pd.read_csv('./static/data/taxi_zones.csv')
data = pd.read_csv('./static/data/analyze_pattern.csv')
data_with_hour_pickUp = pd.read_csv('./static/data/route_data/analyze_pattern_hourascend_pickupDateTime.csv')
data_with_hour_dropOff = pd.read_csv('./static/data/route_data/analyze_pattern_hourascend_dropoffDateTime.csv')
data_heatmap_pickUp = pd.read_csv('./static/data/heatmap_data/count_locationid_hourascend_pickupDateTime.csv')
data_heatmap_dropOff = pd.read_csv('./static/data/heatmap_data/count_locationid_hourascend_dropoffDateTime.csv')

data_vehicleSpeed_pickUp = pd.read_csv('./static/data/speed_data/speedPU.csv')
data_vehicleSpeed_dropOff = pd.read_csv('./static/data/speed_data/speedDO.csv')
data_orderDistance_pickUp = pd.read_csv('./static/data/distance_data/distancePU.csv')
data_orderDistance_dropOff = pd.read_csv('./static/data/distance_data/distanceDO.csv')

# 按订单的平均价格进行聚类
data_kmeans = pd.read_csv('./static/data/kmeans/pu_means_kmeans.csv')
k = 6

rows,cols = zones.shape
rowsKM,colsKM = data_kmeans.shape
zonesInfo = []
kmInfo = []
# data = pd.read_csv('./static/data/Taxidata_20180501_20180526.csv')
# 创建应用程序
# web 应用程序

app = Flask(__name__)

# for i in range(len(res)):
#     if res[i]==')':
#         print(i)

def str2poly(polyStr):
    polyStr = polyStr[16:-3]
    n = len(polyStr)
    newStr = []
    for i in range(n):
        if polyStr[i]==' ' :
            if polyStr[i-1]==',':
                continue
            else:
                newStr.append(',')
        else:
            newStr.append(polyStr[i])
    # return ''.join(newStr)
    return list(eval(''.join(newStr)))

# data processing
for i in range(rows):
    # print(i)
    entry = zones.loc[i]
    polyStr = entry['the_geom']
    zonesInfo.append({'LocationID':str(entry['LocationID']),'zone':entry['zone'],'borough':entry['borough']
                    ,'polygon':eval(entry['the_geom'])})

for i in range(rowsKM):
    entry = data_kmeans.loc[i]
    kmInfo.append({"LocationID":str(entry["pulocationid"]),"cluster":str(entry["prediction"])})

def filterTime(hour,data,key='tpepPickupDateTime',filter=True):
    print(hour)
    if filter:
        targetData = data[data[key]==hour]
    else:
        targetData = data
    targetData = targetData.reset_index(drop=True)
    rows,cols = targetData.shape
    MAX_COUNT = targetData['count'].max()
    MIN_COUNT = targetData['count'].min()
    res = []
    for i in range(rows):
        entry = targetData.loc[i]
        res.append({'puLocationId':str(entry.get('puLocationId',-1)),'doLocationId':str(entry.get('doLocationId',-1))
                       ,'count':str(entry.get('count',0)),'hour':str(hour)})
    return [res,MIN_COUNT,MAX_COUNT]



# routing
@app.route("/")
def index():
    return render_template('index.html',title='大规模分布式系统')

@app.route("/_loadZoneData")
def loadZoneData():
    return jsonify(zonesInfo=zonesInfo, kmeans_PU = kmInfo, k=k)

@app.route("/_loadQueryData")
def loadQueryData():
    hour = request.args.get('hour', 18, type=int) # 默认查询晚上 18 点
    # temp1 = temp2 = 0
    # routeInfo,MIN_COUNT,MAX_COUNT=filterTime(hour=hour,data=data)
    routeInfoPU, MIN_COUNT_PU, MAX_COUNT_PU = filterTime(hour=hour, data=data_with_hour_pickUp
                                                         , key='tpepPickupDateTime')
    routeInfoDO, MIN_COUNT_DO, MAX_COUNT_DO = filterTime(hour=hour, data=data_with_hour_dropOff
                                                         , key='tpepDropoffDateTime')
    heatmapInfoPU, MIN_COUNT_PU_Heat, MAX_COUNT_PU_Heat = filterTime(hour=hour, data=data_heatmap_pickUp
                                                                     , key='tpepPickupDateTime')
    heatmapInfoDO, MIN_COUNT_DO_Heat, MAX_COUNT_DO_Heat = filterTime(hour=hour, data=data_heatmap_dropOff
                                                                     , key='tpepDropoffDateTime')
    vehicleSpeedPU, MIN_SPEED_PU, MAX_SPEED_PU = filterTime(hour=hour,data=data_vehicleSpeed_pickUp
                                                            , key='tpepPickupDateTime')
    vehicleSpeedDO, MIN_SPEED_DO, MAX_SPEED_DO = filterTime(hour=hour, data=data_vehicleSpeed_dropOff
                                                            , key='tpepDropoffDateTime')
    orderDistancePU, MIN_DISTANCE_PU, MAX_DISTANCE_PU = filterTime(hour=hour,data=data_orderDistance_pickUp
                                                                   , key='tpepPickupDateTime',filter=False)
    orderDistanceDO, MIN_DISTANCE_DO, MAX_DISTANCE_DO = filterTime(hour=hour, data=data_orderDistance_dropOff
                                                                   , key='tpepDropoffDateTime',filter=False)
    # return jsonify(routeInfo=routeInfo,MIN_COUNT=MIN_COUNT,MAX_COUNT=MAX_COUNT)
    return jsonify(routeInfoPU=routeInfoPU, routeInfoDO=routeInfoDO,
                   MIN_COUNT_PU=MIN_COUNT_PU, MIN_COUNT_DO=MIN_COUNT_DO,
                   MAX_COUNT_PU=MAX_COUNT_PU, MAX_COUNT_DO=MAX_COUNT_DO,
                   heatmapInfoPU=heatmapInfoPU,heatmapInfoDO=heatmapInfoDO,
                   MIN_COUNT_PU_Heat=MIN_COUNT_PU_Heat,MAX_COUNT_PU_Heat=MAX_COUNT_PU_Heat,
                   MIN_COUNT_DO_Heat=MIN_COUNT_DO_Heat,MAX_COUNT_DO_Heat=MAX_COUNT_DO_Heat,
                   vehicleSpeedPU=vehicleSpeedPU,vehicleSpeedDO=vehicleSpeedDO,
                   MIN_SPEED_PU=MIN_SPEED_PU,MAX_SPEED_PU=MAX_SPEED_PU,
                   MIN_SPEED_DO=MIN_SPEED_DO,MAX_SPEED_DO=MAX_SPEED_DO,
                   orderDistancePU=orderDistancePU,orderDistanceDO=orderDistanceDO,
                   MIN_DISTANCE_PU=MIN_DISTANCE_PU, MAX_DISTANCE_PU=MAX_DISTANCE_PU,
                   MIN_DISTANCE_DO=MIN_DISTANCE_DO, MAX_DISTANCE_DO=MAX_DISTANCE_DO)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=11666)
    app.debug=True