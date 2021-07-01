var zonePolys = []; // zonePolys = {locationID:[poly1,...,polyn]}
var idArray = [];
var dataOut;
// var dataRoute; //
var dataRoutePU;
var dataRouteDO;
// var rangeOfCountInDataRoute=[0,0];
var rangeOfCountInDataRoutePU=[0,0];
var rangeOfCountInDataRouteDO=[0,0];
var dataHeatMapPU;
var dataHeatMapDO;
var rangeOfCountInDataHeatMapPU=[0,0];
var rangeOfCountInDataHeatMapDO=[0,0];
var rangeOfCount2ColorMap=[0.01,0.85];
var queryTime = 18; // hour - default 18
var center = { lat: 40.825167, lng: -73.361 } //
// var bubble=new H.ui.InfoBubble( new H.geo.Point(center.lat,center.lng) , {content:''});
var dataVehicleSpeedPU;
var dataVehicleSpeedDO;
var rangeOfSpeedPU = [0,0];
var rangeOfSpeedDO = [0,0];
var dataDistancePU;
var dataDistanceDO;
var rangeOfDistancePU = [0,0];
var rangeOfDistanceDO = [0,0];

var entryNumShow = 15;

var group = new H.map.Group(); // group 装 所有的 polygon
var initialStyle = {fillColor:'rgba(255, 100, 100, 0)',strokeColor:'#020202',lineWidth: 1};
var anotherStyle = {fillColor:'rgba(255, 100, 100, 0.5)',strokeColor:'#020202',lineWidth: 1};

var dataKM;
var k;
var location2cluster = {};

var myfun;
var event;

var color_mapper = {
    0: "#DD6B66",
    1: "#759AA0",
    2: "#E69D87",
    3: "#8DC1A9",
    4: "#EA7E53",
    5: "#EEDD78"
}


// Btn Load Data
let readBtn = d3.select('#readBtn').on('click',function () {
    d3.select(this).attr('disabled','true'); // 按钮 ：被禁用的元素既不可用，也不可点击
    loadZoneData();
    console.log('Data loading is complete');
});

// Btn Set
let timeBtn = d3.select('#timeBtn').on('click',function(){
    queryTime = +(d3.select('#inputTime').property('value'));
    // hour = +(d3.select('#inputTime').property('value'));
    if (queryTime<0 || queryTime>23 || !(isInteger(queryTime) )){
        alert('Query Hour should be integer in the range 0~23.');
    }
    else {
        console.log(queryTime);
        loadQueryData(queryTime);
        console.log('set time');
    }
});

// 点击后，展示当前时间段的 pick up 热力图(热点图) - 向后台请求数据；
let heatmapPUBtn = d3.select('#heatmapPUBtn').on('click',function(){
    console.log('heatmapPUBtn has been clicked');
    MIN_COUNT = rangeOfCountInDataHeatMapPU[0];
    MAX_COUNT = rangeOfCountInDataHeatMapPU[1];
    let count2opacity = d3.scaleLinear().domain([Math.log(MIN_COUNT),Math.log(MAX_COUNT)]).range(rangeOfCount2ColorMap);
    dataHeatMapPU.forEach(d=>{
        let LocationID = d.puLocationId;
        let tripCount = d.count;
        let opacity = count2opacity(Math.log(tripCount));
        let polys = zonePolys[LocationID];
        if (polys!=undefined){
            polys.forEach(polygon=>{
                polygon.setStyle({
                        fillColor:'rgba(255, 100, 100, '+opacity.toString()+')',
                        strokeColor:'#020202',
                        lineWidth: 1
                    }
                );
            });
        };
    });
});

// 点击后，展示当前时间段的 drop off 热力图(热点图) - 向后台请求数据；
let heatmapDOBtn = d3.select('#heatmapDOBtn').on('click',function(){
    console.log('heatmapDOBtn has been clicked');
    MIN_COUNT = rangeOfCountInDataHeatMapDO[0];
    MAX_COUNT = rangeOfCountInDataHeatMapDO[1];
    let count2opacity = d3.scaleLinear().domain([Math.log(MIN_COUNT),Math.log(MAX_COUNT)]).range(rangeOfCount2ColorMap);
    dataHeatMapDO.forEach(d=>{
        let LocationID = d.doLocationId;
        let tripCount = d.count;
        let opacity = count2opacity(Math.log(tripCount));
        let polys = zonePolys[LocationID];
        if(polys!=undefined){
            polys.forEach(polygon=>{
                polygon.setStyle({
                        fillColor:'rgba(255, 100, 100, '+opacity.toString()+')',
                        strokeColor:'#020202',
                        lineWidth: 1
                    }
                );
            });
        };
    });
});

// 点击后，展示当前时间段的 各个区域 pick up 所对应的 车速分布 热力图(热点图) - 向后台请求数据；
let speedPUBtn = d3.select("#speedPUBtn").on('click',function () {
    console.log('speedPUBtn has been clicked');
    let entryCount = 0;
    let stop = 0;
    // let context = 'No.\tID\tZone\t\t\tborough\n';
    MIN_SPEED = rangeOfSpeedPU[0];
    MAX_SPEED = rangeOfSpeedPU[1];
    let count2opacity = d3.scaleLinear().domain([Math.log(MIN_SPEED+1),Math.log(MAX_SPEED+1)]).range(rangeOfCount2ColorMap);
    myfun = count2opacity;
    dataVehicleSpeedPU.forEach(d=>{
        entryCount += 1;
        if (entryCount>entryNumShow){
            stop = 1;
        }
        let LocationID = Math.round(d.puLocationId);
        let speed = String(d.count);
        speed = +(speed.substring(0,speed.indexOf(".")+4)); // 保留小数点前3位
        console.log(speed,MIN_SPEED,MAX_SPEED);
        let opacity = count2opacity(Math.log(1+speed));
        console.log(opacity);
        let polys = zonePolys[LocationID];
        let flag = 0;
        let info;
        if (polys!=undefined){
            polys.forEach(polygon=>{
                if (flag == 0 && stop==0){
                    info = polygon.getData();
                    // context += String(entryCount)+'\t'+info.LocationID.toString()+'\t\t'+info.zone+'\t\t'+info.borough+'\n';
                    addOneRow(String(entryCount),info.LocationID.toString(),info.zone,info.borough,String(speed));
                    flag = 1;
                }
                polygon.setStyle({
                        fillColor:'rgba(18, 255, 33, '+opacity.toString()+')',
                        strokeColor:'#020202',
                        lineWidth: 1
                    }
                );
            });
        };
    });
});


// 点击后，展示当前时间段的 各个区域 drop off 所对应的 车速分布 热力图(热点图) - 向后台请求数据；
let speedDOBtn = d3.select("#speedDOBtn").on('click',function () {
    console.log('speedDOBtn has been clicked');
    let entryCount = 0;
    let stop = 0;
    // let context = 'No.\tID\tZone\t\t\tborough\n';
    MIN_SPEED = rangeOfSpeedDO[0];
    MAX_SPEED = rangeOfSpeedDO[1];
    let count2opacity = d3.scaleLinear().domain([Math.log(MIN_SPEED+1),Math.log(MAX_SPEED+1)]).range(rangeOfCount2ColorMap);
    dataVehicleSpeedDO.forEach(d=>{
        entryCount += 1;
        if (entryCount>entryNumShow){
            stop = 1;
        }
        let LocationID = Math.round(d.doLocationId);
        let speed = String(d.count);
        speed = +(speed.substring(0,speed.indexOf(".")+4)); // 保留小数点前3位
        let opacity = count2opacity(Math.log(1+speed));
        let polys = zonePolys[LocationID];
        let flag = 0;
        let info;
        if(polys!=undefined){
            polys.forEach(polygon=>{
                if (flag == 0 && stop==0){
                    info = polygon.getData();
                    addOneRow(String(entryCount),info.LocationID.toString(),info.zone,info.borough,String(speed));
                    flag = 1;
                }
                polygon.setStyle({
                        fillColor:'rgba(18, 255, 33, '+opacity.toString()+')',
                        strokeColor:'#020202',
                        lineWidth: 1
                    }
                );
            });
        };
    });
});

// 点击后，展示当前时间段的 各个区域 pick up 所对应的 行车距离分布 热力图(热点图) - 向后台请求数据；
let distancePUBtn = d3.select("#distancePUBtn").on('click',function () {
    console.log('distancePUBtn has been clicked');
    let entryCount = 0;
    let stop = 0;
    // let context = 'No.\tID\tZone\t\t\tborough\n';
    MIN_DISTANCE = rangeOfDistancePU[0];
    MAX_DISTANCE = rangeOfDistancePU[1];
    count2opacity = d3.scaleLinear().domain([MIN_DISTANCE,MAX_DISTANCE]).range(rangeOfCount2ColorMap);
    dataDistancePU.forEach(d=>{
        entryCount += 1;
        if (entryCount>entryNumShow){
            stop = 1;
        }
        let LocationID = Math.round(d.puLocationId);
        // console.log("Id Number:"+String(d.puLocationId));
        let distance = String(d.count);
        distance = +(distance.substring(0,distance.indexOf(".")+4)); // 保留小数点前3位
        let opacity = count2opacity(distance);
        let polys = zonePolys[LocationID];
        let flag = 0;
        let info;
        if (polys != undefined){
            polys.forEach(polygon=>{
            if (flag == 0 && stop==0){
                info = polygon.getData();
                // context += String(entryCount)+'\t'+info.LocationID.toString()+'\t\t'+info.zone+'\t\t'+info.borough+'\n';
                addOneRow(String(entryCount),info.LocationID.toString(),info.zone,info.borough,String(distance));
                flag = 1;
            }
            polygon.setStyle({
                    fillColor:'rgba(255, 171, 3, '+opacity.toString()+')',
                    strokeColor:'#020202',
                    lineWidth: 1
                }
            );
        });
        }
    });
});

// 点击后，展示当前时间段的 各个区域 drop off 所对应的 行车距离分布 热力图(热点图) - 向后台请求数据；
let distanceDOBtn = d3.select("#distanceDOBtn").on('click',function () {
    console.log('distanceDOBtn has been clicked');
    let entryCount = 0;
    let stop = 0;
    // let context = 'No.\tID\tZone\t\t\tborough\n';
    MIN_DISTANCE = rangeOfDistanceDO[0];
    MAX_DISTANCE = rangeOfDistanceDO[1];
    count2opacity = d3.scaleLinear().domain([MIN_DISTANCE,MAX_DISTANCE]).range(rangeOfCount2ColorMap);
    dataDistanceDO.forEach(d=>{
        entryCount += 1;
        if (entryCount>entryNumShow){
            stop = 1;
        }
        let LocationID = Math.round(d.doLocationId);
        let distance = String(d.count);
        distance = +(distance.substring(0,distance.indexOf(".")+4)); // 保留小数点前3位
        let opacity = count2opacity(distance);
        let polys = zonePolys[LocationID];
        let flag = 0;
        let info;
        if(polys!=undefined){
            console.log("Id Number:"+String(d.doLocationId));
            polys.forEach(polygon=>{
                if (flag == 0 && stop==0){
                    console.log("painting");
                    info = polygon.getData();
                    addOneRow(String(entryCount),info.LocationID.toString(),info.zone,info.borough,String(distance));
                    flag = 1;
                }
                polygon.setStyle({
                        fillColor:'rgba(255, 171, 3, '+opacity.toString()+')',
                        strokeColor:'#020202',
                        lineWidth: 1
                    }
                );
            });
        }else{
            console.log("NULL!!");
        };

    });
})

// 点击后, 初始化排行榜显示cleanRankingBtn
// let cleanRankingBtn = d3.select('#cleanRankingBtn').on('click',function () {
//     console.log("cleanRankingBtn has been clicked");
//     // d3.select('#rankingList').text("Ranking List - Showcase Some Statistical Information...");
//     $("#rankingTable tbody tr").remove(); // 删除表格所有行
// })
let kmBtn = d3.select('#kmBtn').on('click',function () {
    console.log("kmBtn has been clicked");
    // let mapper = d3.scaleLinear().domain([Math.log(MIN_COUNT),Math.log(MAX_COUNT)]).range(rangeOfCount2ColorMap);
    // let mapper = d3.scaleBand().domain(Array.from({length: k}, (a, i) => i)).range(rangeOfCount2ColorMap);
    dataKM.forEach(d=>{
        let LocationID = d.LocationID;
        let cluster = Math.round(+(d.cluster));
        let polys = zonePolys[LocationID];
        if(polys!=undefined){
            polys.forEach(polygon=>{
                polygon.setStyle({
                    fillColor:color_mapper[cluster],strokeColor:'#020202',lineWidth: 1
                }); // 将所有的区域（polygons）恢复到初始状态;
            });
        };
    });
    addOneRow(150.27,"","","",1);
    addOneRow(84.61,"","","",5);
    addOneRow(56.94,"","","",2);
    addOneRow(40.91,"","","",4);
    addOneRow(28.16,"","","",0);
    addOneRow(16.27,"","","",3);
});
// [array([28.16012677]), array([150.26874314]), array([56.94357779]), array([16.2717591]), array([40.91438379]), array([84.61061927])]

// 点击后，清除图上的所有效果/复原
let cleanBtn = d3.select('#cleanBtn').on('click',function(){
    console.log('cleanBtn has been clicked');
    idArray.forEach(LocationID=>{
        let polys = zonePolys[LocationID];
        polys.forEach(polygon=>{
            polygon.setStyle(initialStyle); // 将所有的区域（polygons）恢复到初始状态;
        });
    });
    $("#rankingTable tbody tr").remove(); // 删除表格所有行
});

// 利用我们所获得的 API-key 来初始化我们在后端所对应的service(由HERE REST APIs提供)
// 后端服务(service)处理我们前端(web)对地图数据的请求，并将其交付给应用程序进行显示
// 相当于这里初始化的 service 是我们访问 HERE APIs 的代理人;
let platform = new H.service.Platform({
    'apikey':'V_-QdHSHsrim2UVx5uAZaqRpGEGNfYOmt48W2PUDa3M',
});

// Obtain the default map types from the platform object:
// defaultLayers 包含了多种 地图风格，可自己选择
let defaultLayers = platform.createDefaultLayers();


// Instantiate (and display) a map object: - 初始化并展示一个地图;
// document.getElementById('mapContainer') —— 指定地图容器元素
// defaultLayers.vector.normal.map — 指定要使用的地图类型
let map = new H.Map(
    document.getElementById('mapContainer'),
    defaultLayers.vector.normal.map,
    {
      zoom: 10, // 显示 缩放级别
      center: center // 地图中心的地理坐标
    });
map.addObject(group);

let mapEvents = new H.mapevents.MapEvents(map);
// Instantiate the default behavior, providing the mapEvents object:
var behavior = new H.mapevents.Behavior(mapEvents);
// create default UI with layers provided by the platform
var ui = H.ui.UI.createDefault(map, defaultLayers);

// 可以在地图运行期间查询和改变风格信息，以便根据新的业务规则突出显示特征，或删除目前在应用程序中不需要的图层

function array2lnglats(arr){
    lnglats = [];
    for (i = 0; i < arr.length;) {
        lnglats.push({lng:arr[i],lat:arr[i+1]});
        i = i+2;
    }
    return lnglats
};

function loadZoneData(){    // 点击 Load Data 后执行, 加载区域的相关数据
    $.getJSON($SCRIPT_ROOT + "/_loadZoneData",{},function(data){
        // let zonesInfo = data.zonesInfo;
        console.log(data);
        dataOut = data;
        dataKM = data.kmeans_PU;
        data.kmeans_PU.forEach(d=>{
            location2cluster[d.LocationID]=d.cluster;
        });
        k = data.k;
        data.zonesInfo.forEach(d=>{ // d 对应 每个 zone
            locationID = +(d['LocationID']);
            zone = d['zone'];
            borough = d['borough'];
            console.log(locationID);
            if(!(locationID in zonePolys)){
                idArray.push(locationID);
                zonePolys[locationID]=[];
            }
            d['polygon'].forEach(arr=>{ // 依据每个 array 绘制一个 polygon
                let lineString = new H.geo.LineString();
                // Initialize a linestring and add all the points to it:
                array2lnglats(arr).forEach(point=>{
                    lineString.pushPoint(point);
                });
                let poly = new H.map.Polygon(lineString, {
                    style: initialStyle
                    // style: anotherStyle
                });
                poly.setData({'LocationID':locationID,'zone':zone,'borough':borough,'cluster':location2cluster[locationID]});
                zonePolys[locationID].push(poly);
                // 进如区域显示弹窗-显示相关地区的信息
                poly.addEventListener('pointerenter',function (evt) {
                    coord = map.screenToGeo(evt.currentPointer.viewportX,evt.currentPointer.viewportY)
                    // console.log(evt.type,evt.currentPointer.type);
                    event = evt;
                    polygonData = evt.target.getData();
                    LocationID = polygonData.LocationID;
                    zone = polygonData.zone;
                    borough = polygonData.borough;
                    d3.select('#IdText').text(LocationID);
                    d3.select('#ZoneText').text(zone);
                    d3.select('#BoroughText').text(borough);
                    d3.select('#ClusterText').text(location2cluster[LocationID]);
                }, false);
                // 离开区域显示弹窗-关闭相关地区的信息 - 没必要了
                // poly.addEventListener('pointerleave',function (evt) {
                //     console.log(evt.type,evt.currentPointer.type);
                // });
                poly.addEventListener('tap',function (evt) { // 显示 对应时间下 目的地是被点击区域的 出发区域的颜色
                    // 目前的想法是，点击后，我们向后台 返回当前的时间和被点击的polygon1对应的ID
                    // 然后从后端得到的是, 在当前 hour 对应的小时内，我们有哪些 region id 是
                    // console.log(evt.type,evt.currentPointer.type);
                    renderRouteColorDO(evt.target.getData().LocationID);
                });
                poly.addEventListener('longpress',function (evt) { // 显示 对应时间下 出发区域是被点击区域的 目的区域的颜色
                    // 目前的想法是，点击后，我们向后台 返回当前的时间和被点击的polygon1对应的ID
                    // 然后从后端得到的是, 在当前 hour 对应的小时内，我们有哪些 region id 是
                    // console.log(evt.type,evt.currentPointer.type);
                    renderRouteColorPU(evt.target.getData().LocationID);
                });
                // map.addObject(poly);
                group.addObject(poly);
                map.getViewModel().setLookAtData({bounds: poly.getBoundingBox()});
            });
        });
        // addInfoBubble(map);
    });
}
function isInteger(obj) {
    return obj%1 === 0
}
function loadQueryData(hour){   // 点击 Set 后执行
    $.getJSON($SCRIPT_ROOT + "/_loadQueryData"
        ,{hour:hour}
        ,function(data){
        console.log(data);
        // dataRoute = data.routeInfo;
        dataRoutePU = data.routeInfoPU;
        dataRouteDO = data.routeInfoDO;
        dataHeatMapPU = data.heatmapInfoPU;
        dataHeatMapDO = data.heatmapInfoDO;
        dataVehicleSpeedPU = data.vehicleSpeedPU;
        dataVehicleSpeedDO = data.vehicleSpeedDO;
        dataDistancePU = data.orderDistancePU;
        dataDistanceDO = data.orderDistanceDO;
        rangeOfCountInDataRoutePU[0]=data.MIN_COUNT_PU;
        rangeOfCountInDataRoutePU[1]=data.MAX_COUNT_PU;
        rangeOfCountInDataRouteDO[0]=data.MIN_COUNT_DO;
        rangeOfCountInDataRouteDO[1]=data.MAX_COUNT_DO;
        rangeOfCountInDataHeatMapPU[0]=data.MIN_COUNT_PU_Heat;
        rangeOfCountInDataHeatMapPU[1]=data.MAX_COUNT_PU_Heat;
        rangeOfCountInDataHeatMapDO[0]=data.MIN_COUNT_DO_Heat;
        rangeOfCountInDataHeatMapDO[1]=data.MAX_COUNT_DO_Heat;
        rangeOfSpeedPU[0] = data.MIN_SPEED_PU;
        rangeOfSpeedPU[1] = data.MAX_SPEED_PU;
        rangeOfSpeedDO[0] = data.MIN_SPEED_DO;
        rangeOfSpeedDO[1] = data.MAX_SPEED_DO;
        rangeOfDistancePU[0] = data.MIN_DISTANCE_PU;
        rangeOfDistancePU[1] = data.MAX_DISTANCE_PU;
        rangeOfDistanceDO[0] = data.MIN_DISTANCE_DO;
        rangeOfDistanceDO[1] = data.MAX_DISTANCE_DO;
        alert('loading data completed.');
    });
}

function loadHeatMapData(hour){
    // to be realized
    // $.getJSON($SCRIPT_ROOT + "/_loadHeatMapData"
    //     ,{hour:hour}
    //     ,function(data){
    //     console.log(data);
    //     dataHeatMap = data.routeInfo;
    // });
}

function renderRouteColorPU(centerLocationID){
    console.log(centerLocationID);
    let MAX_COUNT=0;
    let MIN_COUNT=1E10;
    let polygonsToChange = [];
    // dataRoute.forEach(d=>{
    dataRoutePU.forEach(d=>{
        let puLocationId = +(d.puLocationId);
        let tripCount = +(d.count);
        let doLocationId = +(d.doLocationId);
        if (puLocationId == +(centerLocationID)){
            if(tripCount > MAX_COUNT){
                MAX_COUNT = tripCount;
            }
            if(tripCount < MIN_COUNT){
                MIN_COUNT = tripCount;
            }
            polygonsToChange.push([doLocationId,tripCount]);
        }
    });
    count2opacity = d3.scaleLinear().domain([Math.log(MIN_COUNT),Math.log(MAX_COUNT)]).range(rangeOfCount2ColorMap);
    polygonsToChange.forEach(d=>{
        let doLocationId=d[0];
        let tripCount=d[1];
        let opacity = count2opacity(Math.log(tripCount));
        let polys = zonePolys[doLocationId];
        // 需要去除掉 id 为 264和265的区域;
        console.log(doLocationId);
        console.log(tripCount);
        console.log(opacity);
        if (polys!=undefined){
            polys.forEach(polygon=>{
                polygon.setStyle({
                    fillColor:'rgba(255, 100, 100, '+opacity.toString()+')',
                    strokeColor:'#020202',
                    lineWidth: 1});
            });
        };
    });
}

function renderRouteColorDO(centerLocationID){
    console.log(centerLocationID);
    let MAX_COUNT=0;
    let MIN_COUNT=1E10;
    let polygonsToChange = [];
    // dataRoute.forEach(d=>{
    dataRouteDO.forEach(d=>{
        let puLocationId = +(d.puLocationId);
        let tripCount = +(d.count);
        let doLocationId = +(d.doLocationId);
        if (doLocationId == +(centerLocationID)){
            if(tripCount > MAX_COUNT){
                MAX_COUNT = tripCount;
            }
            if(tripCount < MIN_COUNT){
                MIN_COUNT = tripCount;
            }
            polygonsToChange.push([puLocationId,tripCount]);
        }
    });
    count2opacity = d3.scaleLinear().domain([Math.log(MIN_COUNT),Math.log(MAX_COUNT)]).range(rangeOfCount2ColorMap);
    polygonsToChange.forEach(d=>{
        let puLocationId=d[0];
        let tripCount=d[1];
        let opacity = count2opacity(Math.log(tripCount));
        let polys = zonePolys[puLocationId];
        // 需要去除掉 id 为 264和265的区域;
        console.log(puLocationId);
        console.log(tripCount);
        console.log(opacity);
        if(polys!=undefined){
            polys.forEach(polygon=>{
                polygon.setStyle({
                    fillColor:'rgba(255, 100, 100, '+opacity.toString()+')',
                    strokeColor:'#020202',
                    lineWidth: 1});
            });
        };
    });
}

function addOneRow(number,id,zone,borough,value){
    let table=document.getElementById("innerTbody");
    let row=table.insertRow(-1);
    row.insertCell(0).innerHTML=number;
    row.insertCell(1).innerHTML=id;
    row.insertCell(2).innerHTML=zone;
    row.insertCell(3).innerHTML=borough;
    row.insertCell(4).innerHTML=value;
}

// define a map from heatvalue to color
let colors = new H.data.heatmap.Colors({
    '0'  : 'rgba(0, 0, 136, 0.5)',     // dark blue
    '0.2': 'rgba(0, 187, 0, 0.5)',   // medium green
    '0.5': 'rgba(255, 255, 0, 0.5)', // half-transparent yellow
    '0.7': 'rgba(255, 0, 0, 0.5)'  // half-transparent white
  },
  true  // interpolate between the stops to create a smooth color gradient
);


