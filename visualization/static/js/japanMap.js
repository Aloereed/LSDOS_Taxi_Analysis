
var platform = new H.service.Platform({
  apikey: "V_-QdHSHsrim2UVx5uAZaqRpGEGNfYOmt48W2PUDa3M",
});

// configure an OMV service to use the `core` enpoint
var omvService = platform.getOMVService({ path: "v2/vectortiles/core/mc" });
var baseUrl = "https://js.api.here.com/v3/3.1/styles/omv/oslo/japan/";

// create a Japan specific style
var style = new H.map.Style(`${baseUrl}normal.day.yaml`, baseUrl);

// instantiate provider and layer for the basemap
var omvProvider = new H.service.omv.Provider(omvService, style);
var omvlayer = new H.map.layer.TileLayer(omvProvider, { max: 22 ,dark:true});

// instantiate (and display) a map:
var map = new H.Map(document.getElementById("map"), omvlayer, {
  zoom: 17,
  center: { lat: 35.68026, lng: 139.76744 },
});

// add a resize listener to make sure that the map occupies the whole container
window.addEventListener("resize", () => map.getViewPort().resize());

// MapEvents enables the event system
// Behavior implements default interactions for pan/zoom (also on mobile touch environments)
var behavior = new H.mapevents.Behavior(new H.mapevents.MapEvents(map));