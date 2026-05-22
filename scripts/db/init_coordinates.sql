DELETE FROM Coordinates;
INSERT INTO Coordinates (altitude, latitudeDD, longitudeDD, codeSWstation, idPoint, startRain, EndRain)
SELECT CAST(DemTemp.dem_average AS INTEGER) as altitude,
DemTemp.lat as latitudeDD,
DemTemp.lon as longitudeDD,
'' as codeSWstation,
DemTemp.lat || '_' || DemTemp.lon as idPoint,
'' as startRain,
'' as EndRain
FROM DemTemp
