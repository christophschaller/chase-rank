import gpxpy
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point


def load_gpx(path: str) -> gpd.GeoDataFrame:
    with open(path) as file_pointer:
        gpx_content = gpxpy.parse(file_pointer)

    gpx_data = []
    gpx_geometry = []
    for point in gpx_content.tracks[0].segments[0].points:
        gpx_geometry.append(Point(point.longitude, point.latitude))
        gpx_data.append({
            # clear tzinfo until it can be handled for the match query
            # TODO: is this still causing issues with valhalla?
            "time": point.time.replace(tzinfo=None),
            "elev": point.elevation,
            "longitude": point.longitude,
            "latitude": point.latitude
        })

    gpx_frame = gpd.GeoDataFrame(
        gpx_data, geometry=gpx_geometry, crs="EPSG:4326").to_crs("EPSG:3857")
    # search for splits in the trace bigger than 1s and label consecutive sections
    # TODO: automatically determine default interval; it can't be 1s for every track right?
    gpx_frame["section"] = (gpx_frame["time"].diff() != pd.Timedelta("1 second")).cumsum()

    return gpx_frame
