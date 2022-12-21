from typing import Dict, Tuple, List

import gpxpy
import geopandas as gpd
import pandas as pd
import routingpy.exceptions
from shapely.geometry import LineString, Point
from routingpy import Valhalla, exceptions
from routingpy import utils as routingpy_utils

# TODO: get the address from env vars
VALHALLA_CLIENT = Valhalla(base_url="http://127.0.0.1:8002")


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


def load_trace(matched_points: List, edges_size: int) -> gpd.GeoDataFrame:
    # edges_size should be large enough to filter out the strange outlies in edge_index
    # TODO: find large outliers in edge_index without accessing edges or using an arbitrary number

    trace_data = []
    trace_geometry = []
    for index, point in enumerate(matched_points):
        trace_geometry.append(Point(point["lon"], point["lat"]))
        if point.get("edge_index"):
            # sometimes the edge_index seems to hold a super high number (maybe an id?)
            # if this happens we set the edge_index to the same as the previous points
            if point["edge_index"] >= edges_size:
                if index > 0:
                    point["edge_index"] = matched_points[index - 1]["edge_index"]
                else:
                    point["edge_index"] = 0
        elif edges_size:
            if index == 0:
                point["edge_index"] = 0
            if index == len(matched_points) - 1:
                point["edge_index"] = edges_size - 1
        else:
            point["edge_index"] = None
        trace_data.append(point)

    trace_df = gpd.GeoDataFrame(
        trace_data,
        geometry=trace_geometry,
        crs="EPSG:4326").to_crs("EPSG:3857")

    return trace_df


def load_edges(edges: List, match_shape: List) -> gpd.GeoDataFrame:
    edge_data = []
    edge_geometry = []
    for edge in edges:
        edge_points = match_shape[edge["begin_shape_index"]:edge["end_shape_index"]+1]
        edge_geometry.append(LineString(edge_points))
        # TODO: figure out what is most often available and what we really need
        edge_data.append({
            "length": edge.get("length"),
            "speed": edge.get("speed"),
            "road_class": edge.get("road_class"),
            "traversability": edge.get("traversability"),
            "use": edge.get("use"),
            "unpaved": edge.get("unpaved"),
            "tunnel": edge.get("tunnel"),
            "bridge": edge.get("bridge"),
            "roundabout": edge.get("roundabout"),
            "internal_intersection": edge.get("internal_intersection"),
            "surface": edge.get("surface"),
            "travel_mode": edge.get("travel_mode"),
            "osm_way_id": edge.get("way_id"),
            "max_upward_grade": edge.get("max_upward_grade"),
            "max_downward_grade": edge.get("max_downward_grade"),
            "mean_elevation": edge.get("mean_elevation"),
            "sac_scale": edge.get("sac_scale"),
            "speed_limit": edge.get("speed_limit"),
            "indoor": edge.get("indoor"),
        })

    return gpd.GeoDataFrame(
        edge_data,
        geometry=edge_geometry,
        crs="EPSG:4326").to_crs("EPSG:3857")


def load_match(match: Dict) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    # TODO: is it always a polyline6? how can we figure that out before getting an inf somewhere?
    match_shape = routingpy_utils.decode_polyline6(match["shape"])
    trace_df = load_trace(match["matched_points"], len(match["edges"]))
    edges_df = load_edges(match["edges"], match_shape)

    return trace_df, edges_df


def match_section(section: gpd.GeoDataFrame) -> Dict:
    # most of this is taken from routingpy.Valhalla.trace_attributes()
    # but the whole parsing section of routingpy is causing too much hassle
    # TODO: encode locations and timecodes into polyline instead of just passing locations
    locations = section[["longitude", "latitude"]].values.tolist()
    print("locations", len(locations))
    if len(locations) <= 1:
        # if we got less than two points its not a proper shape
        return {

        }

    encoded_polyline = None
    filters = None
    filters_action = None
    # "costing_options": {"bicycle": {"ignore_access": True}}
    # https://valhalla.readthedocs.io/en/latest/api/turn-by-turn/api-reference/#costing-models
    costing_options = {
        "ignore_access": True,  # allow to use roads we are not allowed to use -> oneway in false direction etc.
                                # does not include stairs :(
        "maneuver_penalty": 5,  # default is 5 seconds / penalty for switching to road with different name
        "bicycle_type": "Cross",  # Road, Hybrid / City, Cross, Mountain
        # "cycling_speed": 20,  # 20 default for Cross
        "use_hills": 0.5,  # default 0.5
        "use_living_streets": 0.5,  # default 0.5
        "avoid_bad_surfaces": 0,  # default 0.25
        "shortest": False  # always set False -> deactivates all other costing_options
    }
    dry_run = None

    get_params = {"access_token": VALHALLA_CLIENT.api_key} if VALHALLA_CLIENT.api_key else {}
    if locations and encoded_polyline:
        raise ValueError
    # search_radius
    params = VALHALLA_CLIENT.get_trace_attributes_params(
        locations,
        "bicycle",          # TODO: possible fallback to walking?
        "map_snap",
        encoded_polyline,
        filters,
        filters_action,
        costing_options,

    )
    # "costing_options":{"bicycle":{"ignore_access":true}}

    # TODO: some error handling with proper messages instead of just returning whatever we get
    try:
        return VALHALLA_CLIENT.client._request(
            "/trace_attributes", get_params=get_params, post_params=params, dry_run=dry_run
        )
    except routingpy.exceptions.RouterApiError as e:
        print(e)
        return {}


def match_track(track: gpd.GeoDataFrame) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    traces = []
    edges = []

    print(len(track.groupby(track["section"])), "\n")
    edge_index_offset = 0
    for i, section in track.groupby(track["section"]):
        print("\n", i)
        match = match_section(section)
        print(match)

        if match.get("matched_points"):
            trace_df = load_trace(match["matched_points"], len(match["edges"]))
        else:
            data = [{
                "lon": None,
                "lat": None,
                "type": None,
                "edge_index": None,
                "distance_along_edge": None,
                "distance_from_trace_point": None
            } for point in range(len(section))]
            trace_df = gpd.GeoDataFrame(data)

        # if "edge_index" in trace_df.keys() and match["edges"]:
        if match.get("edges"):
            match_shape = routingpy_utils.decode_polyline6(match["shape"])
            edges_df = load_edges(match["edges"], match_shape)
            try:
                trace_df["edge_index"] = trace_df["edge_index"].apply(
                    lambda index: index + edge_index_offset if isinstance(index, int) else None)
            except TypeError as e:
                print("### Exception")
                print(match["matched_points"])
                print(match["edges"])
                print(trace_df["edge_index"])
                print()
                raise e

            edge_index_offset += len(edges_df)
            edges.append(edges_df)

        traces.append(trace_df)

    trace_df = pd.concat(traces, axis=0).reset_index(drop=True)
    edges_df = pd.concat(edges, axis=0).reset_index(drop=True)

    return trace_df, edges_df


if __name__ == '__main__':
    TEST_TRACK_PATH = "../data/routes/test_track.gpx"

    with open(TEST_TRACK_PATH) as file_pointer:
        gpx_content = gpxpy.parse(file_pointer)

    gpx_data = []
    gpx_geometry = []
    for point in gpx_content.tracks[0].segments[0].points:
        gpx_geometry.append(Point(point.longitude, point.latitude))
        gpx_data.append({
            # clear tzinfo until it can be handled for the match query
            "time": point.time.replace(tzinfo=None),
            "elev": point.elevation,
            "longitude": point.longitude,
            "latitude": point.latitude
        })

    gpx_frame = gpd.GeoDataFrame(
        gpx_data, geometry=gpx_geometry, crs="EPSG:4326").to_crs("EPSG:3857")
    # search for splits in the trace bigger than 1s and label consecutive sections
    gpx_frame["section"] = (gpx_frame["time"].diff() != pd.Timedelta("1 second")).cumsum()

    trace_df, edges_df = match_track(gpx_frame)