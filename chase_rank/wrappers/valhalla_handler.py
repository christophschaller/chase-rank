from typing import Dict, List, Tuple

import requests
import geopy.distance
import geopandas as gpd
import numpy as np
import pandas as pd
from routingpy import utils as routingpy_utils
from shapely.geometry import Point, LineString


class ValhallaHandler:

    def __init__(self, base_url: str = "http://127.0.0.1:8002"):
        self.base_url = base_url

    @staticmethod
    def _request(method: str, url: str, params: Dict = None, json: Dict = None):
        headers = {}
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=json
        )
        if not response.ok:
            print("lol", response)
            print(response.content)
            return None
        # b'{"error_code":154,"error":"Path distance exceeds the max distance limit: 200000 meters",
        # "status_code":400,"status":"Bad Request"}'
        # b'{"error_code":171,"error":"No suitable edges near location","status_code":400,"status":"Bad Request"}'

        if response.ok:
            return response

    @staticmethod
    def _load_trace(matched_points: List, edges_size: int) -> gpd.GeoDataFrame:
        # edges_size should be large enough to filter out the strange outlies in edge_index
        # TODO: find large outliers in edge_index without accessing edges or using an arbitrary number

        trace_data = []
        trace_geometry = []
        for index, point in enumerate(matched_points):
            trace_geometry.append(Point(point["lon"], point["lat"]))
            if point.get("edge_index") is not None:
                # sometimes the edge_index seems to hold a super high number (maybe an id?)
                # if this happens we set the edge_index to the same as the previous points
                if point["edge_index"] >= edges_size:
                    if index > 0:
                        previous_edge_index = matched_points[index - 1].get("edge_index")
                        point["edge_index"] = previous_edge_index if previous_edge_index else None
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

    @staticmethod
    def _load_edges(edges: List, match_shape: List) -> gpd.GeoDataFrame:
        edge_data = []
        edge_geometry = []
        for edge in edges:
            edge_points = match_shape[edge["begin_shape_index"]:edge["end_shape_index"] + 1]
            if len(edge_points) > 1:
                edge_geometry.append(LineString(edge_points))
            else:
                edge_geometry.append(Point(edge_points[0]))
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

    @staticmethod
    def _combine_data(gpx_df, trace_df, edges_df):
        empty_edge = edges_df.iloc[0].copy()
        empty_edge.values[:] = None
        trace_data_df = trace_df.apply(
            lambda row: edges_df.iloc[int(row["edge_index"])]
            if not pd.isnull(row["edge_index"]) else empty_edge, axis=1)
        trace_data_df = trace_data_df[
            ["length", "speed", "use", "unpaved", "surface", "travel_mode", "osm_way_id", "geometry"]]
        trace_data_df["surface_section"] = (trace_data_df["surface"] != trace_data_df.shift()["surface"]).cumsum()
        gpx_df_copy = gpx_df.copy()
        gpx_df_copy[["surface", "surface_section", "use", "osm_way_id"]] = trace_data_df[
            ["surface", "surface_section", "use", "osm_way_id"]]

        # get distance
        shift_frame = gpx_df_copy.shift(-1).drop("geometry", axis=1).rename(
            columns={"longitude": "longitude_2", "latitude": "latitude_2"})

        def dist(row: pd.core.series.Series) -> np.float64:
            return geopy.distance.geodesic(
                (row["latitude"], row["longitude"]), (row["latitude_2"], row["longitude_2"])
            ).meters

        gpx_df_copy["distance"] = pd.concat([gpx_df_copy, shift_frame], axis=1)[
            ["longitude", "latitude", "longitude_2", "latitude_2"]
        ].fillna(method="ffill", axis=0).apply(dist, axis=1).fillna(np.float64(0))
        del shift_frame

        return gpx_df_copy

    def _match_section(self, locations: List[Tuple[int, int]], timestamps: List[int] = None) -> Dict:
        if len(locations) <= 1:
            # if we got less than two points it's not a proper shape
            return {}

        url = f"{self.base_url}/trace_attributes"
        if timestamps is None:
            locations = [{"lon": lon, "lat": lat} for lon, lat in locations]
        else:
            locations = [
                {"lon": lon, "lat": lat, "time": time}
                for lon, lat, time in zip(*zip(*locations), timestamps)
            ]

        # the following dict holds parameters to tune the costing model used for matching
        # https://valhalla.readthedocs.io/en/latest/api/turn-by-turn/api-reference/#costing-models
        # "ignore_access" was undocumented the las time I looked,
        # it allows usage of oneways in the wrong direction etc. but not stairs and other surfaces deemed unrideable
        costing_options = {
            "ignore_access": True,
            # allow to use roads we are not allowed to use -> oneway in false direction etc.
            # does not include stairs :(
            "maneuver_penalty": 5,  # default is 5 seconds / penalty for switching to road with different name
            "bicycle_type": "Cross",  # Road, Hybrid / City, Cross, Mountain
            "cycling_speed": 20,  # 20 default for Cross
            "use_hills": 0.5,  # default 0.5
            "use_living_streets": 0.5,  # default 0.5
            "avoid_bad_surfaces": 0,  # default 0.25
            "shortest": False  # always set False -> deactivates all other costing_options
        }

        payload = {
            "shape": locations,
            "encoded_polyline": None,
            "shape_match": "map_snap",  # default "walk_or_snap" -> we can't use walk
            "filters": None,  # TODO: filter the response fields we actually need
            "action": None,
            "costing": "bicycle",  # the costing model to use for matching
            # TODO: possible fallback to walking?, "pedestrian"
            "costing_options": costing_options,  # parameters for the costing model
            "directions_options": None
        }
        response = self._request(
            method="post",
            url=url,
            params=None,
            json=payload,
        )
        if response:
            return response.json()
        return {}

    @staticmethod
    def _split_track(track: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        # TODO: this whole function should be based on detecting pauses in movement
        #       so far I'm just splitting stuff at random
        track = track.copy()

        # search for splits in the trace bigger than 1s and label consecutive sections
        # TODO: automatically determine default interval; it can't be 1s for every track right?
        # NOTE: Smartphones really fuck up the interval, 5s seems to be save
        #       10s and more confuse the matcher and 2~3 are still to short to catch all outliers
        track["match_section"] = (track["timestamp"].diff() > pd.Timedelta(seconds=5)).cumsum()

        # get distance
        # we probably also do this somewhere else, but I really need it here and haven't cleaned up yet
        # TODO: clean up
        shift_track = track.shift(-1).drop("geometry", axis=1).rename(
            columns={"longitude": "longitude_2", "latitude": "latitude_2"})

        def dist(row: pd.Series) -> np.float64:
            return geopy.distance.geodesic(
                (row["latitude"], row["longitude"]), (row["latitude_2"], row["longitude_2"])
            ).meters

        track["distance"] = pd.concat([track, shift_track], axis=1)[
            ["longitude", "latitude", "longitude_2", "latitude_2"]
        ].fillna(method="ffill", axis=0).apply(dist, axis=1).fillna(np.float64(0))
        del shift_track

        # check if section we got so far are too long and split them up
        group_sections = []
        last_section_index = 0
        for index, group in track.groupby("match_section"):
            section_distance = group["distance"].sum()
            if section_distance > 10000:  # 20000 is the maximum
                new_sections = group["distance"].cumsum().apply(
                    lambda d: int(d // 5000) + last_section_index).values.tolist()
                group_sections.extend(new_sections)
                last_section_index = new_sections[-1]
            else:
                last_section_index += 1
                group_sections.extend([last_section_index] * len(group))

        track["match_section"] = group_sections
        return track

    def match(self, track: gpd.GeoDataFrame) -> (gpd.GeoDataFrame, None):
        # split track in sections small enough for matching
        track = self._split_track(track)

        traces = []
        edges = []
        edge_index_offset = 0
        for i, section in track.groupby(track["match_section"]):
            start_time = section["timestamp"].iloc[0]
            match = self._match_section(
                section[["longitude", "latitude"]].values.tolist(),
                section["timestamp"].apply(lambda t: (t - start_time).seconds).values.tolist()
            )

            if match.get("matched_points"):
                trace_df = self._load_trace(match["matched_points"], len(match["edges"]))
            else:
                # add empty rows to keep the overall length the same as the source
                data = [{
                    "lon": None,
                    "lat": None,
                    "type": None,
                    "edge_index": None,
                    "distance_along_edge": None,
                    "distance_from_trace_point": None
                }] * len(section)
                trace_df = gpd.GeoDataFrame(data)

            if match.get("edges"):
                match_shape = routingpy_utils.decode_polyline6(match["shape"])
                edges_df = self._load_edges(match["edges"], match_shape)
                try:
                    trace_df["edge_index"] = trace_df["edge_index"].apply(
                        lambda index: index + edge_index_offset if index is not None else None)
                except TypeError as e:
                    # TODO: logging instead of prints
                    print("### Exception")
                    print(match["matched_points"])
                    print(match["edges"])
                    print(trace_df["edge_index"])
                    print()
                    raise e

                edge_index_offset += len(edges_df)
                edges.append(edges_df)

            traces.append(trace_df)

        if not traces or not edges:
            return None

        trace_df = pd.concat(traces, axis=0).reset_index(drop=True)
        edges_df = pd.concat(edges, axis=0).reset_index(drop=True)
        return self._combine_data(track, trace_df, edges_df)
