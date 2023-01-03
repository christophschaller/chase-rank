from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
import geopandas as gpd
from routingpy import utils as routingpy_utils
from shapely.geometry import LineString


class ActivityHandler:

    def __init__(self, path: Path = None):
        self.path = path
        self.activities: gpd.GeoDataFrame = gpd.GeoDataFrame()
        self.load_activities()

    def __getitem__(self, key):
        return self.activities.loc[key]

    def load_activities(self):
        if self.path.exists():
            self.activities = gpd.read_parquet(self.path)
        else:
            print("No stored activities found!")
            # TODO: log / error

    def save_activities(self):
        self.activities.to_parquet(self.path)

    def add_activity(self, activity: Dict, save: bool = True):
        self.add_activities([activity])
        if save:
            self.save_activities()

    def add_activities(self, activities: List[Dict], save: bool = True):
        # TODO: should converting the Dict to DF happen here?
        # probably won't be like this for ORM?
        new_activity = gpd.GeoDataFrame(
            index=[act["id"] for act in activities],
            data=[{
                "strava_id": activity["id"],
                "strava_name": activity["name"],
                "user": activity["athlete"]["id"],
                "distance": activity.get("distance"),
                "moving_time": activity.get("moving_time"),
                "elapsed_time": activity["elapsed_time"],
                "total_elevation_gain": activity.get("total_elevation_gain"),
                "sport_type": activity["sport_type"],
                "start_date": datetime.strptime(
                    activity["start_date"], "%Y-%m-%dT%H:%M:%SZ"),
                "timezone": activity["timezone"],
                "start_lat": activity["start_latlng"][0] if activity.get(
                    "start_latlng") else None,
                "start_lng": activity["start_latlng"][1] if activity.get(
                    "start_latlng") else None,
                "end_lat": activity["end_latlng"][0] if activity.get(
                    "end_latlng") else None,
                "end_lng": activity["end_latlng"][1] if activity.get(
                    "end_latlng") else None,
                "average_speed": activity.get("average_speed"),
                "max_speed": activity.get("max_speed"),
                "elev_high": activity.get("elev_high"),
                "elev_low": activity.get("elev_low"),
                "external_id": activity.get("external_id"),
            } for activity in activities],
            geometry=[
                LineString(
                    routingpy_utils.decode_polyline5(act["map"]["summary_polyline"]))
                for act in activities
            ],
            crs="EPSG:4326"
        ).to_crs("EPSG:3857")
        self.activities = pd.concat([self.activities, new_activity])

        if save:
            self.save_activities()
