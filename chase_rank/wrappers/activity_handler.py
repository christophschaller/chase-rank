from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
import geopandas as gpd
from routingpy import utils as routingpy_utils
from shapely.geometry import LineString

from .strava_handler import StravaHandler


class ActivityHandler:

    def __init__(self, activities_path: Path, strava_handler: StravaHandler):
        self.activities_path = activities_path
        self.activities: gpd.GeoDataFrame = gpd.GeoDataFrame({
            "strava_id": pd.Series(dtype="str"),  # should be int, but parquet can't handle that yet?
            "strava_name": pd.Series(dtype="str"),
            "user_id": pd.Series(dtype="str"),  # should be int, but parquet can't handle that yet?
            "distance": pd.Series(dtype="float"),
            "moving_time": pd.Series(dtype="float"),
            "elapsed_time": pd.Series(dtype="float"),
            "total_elevation_gain": pd.Series(dtype="float"),
            "sport_type": pd.Series(dtype="str"),
            "start_date": pd.Series(dtype="datetime64[ns]"),
            "timezone": pd.Series(dtype="str"),
            "start_lat": pd.Series(dtype="float"),
            "start_lng": pd.Series(dtype="float"),
            "end_lat": pd.Series(dtype="float"),
            "end_lng": pd.Series(dtype="float"),
            "average_speed": pd.Series(dtype="float"),
            "max_speed": pd.Series(dtype="float"),
            "elev_high": pd.Series(dtype="float"),
            "elev_low": pd.Series(dtype="float"),
            "external_id": pd.Series(dtype="str"),
        })
        self._load_activities()

        self.strava = strava_handler

    def __getitem__(self, key: int) -> gpd.GeoSeries:
        return self.get(key)

    @staticmethod
    def _parse_activity(activity: Dict) -> pd.Series:
        return pd.Series(
            {
                "strava_id": str(activity["id"]),  # should be int, but parquet can't handle that yet?
                "user_id": str(activity["athlete"]["id"]),  # should be int, but parquet can't handle that yet?
                "strava_name": activity["name"],
                "distance": activity.get("distance"),
                "moving_time": activity["moving_time"] if activity.get("moving_time") else None,
                "elapsed_time": activity["elapsed_time"] if activity.get("elapsed_time") else None,
                "total_elevation_gain": activity.get("total_elevation_gain"),
                "sport_type": activity["sport_type"],
                "start_date": datetime.strptime(activity["start_date"], "%Y-%m-%dT%H:%M:%SZ"),
                "timezone": activity["timezone"],
                "start_lat": activity["start_latlng"][0] if activity.get("start_latlng") else None,
                "start_lng": activity["start_latlng"][1] if activity.get("start_latlng") else None,
                "end_lat": activity["end_latlng"][0] if activity.get("end_latlng") else None,
                "end_lng": activity["end_latlng"][1] if activity.get("end_latlng") else None,
                "average_speed": activity.get("average_speed"),
                "max_speed": activity.get("max_speed"),
                "elev_high": activity.get("elev_high"),
                "elev_low": activity.get("elev_low"),
                "external_id": activity.get("external_id"),
                "private": bool(activity.get("private")),
                "trainer": bool(activity.get("trainer")),
                "manual": bool(activity.get("manual")),
                "commute": bool(activity.get("commute")),
                # "geometry": LineString(routingpy_utils.decode_polyline5(activity["map"]["summary_polyline"]))
            }
        )

    def _load_activities(self):
        if self.activities_path.exists():
            self.activities = gpd.read_parquet(self.activities_path)
        else:
            print("No stored activities found!")
            # TODO: log / error

    def _save_activities(self):
        self.activities.to_parquet(self.activities_path)

    def _add_activities(self, activities: List[Dict]):
        # filter for duplicates
        activities = [act for act in activities if act["id"] not in self.activities.index]
        # TODO: should converting the Dict to DF happen here?
        # probably won't be like this for ORM?
        new_activity = gpd.GeoDataFrame(
            index=[act["id"] for act in activities],
            data=[self._parse_activity(activity) for activity in activities],
            geometry=[
                LineString(routingpy_utils.decode_polyline5(act["map"]["summary_polyline"]))
                for act in activities
            ],
            crs="EPSG:4326"
        ).to_crs("EPSG:3857")
        self.activities = pd.concat([self.activities, new_activity])
        self._save_activities()

    def add(self, activities: (Dict, List[Dict])):
        if isinstance(activities, Dict):
            self._add_activities([activities])
        if isinstance(activities, List):
            self._add_activities(activities)

    def get(self, activity_id: int, user_id: int = None) -> gpd.GeoSeries:
        # check if we already have the track of the activity stored
        if activity_id in self.activities:
            return self.activities[activity_id]

        # try to fetch the activity from strava
        if not user_id:
            # TODO: proper logging
            raise KeyError
        activity = self.strava.get_activity_by_id(user_id=user_id, activity_id=activity_id)
        self.add(activity)
        return self.activities[activity_id]

    def get_user_activities(self,
                            user_id: int,
                            before: datetime = None,
                            after: datetime = None,
                            refresh: bool = False
                            ) -> gpd.GeoDataFrame:
        # fill empty time limits with default values
        before = before or datetime.now()
        after = after or datetime(year=2000, month=1, day=1)

        # get activities from strava
        if refresh:
            activities = self.strava.get_logged_in_athlete_activities(user_id=user_id, before=before, after=after)
            self.add(activities)

        return self.activities[
            (self.activities.user_id == str(user_id)) &  # user_id should be int, but parquet can't handle that yet?
            (after < self.activities.start_date) &
            (self.activities.start_date < before)
            ]
