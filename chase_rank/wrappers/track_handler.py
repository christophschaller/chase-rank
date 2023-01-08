from datetime import datetime, timedelta
from pathlib import Path

import gpxpy.gpx
import geopandas as gpd
from shapely.geometry import Point

from .strava_handler import StravaHandler


class TrackHandler:

    def __init__(self, track_folder_path: Path, strava_handler: StravaHandler = None):
        self.track_folder_path = track_folder_path
        self.track_id_list = []
        self._load_track_id_list()

        self.strava = strava_handler

    def __getitem__(self, key: int) -> gpd.GeoDataFrame:
        return self.get(key)

    def _load_track_id_list(self):
        self.track_id_list = [int(file.stem)
                              for file in self.track_folder_path.iterdir()
                              if file.is_file() and file.suffix == ".parquet"]

    def _load_track(self, activity_id: int) -> gpd.GeoDataFrame:
        track_path = Path(self.track_folder_path, f"{activity_id}.parquet")
        return gpd.read_parquet(track_path)

    def _save_track_as_gpx(self, activity_id: int, track: gpd.GeoDataFrame):
        # will be removed
        # but having some gpx tracks for debugging is nice
        track_path = Path(self.track_folder_path, f"{activity_id}.gpx")

        segment = gpxpy.gpx.GPXTrackSegment()
        segment.points = [
            gpxpy.gpx.GPXTrackPoint(
                latitude=lat, longitude=long, elevation=alt, time=time)
            for lat, long, alt, time in
            track[["latitude", "longitude", "altitude", "timestamp"]].values.tolist()
        ]
        # TODO: find some better names that won't shadow everything else
        track_ = gpxpy.gpx.GPXTrack()
        track_.segments.append(segment)
        gpx_ = gpxpy.gpx.GPX()
        gpx_.tracks.append(track_)

        with open(track_path, "w") as file_pointer:
            file_pointer.write(gpx_.to_xml(prettyprint=True))

    def _save_track(self, activity_id: int, track: gpd.GeoDataFrame):
        track_path = Path(self.track_folder_path, f"{activity_id}.parquet")
        track.to_parquet(track_path)
        # self._save_track_as_gpx(activity_id, track)

    def add(self, activity_id: int, track: gpd.GeoDataFrame):
        self._save_track(activity_id, track)
        self.track_id_list.append(int(activity_id))

    def get(self, activity_id: int, user_id: int = None, start_time: datetime = None) -> gpd.GeoDataFrame:
        # check if we already have the track of the activity stored
        if activity_id in self.track_id_list:
            return self._load_track(activity_id)

        # try to fetch the activity from strava
        if not user_id or not start_time or not self.strava:
            # TODO: proper logging
            raise KeyError
        streams = self.strava.get_activity_streams(
            user_id=user_id, activity_id=activity_id, streams=["latlng", "altitude", "time"])
        latlng_stream = streams.get("latlng")
        alt_stream = streams.get("altitude")
        time_stream = streams.get("time")
        if not all((latlng_stream, alt_stream, time_stream)):
            # TODO: proper Error
            print(
                f"Can't Build Track\nlatlng_stream: {bool(latlng_stream)}\nalt_stream: "
                f"{bool(alt_stream)}\ntime_stream: {bool(time_stream)}")
            # TODO: proper logging
            raise KeyError

        lat_stream, lng_stream = zip(*streams["latlng"]["data"])
        track = gpd.GeoDataFrame(
            data={
                "latitude": lat_stream,
                "longitude": lng_stream,
                "altitude": alt_stream["data"],
                "timestamp": [start_time + timedelta(seconds=seconds)
                              for seconds in time_stream["data"]]
            },
            # x is longitude, y is latitude
            geometry=[Point(lng, lat) for lat, lng in streams["latlng"]["data"]],
            crs="EPSG:4326"
        ).to_crs("EPSG:3857")

        self.add(activity_id, track)
        return self._load_track(activity_id)
