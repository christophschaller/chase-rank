from pathlib import Path

import gpxpy.gpx
import geopandas as gpd


class TrackHandler:

    def __init__(self, path: Path):
        self.path = path
        self.tracklist = []
        self._load_tracklist()

    def __getitem__(self, key):
        if key in self.tracklist:
            return self.load_track(key)
        else:
            raise KeyError

    def _load_tracklist(self):
        self.tracklist = [int(file.stem)
                          for file in self.path.iterdir()
                          if file.is_file() and file.suffix == ".parquet"]

    def load_track(self, activity_id: str):
        track_path = Path(self.path, f"{activity_id}.parquet")
        return gpd.read_parquet(track_path)

    def save_track_as_gpx(self, activity_id: str, track: gpd.GeoDataFrame):
        track_path = Path(self.path, f"{activity_id}.gpx")

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

    def save_track(self, activity_id: str, track: gpd.GeoDataFrame):
        track_path = Path(self.path, f"{activity_id}.parquet")
        track.to_parquet(track_path)
        self.save_track_as_gpx(activity_id, track)

    def add_track(self, activity_id: str, track: gpd.GeoDataFrame):
        self.save_track(activity_id, track)
        self.tracklist.append(int(activity_id))
