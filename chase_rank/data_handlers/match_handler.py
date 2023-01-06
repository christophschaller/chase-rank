from pathlib import Path

import geopandas as gpd


class MatchHandler:

    def __init__(self, path: Path):
        self.path = path
        self.match_ids = []
        self._load_match_ids()

    def __getitem__(self, key):
        if key in self.match_ids:
            return self.load_match(key)
        else:
            raise KeyError

    def _load_match_ids(self):
        self.match_ids = [int(file.stem)
                          for file in self.path.iterdir()
                          if file.is_file() and file.suffix == ".parquet"]

    def load_match(self, activity_id: str):
        match_path = Path(self.path, f"{activity_id}.parquet")
        return gpd.read_parquet(match_path)

    def save_match(self, activity_id: str, match: gpd.GeoDataFrame):
        match_path = Path(self.path, f"{activity_id}.parquet")
        match.to_parquet(match_path)

    def add_match(self, activity_id: str, track: gpd.GeoDataFrame):
        self.save_match(activity_id, track)
        self.match_ids.append(int(activity_id))
