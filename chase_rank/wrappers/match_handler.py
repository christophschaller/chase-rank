from pathlib import Path

import geopandas as gpd


class MatchHandler:

    def __init__(self, path: Path):
        self.path = path
        self.match_id_list = []
        self._load_match_ids()

    def __getitem__(self, key: int) -> gpd.GeoDataFrame:
        return self.get(key)

    def _load_match_ids(self):
        self.match_id_list = [int(file.stem)
                              for file in self.path.iterdir()
                              if file.is_file() and file.suffix == ".parquet"]

    def _load_match(self, activity_id: int) -> gpd.GeoDataFrame:
        match_path = Path(self.path, f"{activity_id}.parquet")
        return gpd.read_parquet(match_path)

    def _save_match(self, activity_id: int, match: gpd.GeoDataFrame):
        match_path = Path(self.path, f"{activity_id}.parquet")
        match.to_parquet(match_path)

    def add(self, activity_id: int, track: gpd.GeoDataFrame):
        self._save_match(activity_id, track)
        self.match_id_list.append(activity_id)

    def get(self, activity_id: int) -> gpd.GeoDataFrame:
        if activity_id in self.match_id_list:
            return self._load_match(activity_id)
        else:
            raise KeyError
