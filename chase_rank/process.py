import json
from pathlib import Path
from datetime import timedelta

import geopy.distance
import numpy as np
import pandas as pd
import geopandas as gpd

import collections
import time

# TODO: könnte schon etwas dynamischer sein
DATA_PATH = Path("../data")
TEST_TRACK_PATH = Path(DATA_PATH, "routes/test_track.gpx")


def combine_data(gpx_df, trace_df, edges_df):
    trace_data_df = trace_df.apply(
        lambda row: edges_df.iloc[row["edge_index"]]
        if isinstance(row["edge_index"], int) else None, axis=1)[
        ["length", "speed", "use", "unpaved", "surface", "travel_mode", "osm_way_id",
         "geometry"]]
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


def get_section_analytics(gpx_frame: gpd.GeoDataFrame) -> pd.DataFrame:
    last_time = None
    section_dict = {
        "section": [],
        "paused_time": [],
        "total_asc": [],
        "total_desc": [],
        "total_distance": [],
        "arg_kph": [],
        "distance_between_stops": []
    }

    # Pause TIME
    for index, group in gpx_frame.groupby(gpx_frame["section"]):
        paused_time = group.iloc[0]["time"] - last_time if last_time else timedelta(days=0)
        last_time = group.iloc[-1]["time"]

        # print(index,paused_time)
        section_dict["section"].append(index)
        section_dict["paused_time"].append(paused_time)

        # Asc Desc
        total_asc = 0
        total_desc = 0
        for i in range(0, len(group) - 1):
            a = i + 1
            diff = group.iloc[a]["elev"] - group.iloc[i]["elev"]

            if diff >= 0:
                total_asc = total_asc + diff
            elif diff <= 0:
                total_desc = total_desc + diff

        section_dict["total_asc"].append(round(total_asc, 2))
        section_dict["total_desc"].append(round(total_desc, 2))

        # Km and Kph
        distance = group["distance"].sum()

        avrg_kph = distance / len(group) * 3.6
        section_dict["total_distance"].append(round(distance, 2))
        section_dict["arg_kph"].append(round(avrg_kph, 2))

        # Distance between Stops
        section_dict["distance_between_stops"].append(group.iloc[-1]["distance"])

    section_df = pd.DataFrame(data=section_dict)
    return section_df

def get_duration(points_df):
    time_as_int = len(points_df)
    time_as_time = time.strftime('%H:%M:%S', time.gmtime(time_as_int))
    return time_as_time

# TODO: Paused Time ist noch nicht korrekt. Diff zu SUM(Section Paused time), ebenso die Duration
def get_pause_duration(points_df):
    points_df['coordinates'] = points_df.apply(lambda row: (row['latitude'], row['longitude']), axis=1)
    driving_time = points_df['coordinates'].nunique()
    time_as_int = len(points_df)-driving_time
    time_as_time = time.strftime('%H:%M:%S', time.gmtime(time_as_int))
    return time_as_time

def get_elevation_info(points_df):
    diffs = points_df["elev"].diff()
    total_asc = diffs[diffs > 0].sum()
    total_desc = diffs[diffs < 0].sum()
    return total_asc, total_desc

def checkKey(dic, key):
    if key in dic.keys():
        result = dic[key]
    else:
        result = 0
    return result


# TODO: mach so als funktion auch keinen Sinn
def save_match(points_df, section_analytics_df):
    PROCESSED_TRACKS_PATH = Path(DATA_PATH, "processed")

    source_track_name = TEST_TRACK_PATH.stem
    start_date = points_df["time"][0].isoformat().replace(":", "-")
    distance = section_analytics_df["total_distance"].sum() - section_analytics_df["distance_between_stops"].sum()

    folder_path = Path(PROCESSED_TRACKS_PATH, f"{start_date}_{source_track_name}")
    folder_path.mkdir(parents=True, exist_ok=True)

    track_info = {
        "track_name": source_track_name,
        "date": start_date,
        # store distance without taking travel during pauses into account
        "distance": distance,
        "surfaces": surface_stats(points_df),
        "total_ascend":get_elevation_info(points_df)[0],
        "total_descend":get_elevation_info(points_df)[1],
        "duration": get_duration(points_df),
        "paused_time": get_pause_duration(points_df),
        'paved_smooth': checkKey(surface_stats(points_df),'paved_smooth'),
        'compacted': checkKey(surface_stats(points_df),'compacted'),
        'path': checkKey(surface_stats(points_df),'path'),
        'paved': checkKey(surface_stats(points_df),'paved'),
        'paved_rough': checkKey(surface_stats(points_df),'paved_rought'),
        'gravel': checkKey(surface_stats(points_df),'gravel'),
        'dirt': checkKey(surface_stats(points_df),'dirt'),
        'null': checkKey(surface_stats(points_df),'null'),
        #"average_kph": 
    }

    with open(Path(folder_path, "track_info.json"), "w") as file_pointer:
        json.dump(track_info, file_pointer)

    points_df.to_csv(Path(folder_path, "points.csv"))
    section_analytics_df.to_csv(Path(folder_path, "sections.csv"))


def surface_stats(gpx_frame: gpd.GeoDataFrame) -> dict:
    surfaces = collections.defaultdict(lambda: 0.)# Distance(kilometers=0))
    for index, surface_group in gpx_frame.groupby("surface_section"):
        surface = surface_group["surface"].values[0]
        # distance = group["distance"].dropna(axis=0).sum()
        distance = surface_group["distance"].sum()
        surfaces[surface] += distance

    # remove distances travelled during pauses
    for index, section_group in gpx_frame.groupby("section"):
        surface = section_group["surface"].values[-1]
        surfaces[surface] -= section_group["distance"].values[-1]

    return dict(surfaces)


def process_match(gpx_df, trace_df, edges_df):
    points_df = combine_data(gpx_df, trace_df, edges_df)
    sections_df = get_section_analytics(points_df)

    save_match(points_df, sections_df)
