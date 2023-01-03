import time
from datetime import datetime, timedelta
from typing import Dict

import requests
import geopandas as gpd
from shapely.geometry import Point

from ..data_handlers import ActivityHandler, UserHandler, TrackHandler


def sleep_until_next_quarter():
    # TODO: come up with better solution with some safety checks
    now = datetime.now()
    # get timedelta to the next full 15 minutes
    delta = timedelta(minutes=15 - now.minute % 15, seconds=-now.second)
    # add a safety second and sleep
    time.sleep(delta.seconds + 1)


class StravaHandler:

    def __init__(self,
                 client_id: str,
                 client_secret: str,
                 user_handler: UserHandler,
                 activity_handler: ActivityHandler,
                 track_handler: TrackHandler
                 ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_handler: UserHandler = user_handler
        self.activity_handler: ActivityHandler = activity_handler
        self.track_handler: TrackHandler = track_handler

        # these values will track the usage reported in the headers of api responses
        self.limit_daily = 1000
        self.limit_15_min = 100
        self.usage_daily = 0
        self.usage_15_min = 0

    def _auth(self, user_id: str):
        if self.user_handler[user_id].refresh_token:
            self._refresh_token(user_id)
        elif self.user_handler[user_id].code:
            self._request_token(user_id)
            self.user_handler[user_id].code = ""
            self.user_handler.save_users()
        else:
            print(
                f"AUTH ERROR: {user_id} has no auth code\ncode: {self.user_handler[user_id].code}")

        # if token expired:
        #     self._refresh_token()
        # elif no token or wrong scope:
        #     self._request_code()
        #     self._request_token()

    def _request_code(self, user_id: str):
        # TODO: call frontend for auth on strava
        # -> https://www.strava.com/oauth/authorize?client_id=99283&response_type=code&redirect_uri=http://localhost
        # /exchange_token&approval_prompt=force&scope=activity:read_all
        pass

    def _request_token(self, user_id: str):
        auth_url = "https://www.strava.com/oauth/token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": self.user_handler[user_id].code,
            "grant_type": "authorization_code",
        }
        response = self._request(
            user_id=user_id,
            method="post",
            url=auth_url,
            data=payload
        )
        self.user_handler[user_id].access_token = response.json()["access_token"]
        self.user_handler[user_id].refresh_token = response.json()["refresh_token"]

    def _refresh_token(self, user_id: str):
        auth_url = "https://www.strava.com/oauth/token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.user_handler[user_id].refresh_token,
            "grant_type": "refresh_token",
        }
        # TODO: add error handling with verbose information on what happened
        response = self._request(
            user_id=user_id,
            method="post",
            url=auth_url,
            data=payload
        )
        self.user_handler[user_id].access_token = response.json()["access_token"]
        self.user_handler[user_id].refresh_token = response.json()["refresh_token"]

    def _track_rate_limit(self, response: requests.Response):
        limit = response.headers.get("X-RateLimit-Limit")
        if limit:
            limit_15_min, limit_daily = limit.split(",")
            self.limit_15_min, self.limit_daily = int(limit_15_min), int(limit_daily)
            # print(f"limit_daily: {limit_daily}\nlimit_15_min: {limit_15_min}")
            # debuggery
        else:
            # TODO: log
            print("No Rate Limit In Headers")

        usage = response.headers.get("X-RateLimit-Usage")
        if usage:
            usage_15_min, usage_daily = usage.split(",")
            self.usage_15_min, self.usage_daily = int(usage_15_min), int(usage_daily)
            # print(f"usage_daily: {usage_daily}\nusage_15_min: {usage_15_min}")
            # debuggery
        else:
            # TODO: log
            print("No Usage In Headers")

    def _rate_limit(self):
        if self.usage_daily >= self.limit_daily:
            # TODO: implement proper Exceptions
            raise Exception("Daily Rate Limit Exceeded")

        if self.usage_15_min >= self.limit_15_min:
            # TODO: logging
            print("15min Rate Limit Exceeded")
            sleep_until_next_quarter()
            self.usage_15_min = 0

    def _request(self,
                 user_id: str,
                 method: str,
                 url: str,
                 data: Dict = None,
                 params: Dict = None,
                 retries: int = 1
                 ):
        self._rate_limit()
        header = {"Authorization": f"Bearer {self.user_handler[user_id].access_token}"}
        response = requests.request(
            method=method,
            url=url,
            headers=header,
            data=data,
            params=params
        )
        self._track_rate_limit(response)

        if not response.ok:
            if response.status_code == 401:
                # 401 Unauthorized
                self._auth(user_id)
                # try again if we have retries left
                if retries:
                    return self._request(
                        user_id=user_id,
                        method=method,
                        url=url,
                        data=data,
                        params=params,
                        retries=retries - 1
                    )
            if response.status_code == 403:
                # Forbidden; you cannot access
                return None
            if response.status_code == 404:
                # Not found; the requested asset does not exist, or you are not authorized to see it
                return None
            if response.status_code == 429:
                # Too Many Requests; you have exceeded rate limits
                pass
            if response.status_code == 500:
                # Strava is having issues
                pass
            # TODO: richtiges logging wäre was feines
            print()
            print(f"request: {method} - {url}"
                  f"\nrequest payload: {data}")
            print("response:", response)
            print("response usage:", response.headers.get("X-RateLimit-Usage"))
            # TODO: richtig schlechter stil
            try:
                print("response content:", response.json())
            except:
                print(response.content)

        if response.ok:
            return response

    def activity(self, user_id: str, activity_id: str):
        if activity_id in self.activity_handler.activities.index:
            return self.activity_handler[activity_id]

        activity_url = f"https://www.strava.com/api/v3/activities/{activity_id}"
        params = {
            "include_all_efforts": False  # we don't use those
        }
        response = self._request(
            user_id=user_id,
            method="get",
            url=activity_url,
            data=None,
            params=params
        )
        self.activity_handler.add_activity(response.json())
        return self.activity_handler[activity_id]

    def activities(self, user_id: str, before: datetime = None, after: datetime = None):
        activities_url = "https://www.strava.com/api/v3/athlete/activities"
        params = {
            "per_page": "200",  # defaults to 30
            "page": 1,  # defaults to 1
            "before": int(before.timestamp()) if before else None,
            "after": int(after.timestamp()) if after else None,
        }

        activities = []
        current_page = 1
        # run request new pages until we get less activities back than we requested
        while True:
            params["page"] = current_page
            response = self._request(
                user_id=user_id,
                method="get",
                url=activities_url,
                data=None,
                params=params
            )
            content = response.json()
            activities.extend(content)

            # if content len is smaller than the amount of activities we requested per
            # page we should have gotten everything available and can stop requesting
            if len(content) < int(params["per_page"]):
                break
            # increase page number
            current_page += 1
            params["page"] = current_page

        self.activity_handler.add_activities(activities)

        return self.activity_handler.activities[
            (self.activity_handler.activities.user == user_id) &
            (after < self.activity_handler.activities.start_date) &
            (self.activity_handler.activities.start_date < before)
            ]

    def activity_track(self, user_id: str, activity_id: str):
        if activity_id in self.track_handler.tracklist:
            return self.track_handler[activity_id]

        url = f"https://www.strava.com/api/v3/activities/{activity_id}/streams"

        if activity_id in self.activity_handler.activities.index:
            activity = self.activity_handler[activity_id]
        else:
            activity = self.activity(user_id=user_id, activity_id=activity_id)
        start_time = activity["start_date"]

        response = self._request(
            user_id,
            method="get",
            url=url,
            params={
                # for some reason we can't just pass a list of keys
                # it needs to be a string of keys separated by ,
                "keys": "latlng,time,altitude",
                "key_by_type": True
            }
        )
        if not response:
            return None
        content = response.json()

        latlng_stream = content.get("latlng")
        alt_stream = content.get("altitude")
        time_stream = content.get("time")
        if not all((latlng_stream, alt_stream, time_stream)):
            # TODO: proper Error
            print(
                f"Can't Build Track\nlatlng_stream: {bool(latlng_stream)}\nalt_stream: "
                f"{bool(alt_stream)}\ntime_stream: {bool(time_stream)}")
            return None

        lat_stream, lng_stream = zip(*content["latlng"]["data"])
        track = gpd.GeoDataFrame(
            data={
                "latitude": lat_stream,
                "longitude": lng_stream,
                "altitude": alt_stream["data"],
                "timestamp": [start_time + timedelta(seconds=seconds)
                              for seconds in time_stream["data"]]
            },
            geometry=[Point(lat, lng) for lat, lng in content["latlng"]["data"]],
            crs="EPSG:4326"
        ).to_crs("EPSG:3857")
        self.track_handler.add_track(activity_id, track)
        return track
