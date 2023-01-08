import time
from datetime import datetime, timedelta
from typing import Dict, List

import requests

from .user_handler import StravaUserHandler


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
                 user_handler: StravaUserHandler,
                 ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_handler: StravaUserHandler = user_handler

        # these values will track the usage reported in the headers of api responses
        self.limit_daily = 1000
        self.limit_15_min = 100
        self.usage_daily = 0
        self.usage_15_min = 0

    def _auth(self, user_id: int):
        if self.user_handler[user_id].refresh_token:
            self._refresh_token(user_id)
        elif self.user_handler[user_id].code:
            self._request_token(user_id)
            self.user_handler[user_id].code = ""
            self.user_handler._save_users()
        else:
            print(
                f"AUTH ERROR: {user_id} has no auth code\ncode: {self.user_handler[user_id].code}")

        # if token expired:
        #     self._refresh_token()
        # elif no token or wrong scope:
        #     self._request_code()
        #     self._request_token()

    def _request_code(self, user_id: int):
        # TODO: call frontend for auth on strava
        # -> https://www.strava.com/oauth/authorize?client_id=99283&response_type=code&redirect_uri=http://localhost
        # /exchange_token&approval_prompt=force&scope=activity:read_all
        pass

    def _request_token(self, user_id: int):
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

    def _refresh_token(self, user_id: int):
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
        else:
            # TODO: log
            print("No Rate Limit In Headers")

        usage = response.headers.get("X-RateLimit-Usage")
        if usage:
            usage_15_min, usage_daily = usage.split(",")
            self.usage_15_min, self.usage_daily = int(usage_15_min), int(usage_daily)
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
                 user_id: int,
                 method: str,
                 url: str,
                 data: Dict = None,
                 params: Dict = None,
                 retries: int = 1
                 ):
        self._rate_limit()
        headers = {"Authorization": f"Bearer {self.user_handler[user_id].access_token}"}
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
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
                return None
            if response.status_code == 500:
                # Strava is having issues
                return None
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

    def get_activity_by_id(self, user_id: int, activity_id: int) -> Dict:
        # getActivityById
        # https://developers.strava.com/docs/reference/#api-Activities-getActivityById
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
        return response.json()

    def get_logged_in_athlete_activities(
            self, user_id: int, before: datetime = None, after: datetime = None) -> List[Dict]:
        # getLoggedInAthleteActivities:
        # https://developers.strava.com/docs/reference/#api-Activities-getLoggedInAthleteActivities
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

        return activities

    def get_activity_streams(self, user_id: int, activity_id: int, streams: List[str] = None) -> (Dict, None):
        # getActivityStreams
        # https://developers.strava.com/docs/reference/#api-Streams-getActivityStreams
        # for latlng, time, altitude
        # https://developers.strava.com/docs/reference/#api-models-LatLng
        # https://developers.strava.com/docs/reference/#api-models-AltitudeStream
        # https://developers.strava.com/docs/reference/#api-models-TimeStream
        url = f"https://www.strava.com/api/v3/activities/{activity_id}/streams"

        streams = streams or ["latlng", "altitude", "time"]
        stream_keys = ",".join(streams)
        response = self._request(
            user_id,
            method="get",
            url=url,
            params={
                # for some reason we can't just pass a list of keys
                # it needs to be a string of keys separated by ,
                "keys": stream_keys,
                "key_by_type": True
            }
        )
        if not response:
            return None
        return response.json()
