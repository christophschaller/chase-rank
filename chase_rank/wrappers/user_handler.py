import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass
class StravaUser:
    user_id: int
    code: str = ""  # will be removed but is more convenient until we run in a webapp
    access_token: str = ""
    refresh_token: str = ""
    scope: list[str] = field(default_factory=list)


class StravaUserHandler:

    def __init__(self, path: Path):
        self.path = path
        self.users: dict[int, StravaUser] = {}
        self._load_users()

    def __getitem__(self, key: int) -> StravaUser:
        return self.get(key)

    def _load_users(self):
        with open(self.path) as file_pointer:
            content = json.load(file_pointer)

        for user_id, user_fields in content.items():
            self.users[int(user_id)] = StravaUser(**user_fields)

    def _save_users(self):
        content = {key: {
            "user_id": user.user_id,
            "code": user.code,
            "access_token": user.access_token,
            "refresh_token": user.refresh_token,
            "scope": user.scope
        }
            for key, user in self.users.items()
        }
        with open(self.path, "w") as file_pointer:
            json.dump(content, file_pointer, indent=2)

    def get(self, user_id: int) -> StravaUser:
        return self.users[user_id]

    def add(self, user_id: int, access_token: str, refresh_token: str, scope: List[str] = None):
        self.users[user_id] = StravaUser(
            user_id=user_id,
            access_token=access_token,
            refresh_token=refresh_token,
            scope=scope
        )

    def update(self, user_id: int, access_token: str, refresh_token: str):
        user = self.users[user_id]
        user.access_token = access_token
        user.refresh_token = refresh_token
