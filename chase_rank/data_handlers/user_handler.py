import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class User:
    user_id: str
    user_name: str
    code: str = ""
    access_token: str = ""
    refresh_token: str = ""
    code_scope: list[str] = field(default_factory=list)


class UserHandler:

    def __init__(self, path: Path):
        self.path = path
        self.users: dict[str, User] = {}
        self.load_users()

    def __getitem__(self, key):
        return self.users[key]

    def load_users(self):
        with open(self.path) as file_pointer:
            content = json.load(file_pointer)

        for user_id, user_fields in content.items():
            self.users[user_id] = User(**user_fields)

    def save_users(self):
        content = {key: {
            "user_id": user.user_id,
            "user_name": user.user_name,
            "code": user.code,
            "access_token": user.access_token,
            "refresh_token": user.refresh_token,
            "code_scope": user.code_scope
        }
            for key, user in self.users.items()
        }
        with open(self.path, "w") as file_pointer:
            json.dump(content, file_pointer, indent=2)

    def update_user(self, client_id: str, access_token: str, refresh_token: str):
        user = self.users[client_id]
        user.access_token = access_token
        user.refresh_token = refresh_token
