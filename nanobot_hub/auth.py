"""Bootstrap admin authentication for nanobot-community-hub."""

from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from nanobot_hub.store import _utc_now, hub_admin_users


PBKDF2_ITERATIONS = 120_000


@dataclass(slots=True)
class HubAdminUser:
    """Authenticated hub administrator."""

    id: int
    username: str
    email: str

    @property
    def label(self) -> str:
        return self.username


class HubAuthService:
    """Manage the single bootstrap admin for the hub."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def has_admin(self) -> bool:
        with self.engine.connect() as conn:
            row = conn.execute(select(hub_admin_users.c.id).limit(1)).first()
        return row is not None

    def create_admin(self, username: str, email: str, password: str) -> HubAdminUser:
        if self.has_admin():
            raise ValueError("An admin account already exists.")

        normalized_username = username.strip()
        normalized_email = email.strip().lower()
        if not normalized_username or not normalized_email or not password:
            raise ValueError("Username, email, and password are required.")

        password_hash = _hash_password(password)
        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    hub_admin_users.insert().values(
                        username=normalized_username,
                        email=normalized_email,
                        password_hash=password_hash,
                        created_at=_utc_now(),
                    )
                )
                admin_id = result.inserted_primary_key[0] if result.inserted_primary_key else None
        except IntegrityError as exc:
            raise ValueError("That username or email is already in use.") from exc

        if admin_id is None:
            raise ValueError("Failed to create admin account.")
        return HubAdminUser(id=int(admin_id), username=normalized_username, email=normalized_email)

    def authenticate(self, identifier: str, password: str) -> HubAdminUser | None:
        if not identifier or not password:
            return None
        normalized_identifier = identifier.strip()
        with self.engine.connect() as conn:
            row = conn.execute(
                select(
                    hub_admin_users.c.id,
                    hub_admin_users.c.username,
                    hub_admin_users.c.email,
                    hub_admin_users.c.password_hash,
                ).where(
                    (hub_admin_users.c.username == normalized_identifier)
                    | (hub_admin_users.c.email == normalized_identifier.lower())
                )
            ).mappings().first()
        if row is None or not _verify_password(password, str(row["password_hash"])):
            return None
        return HubAdminUser(
            id=int(row["id"]),
            username=str(row["username"]),
            email=str(row["email"]),
        )

    def get_admin(self, admin_id: int | None) -> HubAdminUser | None:
        if admin_id is None:
            return None
        with self.engine.connect() as conn:
            row = conn.execute(
                select(
                    hub_admin_users.c.id,
                    hub_admin_users.c.username,
                    hub_admin_users.c.email,
                ).where(hub_admin_users.c.id == admin_id)
            ).mappings().first()
        if row is None:
            return None
        return HubAdminUser(
            id=int(row["id"]),
            username=str(row["username"]),
            email=str(row["email"]),
        )


def _hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    derived = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PBKDF2_ITERATIONS,
    )
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt.hex()}${derived.hex()}"


def _verify_password(password: str, encoded: str) -> bool:
    try:
        _, iterations, salt_hex, hash_hex = encoded.split("$", 3)
    except ValueError:
        return False
    derived = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        bytes.fromhex(salt_hex),
        int(iterations),
    )
    return hmac.compare_digest(derived.hex(), hash_hex)
