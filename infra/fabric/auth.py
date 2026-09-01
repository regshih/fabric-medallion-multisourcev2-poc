"""Fabric REST API auth helper (AAD via az login / DefaultAzureCredential).

No secrets: a bearer token is fetched fresh from DefaultAzureCredential and
never persisted. Reused across all infra/fabric and infra/governance scripts.
"""
from __future__ import annotations

import requests
from azure.identity import DefaultAzureCredential

FABRIC_API = "https://api.fabric.microsoft.com/v1"
_SCOPE = "https://api.fabric.microsoft.com/.default"


class FabricSession(requests.Session):
    """A requests.Session that transparently refreshes its bearer token on 401.

    Fabric API tokens are short-lived; long-running provisioning scripts
    (Databricks/Cosmos seed waits, notebook job polling) can outlive a single
    token, so a plain static Authorization header isn't enough here.
    """

    def __init__(self) -> None:
        super().__init__()
        self._credential = DefaultAzureCredential()
        self._refresh_token()

    def _refresh_token(self) -> None:
        token = self._credential.get_token(_SCOPE).token
        self.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})

    def request(self, method, url, **kwargs):  # type: ignore[override]
        resp = super().request(method, url, **kwargs)
        if resp.status_code == 401:
            self._refresh_token()
            resp = super().request(method, url, **kwargs)
        return resp


def get_session() -> FabricSession:
    return FabricSession()
