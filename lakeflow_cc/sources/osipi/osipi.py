# """
# OSIPI Lakeflow Community Connector (Python Data Source)
#
# Implements the LakeflowConnect interface expected by the Lakeflow Community Connectors template.
#
# This source reads from an OSI PI Web API-compatible endpoint (including the Databricks App mock server)
# and exposes multiple logical tables via the `tableName` option.
#
# Authentication (Databricks Apps)
# -------------------------------
# Databricks Apps commonly require an OAuth access token minted via the workspace OIDC endpoint:
#
#   POST https://<workspace-host>/oidc/v1/token
#     grant_type=client_credentials
#     scope=all-apis
#
# Options supported:
# - pi_base_url / pi_web_api_url (required): base URL, e.g. https://osipi-webserver-...aws.databricksapps.com
# - workspace_host: https://<workspace-host> (required for OIDC token mint)
# - client_id: service principal applicationId
# - client_secret: service principal secret
# - access_token: (optional) pre-minted bearer token
# - username/password: (optional) basic auth for non-App PI servers
#
# Table options (passed via table_configuration / table_options):
# - pi_points:
#   - dataserver_webid (optional)
#   - nameFilter (optional)
#   - maxCount (optional, default 10000)
# - pi_timeseries:
#   - tag_webids (optional csv); if missing will sample points (default_tags)
#   - default_tags (optional int, default 50)
#   - lookback_minutes (optional int, default 60) for first run
#   - maxCount (optional int, default 1000)
# - pi_event_frames:
#   - lookback_days (optional int, default 30) for first run
# """

from datetime import datetime, timedelta, timezone
from typing import Dict, Iterator, List, Optional, Tuple

import requests
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class LakeflowConnect:
    TABLE_DATASERVERS = "pi_dataservers"
    TABLE_POINTS = "pi_points"
    TABLE_TIMESERIES = "pi_timeseries"
    TABLE_AF_HIERARCHY = "pi_af_hierarchy"
    TABLE_EVENT_FRAMES = "pi_event_frames"

    def __init__(self, options: Dict[str, str]) -> None:
        self.options = options
        self.base_url = (options.get("pi_base_url") or options.get("pi_web_api_url") or "").rstrip("/")
        if not self.base_url:
            raise ValueError("Missing required option: pi_base_url (or pi_web_api_url)")

        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self._auth_resolved = False

    def list_tables(self) -> List[str]:
        return [
            self.TABLE_DATASERVERS,
            self.TABLE_POINTS,
            self.TABLE_TIMESERIES,
            self.TABLE_AF_HIERARCHY,
            self.TABLE_EVENT_FRAMES,
        ]

    def get_table_schema(self, table_name: str, table_options: Dict[str, str]) -> StructType:
        if table_name == self.TABLE_DATASERVERS:
            return StructType([
                StructField("webid", StringType(), False),
                StructField("name", StringType(), True),
            ])

        if table_name == self.TABLE_POINTS:
            return StructType([
                StructField("webid", StringType(), False),
                StructField("name", StringType(), True),
                StructField("descriptor", StringType(), True),
                StructField("engineering_units", StringType(), True),
                StructField("path", StringType(), True),
                StructField("dataserver_webid", StringType(), True),
            ])

        if table_name == self.TABLE_TIMESERIES:
            return StructType([
                StructField("tag_webid", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("value", DoubleType(), True),
                StructField("good", BooleanType(), True),
                StructField("units", StringType(), True),
                StructField("ingestion_timestamp", TimestampType(), False),
            ])

        if table_name == self.TABLE_AF_HIERARCHY:
            return StructType([
                StructField("element_webid", StringType(), False),
                StructField("name", StringType(), True),
                StructField("template_name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("path", StringType(), True),
                StructField("parent_webid", StringType(), True),
                StructField("depth", IntegerType(), True),
                StructField("category_names", ArrayType(StringType()), True),
                StructField("ingestion_timestamp", TimestampType(), False),
            ])

        if table_name == self.TABLE_EVENT_FRAMES:
            return StructType([
                StructField("event_frame_webid", StringType(), False),
                StructField("name", StringType(), True),
                StructField("template_name", StringType(), True),
                StructField("start_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True),
                StructField("primary_referenced_element_webid", StringType(), True),
                StructField("description", StringType(), True),
                StructField("category_names", ArrayType(StringType()), True),
                StructField("attributes", MapType(StringType(), StringType()), True),
                StructField("ingestion_timestamp", TimestampType(), False),
            ])

        raise ValueError(f"Unknown table: {table_name}")

    def read_table_metadata(self, table_name: str, table_options: Dict[str, str]) -> Dict:
        if table_name == self.TABLE_DATASERVERS:
            return {"primary_keys": ["webid"], "cursor_field": None, "ingestion_type": "snapshot"}
        if table_name == self.TABLE_POINTS:
            return {"primary_keys": ["webid"], "cursor_field": None, "ingestion_type": "snapshot"}
        if table_name == self.TABLE_TIMESERIES:
            return {"primary_keys": ["tag_webid", "timestamp"], "cursor_field": "timestamp", "ingestion_type": "append"}
        if table_name == self.TABLE_AF_HIERARCHY:
            return {"primary_keys": ["element_webid"], "cursor_field": None, "ingestion_type": "snapshot"}
        if table_name == self.TABLE_EVENT_FRAMES:
            return {"primary_keys": ["event_frame_webid", "start_time"], "cursor_field": "start_time", "ingestion_type": "append"}
        raise ValueError(f"Unknown table: {table_name}")

    def read_table(self, table_name: str, start_offset: dict, table_options: Dict[str, str]) -> Tuple[Iterator[dict], dict]:
        self._ensure_auth()

        if table_name == self.TABLE_DATASERVERS:
            return iter(self._read_dataservers()), {"offset": "done"}
        if table_name == self.TABLE_POINTS:
            return iter(self._read_points(table_options)), {"offset": "done"}
        if table_name == self.TABLE_TIMESERIES:
            return self._read_timeseries(start_offset, table_options)
        if table_name == self.TABLE_AF_HIERARCHY:
            return iter(self._read_af_hierarchy()), {"offset": "done"}
        if table_name == self.TABLE_EVENT_FRAMES:
            return self._read_event_frames(start_offset, table_options)

        raise ValueError(f"Unknown table: {table_name}")

    def _ensure_auth(self) -> None:
        if self._auth_resolved:
            return

        access_token = self.options.get("access_token")
        if access_token:
            self.session.headers.update({"Authorization": f"Bearer {access_token}"})
            self._auth_resolved = True
            return

        workspace_host = (self.options.get("workspace_host") or "").rstrip("/")
        client_id = self.options.get("client_id")
        client_secret = self.options.get("client_secret")
        if workspace_host and client_id and client_secret:
            if not workspace_host.startswith("http://") and not workspace_host.startswith("https://"):
                workspace_host = "https://" + workspace_host

            token_url = f"{workspace_host}/oidc/v1/token"
            resp = requests.post(
                token_url,
                data={"grant_type": "client_credentials", "scope": "all-apis"},
                auth=(client_id, client_secret),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30,
            )
            resp.raise_for_status()
            token = resp.json().get("access_token")
            if not token:
                raise RuntimeError("OIDC token endpoint did not return access_token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            self._auth_resolved = True
            return

        username = self.options.get("username")
        password = self.options.get("password")
        if username and password:
            self.session.auth = (username, password)
            self._auth_resolved = True
            return

        self._auth_resolved = True

    def _get_json(self, path: str, params: Optional[Dict] = None) -> dict:
        url = f"{self.base_url}{path}"
        r = self.session.get(url, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def _post_json(self, path: str, payload: dict) -> dict:
        url = f"{self.base_url}{path}"
        r = self.session.post(url, json=payload, timeout=120)
        r.raise_for_status()
        return r.json()

    def _read_dataservers(self) -> List[dict]:
        data = self._get_json("/piwebapi/dataservers")
        items = data.get("Items", [])
        return [{"webid": i.get("WebId"), "name": i.get("Name")} for i in items if i.get("WebId")]

    def _read_points(self, table_options: Dict[str, str]) -> List[dict]:
        dataservers = self._read_dataservers()
        if not dataservers:
            return []
        server_webid = table_options.get("dataserver_webid") or dataservers[0]["webid"]

        params: Dict[str, str] = {"maxCount": str(table_options.get("maxCount", 10000))}
        name_filter = table_options.get("nameFilter")
        if name_filter:
            params["nameFilter"] = name_filter

        data = self._get_json(f"/piwebapi/dataservers/{server_webid}/points", params=params)
        items = data.get("Items", [])
        out: List[dict] = []
        for p in items:
            out.append({
                "webid": p.get("WebId"),
                "name": p.get("Name"),
                "descriptor": p.get("Descriptor", ""),
                "engineering_units": p.get("EngineeringUnits", ""),
                "path": p.get("Path", ""),
                "dataserver_webid": server_webid,
            })
        return [r for r in out if r.get("webid")]

    def _read_timeseries(self, start_offset: dict, table_options: Dict[str, str]) -> Tuple[Iterator[dict], dict]:
        tag_webids_csv = table_options.get("tag_webids") or self.options.get("tag_webids") or ""
        tag_webids = [t.strip() for t in tag_webids_csv.split(",") if t.strip()]
        if not tag_webids:
            pts = self._read_points(table_options)
            tag_webids = [p["webid"] for p in pts[: int(table_options.get("default_tags", 50))]]

        now = _utcnow()

        start = None
        if start_offset and isinstance(start_offset, dict):
            off = start_offset.get("offset")
            if isinstance(off, str) and off:
                try:
                    start = _parse_ts(off)
                except Exception:
                    start = None
        if start is None:
            lookback_minutes = int(table_options.get("lookback_minutes", 60))
            start = now - timedelta(minutes=lookback_minutes)

        start_str = _isoformat_z(start)
        end_str = _isoformat_z(now)
        max_count = int(table_options.get("maxCount", 1000))

        requests_list = [
            {
                "Method": "GET",
                "Resource": f"/piwebapi/streams/{webid}/recorded",
                "Parameters": {"startTime": start_str, "endTime": end_str, "maxCount": str(max_count)},
            }
            for webid in tag_webids
        ]

        batch = self._post_json("/piwebapi/batch", {"Requests": requests_list})
        responses = batch.get("Responses", [])
        ingest_ts = _utcnow()

        def iterator() -> Iterator[dict]:
            for idx, resp in enumerate(responses):
                if resp.get("Status") != 200:
                    continue
                webid = tag_webids[idx] if idx < len(tag_webids) else None
                content = resp.get("Content", {})
                for item in content.get("Items", []):
                    ts = item.get("Timestamp")
                    if not ts or not webid:
                        continue
                    yield {
                        "tag_webid": webid,
                        "timestamp": _parse_ts(ts),
                        "value": float(item.get("Value")) if item.get("Value") is not None else None,
                        "good": bool(item.get("Good", True)),
                        "units": item.get("UnitsAbbreviation", ""),
                        "ingestion_timestamp": ingest_ts,
                    }

        return iterator(), {"offset": end_str}

    def _read_af_hierarchy(self) -> List[dict]:
        db_list = self._post_json("/piwebapi/assetdatabases/list", {})
        dbs = db_list.get("Items", [])
        if not dbs:
            return []

        out: List[dict] = []
        ingest_ts = _utcnow()

        def walk(elements: List[dict], parent_webid: str, depth: int):
            for e in elements:
                webid = e.get("WebId")
                if not webid:
                    continue
                out.append({
                    "element_webid": webid,
                    "name": e.get("Name", ""),
                    "template_name": e.get("TemplateName", ""),
                    "description": e.get("Description", ""),
                    "path": e.get("Path", ""),
                    "parent_webid": parent_webid or "",
                    "depth": depth,
                    "category_names": e.get("CategoryNames") or [],
                    "ingestion_timestamp": ingest_ts,
                })
                children = e.get("Elements") or []
                if children:
                    walk(children, webid, depth + 1)

        for db in dbs:
            db_webid = db.get("WebId")
            if not db_webid:
                continue
            roots = self._post_json("/piwebapi/assetdatabases/elements", {"db_webid": db_webid}).get("Items", [])
            walk(roots, parent_webid="", depth=0)

        return out

    def _read_event_frames(self, start_offset: dict, table_options: Dict[str, str]) -> Tuple[Iterator[dict], dict]:
        now = _utcnow()
        start = None
        if start_offset and isinstance(start_offset, dict):
            off = start_offset.get("offset")
            if isinstance(off, str) and off:
                try:
                    start = _parse_ts(off)
                except Exception:
                    start = None
        if start is None:
            lookback_days = int(table_options.get("lookback_days", 30))
            start = now - timedelta(days=lookback_days)

        start_str = _isoformat_z(start)
        end_str = _isoformat_z(now)

        db_list = self._post_json("/piwebapi/assetdatabases/list", {})
        dbs = db_list.get("Items", [])
        if not dbs:
            return iter(()), {"offset": end_str}

        all_events: List[dict] = []
        for db in dbs:
            db_webid = db.get("WebId")
            if not db_webid:
                continue
            resp = self._post_json(
                "/piwebapi/assetdatabases/eventframes",
                {"db_webid": db_webid, "startTime": start_str, "endTime": end_str, "searchMode": "Overlapped"},
            )
            all_events.extend(resp.get("Items", []))

        ingest_ts = _utcnow()

        def iterator() -> Iterator[dict]:
            for ef in all_events:
                webid = ef.get("WebId")
                if not webid:
                    continue
                raw_attrs = ef.get("Attributes") or {}
                attrs = {str(k): ("" if v is None else str(v)) for k, v in raw_attrs.items()}
                yield {
                    "event_frame_webid": webid,
                    "name": ef.get("Name", ""),
                    "template_name": ef.get("TemplateName", ""),
                    "start_time": _parse_ts(ef.get("StartTime")) if ef.get("StartTime") else None,
                    "end_time": _parse_ts(ef.get("EndTime")) if ef.get("EndTime") else None,
                    "primary_referenced_element_webid": ef.get("PrimaryReferencedElementWebId"),
                    "description": ef.get("Description", ""),
                    "category_names": ef.get("CategoryNames") or [],
                    "attributes": attrs,
                    "ingestion_timestamp": ingest_ts,
                }

        return iterator(), {"offset": end_str}
