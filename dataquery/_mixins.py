"""
Mixins composed into ``DataQueryClient``.

Each mixin owns one concern. The concrete client inherits from all of them so
the public surface (``client.list_instruments_async`` etc.) stays unchanged
while ``client.py`` itself only carries HTTP plumbing, auth, and downloads.

Mixins fall into two groups:

- **Read-only query mixins** (``InstrumentsMixin``, ``MetadataMixin``,
  ``TimeSeriesMixin``, ``GridMixin``) — depend on three private methods of
  the concrete client: ``_build_api_url``, ``_enter_request_cm``,
  ``_handle_response``. The ``_RequestProto`` base provides typed stubs so
  ``mypy`` is happy.

- **Pure transformation** (``DataFrameMixin``) — no HTTP at all; converts API
  response objects to ``pandas.DataFrame``. Only relies on ``self.logger``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

import aiohttp

from . import _constants as C
from .models import (
    AttributesResponse,
    FiltersResponse,
    GridDataResponse,
    InstrumentsResponse,
    TimeSeriesResponse,
)
from .utils import (
    validate_attributes_list,
    validate_date_format,
    validate_instruments_list,
)

try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    pd = None  # type: ignore[assignment]
    HAS_PANDAS = False

if TYPE_CHECKING:
    import structlog

    from .models import FileInfo, FileList, Group, GroupList


__all__ = [
    "DataFrameMixin",
    "GridMixin",
    "InstrumentsMixin",
    "MetadataMixin",
    "TimeSeriesMixin",
]


# ---------------------------------------------------------------------------
# Typing shim — methods provided by the concrete client
# ---------------------------------------------------------------------------


class _RequestProto:
    """Static typing shim for HTTP helpers the query mixins call on ``self``.

    The concrete ``DataQueryClient`` provides real implementations; the stubs
    here exist only so ``mypy`` can resolve attribute access inside the mixin
    method bodies.
    """

    def _build_api_url(self, endpoint: str) -> str:  # pragma: no cover - stub
        raise NotImplementedError

    async def _enter_request_cm(  # pragma: no cover - stub
        self, method: str, url: str, **kwargs: object
    ) -> aiohttp.ClientResponse:
        raise NotImplementedError

    async def _handle_response(  # pragma: no cover - stub
        self, response: aiohttp.ClientResponse
    ) -> None:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Read-only query mixins
# ---------------------------------------------------------------------------


class InstrumentsMixin(_RequestProto):
    """Instrument discovery and keyword search."""

    async def list_instruments_async(
        self,
        group_id: str,
        instrument_id: Optional[str] = None,
        page: Optional[str] = None,
    ) -> "InstrumentsResponse":
        """Return the complete list of instruments for a dataset."""
        params: dict = {"group-id": group_id}
        if instrument_id:
            params["instrument-id"] = instrument_id
        if page:
            params["page"] = page

        url = self._build_api_url(C.API_GROUP_INSTRUMENTS)
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            data = await response.json()
            return InstrumentsResponse(**data)

    async def search_instruments_async(
        self,
        group_id: str,
        keywords: str,
        page: Optional[str] = None,
    ) -> "InstrumentsResponse":
        """Keyword-search instruments within a dataset."""
        params: dict = {"group-id": group_id, "keywords": keywords}
        if page:
            params["page"] = page

        url = self._build_api_url(C.API_GROUP_INSTRUMENTS_SEARCH)
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            data = await response.json()
            return InstrumentsResponse(**data)


class MetadataMixin(_RequestProto):
    """Group filters and attributes (metadata describing a dataset)."""

    async def get_group_filters_async(self, group_id: str, page: Optional[str] = None) -> "FiltersResponse":
        """Return the filter dimensions available for a dataset."""
        params: dict = {"group-id": group_id}
        if page:
            params["page"] = page

        url = self._build_api_url(C.API_GROUP_FILTERS)
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            payload = await response.json()
            return FiltersResponse(**payload)

    async def get_group_attributes_async(
        self,
        group_id: str,
        instrument_id: Optional[str] = None,
        page: Optional[str] = None,
    ) -> "AttributesResponse":
        """Return analytic attributes for each instrument of a dataset."""
        params: dict = {"group-id": group_id}
        if instrument_id:
            params["instrument-id"] = instrument_id
        if page:
            params["page"] = page

        url = self._build_api_url(C.API_GROUP_ATTRIBUTES)
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            payload = await response.json()
            return AttributesResponse(**payload)


class TimeSeriesMixin(_RequestProto):
    """Time-series retrieval by instrument, expression, or group."""

    async def get_instrument_time_series_async(
        self,
        instruments: List[str],
        attributes: List[str],
        data: str = "REFERENCE_DATA",
        format: str = "JSON",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        calendar: str = "CAL_USBANK",
        frequency: str = "FREQ_DAY",
        conversion: str = "CONV_LASTBUS_ABS",
        nan_treatment: str = "NA_NOTHING",
        page: Optional[str] = None,
    ) -> "TimeSeriesResponse":
        """Time series for explicit instrument + attribute identifiers."""
        validate_instruments_list(instruments)
        validate_attributes_list(attributes)
        if start_date is not None:
            validate_date_format(start_date, "start-date")
        if end_date is not None:
            validate_date_format(end_date, "end-date")

        params: dict = {
            "instruments": instruments,
            "attributes": attributes,
            "data": data,
            "format": format,
            "calendar": calendar,
            "frequency": frequency,
            "conversion": conversion,
            "nan-treatment": nan_treatment,
        }
        if start_date is not None:
            params["start-date"] = start_date
        if end_date is not None:
            params["end-date"] = end_date
        if page is not None:
            params["page"] = page

        url = self._build_api_url(C.API_INSTRUMENTS_TIME_SERIES)
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            payload = await response.json()
            return TimeSeriesResponse(**payload)

    async def get_expressions_time_series_async(
        self,
        expressions: List[str],
        format: str = "JSON",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        calendar: str = "CAL_USBANK",
        frequency: str = "FREQ_DAY",
        conversion: str = "CONV_LASTBUS_ABS",
        nan_treatment: str = "NA_NOTHING",
        data: str = "REFERENCE_DATA",
        page: Optional[str] = None,
    ) -> "TimeSeriesResponse":
        """Time series for a list of traditional DataQuery expressions."""
        if not expressions or not isinstance(expressions, list):
            raise ValueError("'expressions' must be a non-empty list")
        for expr in expressions:
            if not isinstance(expr, str) or not expr.strip():
                raise ValueError("All expressions must be non-empty strings")
        if start_date is not None:
            validate_date_format(start_date, "start-date")
        if end_date is not None:
            validate_date_format(end_date, "end-date")

        params: dict = {
            "expressions": ",".join(expressions),
            "format": format,
            "calendar": calendar,
            "frequency": frequency,
            "conversion": conversion,
            "nan-treatment": nan_treatment,
            "data": data,
        }
        if start_date is not None:
            params["start-date"] = start_date
        if end_date is not None:
            params["end-date"] = end_date
        if page is not None:
            params["page"] = page

        url = self._build_api_url(C.API_EXPRESSIONS_TIME_SERIES)
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            payload = await response.json()
            return TimeSeriesResponse(**payload)

    async def get_group_time_series_async(
        self,
        group_id: str,
        attributes: List[str],
        filter: Optional[str] = None,
        data: str = "REFERENCE_DATA",
        format: str = "JSON",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        calendar: str = "CAL_USBANK",
        frequency: str = "FREQ_DAY",
        conversion: str = "CONV_LASTBUS_ABS",
        nan_treatment: str = "NA_NOTHING",
        page: Optional[str] = None,
    ) -> "TimeSeriesResponse":
        """Time series across instruments + analytics of a dataset, with optional filter."""
        validate_attributes_list(attributes)
        if start_date is not None:
            validate_date_format(start_date, "start-date")
        if end_date is not None:
            validate_date_format(end_date, "end-date")

        params: dict = {
            "group-id": group_id,
            "attributes": attributes,
            "data": data,
            "format": format,
            "calendar": calendar,
            "frequency": frequency,
            "conversion": conversion,
            "nan-treatment": nan_treatment,
        }
        if filter is not None:
            params["filter"] = filter
        if start_date is not None:
            params["start-date"] = start_date
        if end_date is not None:
            params["end-date"] = end_date
        if page is not None:
            params["page"] = page

        url = self._build_api_url(C.API_GROUP_TIME_SERIES)
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            payload = await response.json()
            return TimeSeriesResponse(**payload)


class GridMixin(_RequestProto):
    """Grid-data retrieval by expression or grid ID."""

    async def get_grid_data_async(
        self,
        expr: Optional[str] = None,
        grid_id: Optional[str] = None,
        date: Optional[str] = None,
    ) -> "GridDataResponse":
        """Retrieve grid data using either an expression or a grid ID."""
        if expr and grid_id:
            raise ValueError("Cannot specify both expr and grid_id")
        if not expr and not grid_id:
            raise ValueError("Must specify either expr or grid_id")
        if date is not None:
            validate_date_format(date, "date")

        params: dict = {}
        if expr is not None:
            params["expr"] = expr
        if grid_id is not None:
            params["gridId"] = grid_id
        if date is not None:
            params["date"] = date

        url = self._build_api_url(C.API_GRID_DATA)
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            payload = await response.json()
            return GridDataResponse(**payload)


# ---------------------------------------------------------------------------
# Pure transformation mixin (no HTTP)
# ---------------------------------------------------------------------------


class DataFrameMixin:
    """Pandas conversion methods for API response objects.

    Pure data transformation — no dependency on the HTTP client, auth, or
    rate limiter. Consumers must expose ``self.logger`` (a ``structlog``
    bound logger).
    """

    logger: "structlog.BoundLogger"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def to_dataframe(
        self,
        response_data: Any,
        flatten_nested: bool = True,
        include_metadata: bool = False,
        date_columns: Optional[List[str]] = None,
        numeric_columns: Optional[List[str]] = None,
        custom_transformations: Optional[Dict[str, Callable]] = None,
    ) -> "pd.DataFrame":
        """Convert any API response into a pandas DataFrame.

        Handles Pydantic models, lists, dicts, and primitives. Accepts optional
        date / numeric column hints plus custom per-column callables.
        """
        if not HAS_PANDAS:
            raise ImportError("pandas is required for DataFrame conversion. Install it with: pip install pandas")

        return self._convert_to_dataframe(
            response_data,
            flatten_nested,
            include_metadata,
            date_columns,
            numeric_columns,
            custom_transformations,
        )

    def groups_to_dataframe(
        self,
        groups: Union[List["Group"], "GroupList"],
        include_metadata: bool = False,
    ) -> "pd.DataFrame":
        """Convert a groups response to a DataFrame."""
        if hasattr(groups, "groups"):
            groups = groups.groups

        return self.to_dataframe(
            groups,
            flatten_nested=True,
            include_metadata=include_metadata,
            date_columns=["last_updated", "created_date"],
        )

    def files_to_dataframe(
        self,
        files: Union[List["FileInfo"], "FileList"],
        include_metadata: bool = False,
    ) -> "pd.DataFrame":
        """Convert a files response to a DataFrame."""
        if hasattr(files, "file_group_ids"):
            files = files.file_group_ids

        return self.to_dataframe(
            files,
            flatten_nested=True,
            include_metadata=include_metadata,
            date_columns=["last_modified", "created_date"],
            numeric_columns=["file_size"],
        )

    def instruments_to_dataframe(
        self,
        instruments: Any,
        include_metadata: bool = False,
    ) -> "pd.DataFrame":
        """Convert an instruments response to a DataFrame."""
        if hasattr(instruments, "instruments"):
            instruments = instruments.instruments

        return self.to_dataframe(
            instruments,
            flatten_nested=True,
            include_metadata=include_metadata,
            date_columns=["created_date", "last_updated"],
        )

    def time_series_to_dataframe(self, time_series: Any, include_metadata: bool = False) -> "pd.DataFrame":
        """Convert a time-series response to a DataFrame."""
        if hasattr(time_series, "data"):
            data = time_series.data
        elif hasattr(time_series, "series"):
            data = time_series.series
        elif hasattr(time_series, "time_series"):
            data = time_series.time_series
        else:
            data = time_series

        return self.to_dataframe(
            data,
            flatten_nested=True,
            include_metadata=include_metadata,
            date_columns=["date", "timestamp", "observation_date"],
            numeric_columns=[
                "value",
                "price",
                "volume",
                "open",
                "high",
                "low",
                "close",
            ],
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _convert_to_dataframe(
        self,
        data: Any,
        flatten_nested: bool = True,
        include_metadata: bool = False,
        date_columns: Optional[List[str]] = None,
        numeric_columns: Optional[List[str]] = None,
        custom_transformations: Optional[Dict[str, Callable]] = None,
    ) -> "pd.DataFrame":
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for DataFrame conversion")

        date_columns = date_columns or []
        numeric_columns = numeric_columns or []
        custom_transformations = custom_transformations or {}

        if data is None:
            return pd.DataFrame()

        if not isinstance(data, (list, tuple)):
            if hasattr(data, "__dict__") or hasattr(data, "__slots__"):
                data = [data]
            else:
                return pd.DataFrame({"value": [data]})

        # Chunk through large inputs to keep peak memory bounded.
        chunk_size = 1000
        all_records: list = []
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            chunk_records = []
            for item in chunk:
                record = self._extract_object_data(item, flatten_nested, include_metadata)
                if record:
                    chunk_records.append(record)
            if chunk_records:
                all_records.extend(chunk_records)

        if not all_records:
            return pd.DataFrame()

        df = pd.DataFrame(all_records)
        all_records.clear()

        df = self._apply_data_transformations(df, date_columns, numeric_columns, custom_transformations)
        return df

    def _extract_object_data(
        self, obj: Any, flatten_nested: bool = True, include_metadata: bool = False
    ) -> Dict[str, Any]:
        if obj is None:
            return {}

        record: Dict[str, Any] = {}

        if hasattr(obj, "model_dump"):
            try:
                data = obj.model_dump()
                record.update(self._process_dict_data(data, flatten_nested, include_metadata))
            except Exception:
                if hasattr(obj, "__dict__"):
                    record.update(self._process_dict_data(obj.__dict__, flatten_nested, include_metadata))

        elif hasattr(obj, "__dict__"):
            record.update(self._process_dict_data(obj.__dict__, flatten_nested, include_metadata))

        elif isinstance(obj, dict):
            record.update(self._process_dict_data(obj, flatten_nested, include_metadata))

        else:
            record["value"] = obj

        return record

    def _process_dict_data(
        self,
        data: Dict[str, Any],
        flatten_nested: bool = True,
        include_metadata: bool = False,
    ) -> Dict[str, Any]:
        processed: Dict[str, Any] = {}

        for key, value in data.items():
            if key.startswith("_") and not include_metadata:
                continue

            if isinstance(value, dict) and flatten_nested:
                for nested_key, nested_value in value.items():
                    processed[f"{key}_{nested_key}"] = self._convert_value(nested_value)

            elif isinstance(value, (list, tuple)) and flatten_nested:
                if value and isinstance(value[0], dict):
                    # Capture structure from the first few entries — long arrays
                    # create runaway column counts otherwise.
                    for i, list_item in enumerate(value[:5]):
                        if isinstance(list_item, dict):
                            for nested_key, nested_value in list_item.items():
                                processed[f"{key}_{i}_{nested_key}"] = self._convert_value(nested_value)
                else:
                    processed[key] = str(value) if value else None

            else:
                processed[key] = self._convert_value(value)

        return processed

    def _convert_value(self, value: Any) -> Any:
        if value is None:
            return None

        if hasattr(value, "isoformat"):
            return value.isoformat()

        if hasattr(value, "model_dump"):
            try:
                return str(value.model_dump())
            except Exception:
                return str(value)

        if hasattr(value, "__dict__"):
            return str(value.__dict__)

        return value

    def _apply_data_transformations(
        self,
        df: "pd.DataFrame",
        date_columns: List[str],
        numeric_columns: List[str],
        custom_transformations: Dict[str, Callable],
    ) -> "pd.DataFrame":
        try:
            import pandas as pd
        except ImportError:
            return df

        for column, transform_func in custom_transformations.items():
            if column in df.columns:
                try:
                    df[column] = df[column].apply(transform_func)
                except Exception as e:
                    self.logger.warning(f"Failed to apply transformation to column '{column}': {e}")

        for column in date_columns:
            if column in df.columns:
                try:
                    df[column] = pd.to_datetime(df[column], errors="coerce")
                except Exception as e:
                    self.logger.warning(f"Failed to convert column '{column}' to datetime: {e}")

        for column in numeric_columns:
            if column in df.columns:
                try:
                    df[column] = pd.to_numeric(df[column], errors="coerce")
                except Exception as e:
                    self.logger.warning(f"Failed to convert column '{column}' to numeric: {e}")

        df = self._auto_convert_columns(df)
        return df

    def _auto_convert_columns(self, df: "pd.DataFrame") -> "pd.DataFrame":
        try:
            import pandas as pd
        except ImportError:
            return df

        _DATE_HINTS = ("date", "time", "created", "updated", "modified", "expires")
        _NUMERIC_HINTS = (
            "size",
            "count",
            "bytes",
            "price",
            "value",
            "amount",
            "volume",
            "quantity",
            "number",
            "id",
        )

        for column in df.columns:
            if df[column].dtype != "object":
                continue

            column_lower = column.lower()

            if any(hint in column_lower for hint in _DATE_HINTS):
                try:
                    sample_values = df[column].dropna().head(3)
                    if len(sample_values) > 0:
                        pd.to_datetime(sample_values.iloc[0])  # probe
                        df[column] = pd.to_datetime(df[column], errors="coerce")
                        continue
                except Exception:
                    pass

            if any(hint in column_lower for hint in _NUMERIC_HINTS):
                try:
                    numeric_series = pd.to_numeric(df[column], errors="coerce")
                    if numeric_series.notna().sum() / len(df) > 0.7:
                        df[column] = numeric_series
                except Exception:
                    pass

        return df
