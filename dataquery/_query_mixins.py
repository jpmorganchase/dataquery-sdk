"""
Read-only query mixins for ``DataQueryClient``.

Each mixin covers one API concern and only depends on these methods of the
concrete client:

- ``_build_api_url(endpoint)`` — build a request URL
- ``_enter_request_cm(method, url, **kwargs)`` — issue an authenticated request
- ``_handle_response(response)`` — raise on HTTP errors

Extracting these keeps ``client.py`` focused on HTTP plumbing, auth, and
downloads, while the query surface lives here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

import aiohttp

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

if TYPE_CHECKING:
    pass


class _RequestProto:
    """Static typing shim — methods the mixins expect on ``self``.

    The concrete ``DataQueryClient`` provides these; the stubs here are only
    here to satisfy ``mypy`` when the mixin methods call them.
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

        url = self._build_api_url("group/instruments")
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

        url = self._build_api_url("group/instruments/search")
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

        url = self._build_api_url("group/filters")
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

        url = self._build_api_url("group/attributes")
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

        url = self._build_api_url("instruments/time-series")
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

        url = self._build_api_url("expressions/time-series")
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

        url = self._build_api_url("group/time-series")
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

        params: dict = {}
        if expr is not None:
            params["expr"] = expr
        if grid_id is not None:
            params["gridId"] = grid_id
        if date is not None:
            params["date"] = date

        url = self._build_api_url("grid-data")
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            payload = await response.json()
            return GridDataResponse(**payload)
