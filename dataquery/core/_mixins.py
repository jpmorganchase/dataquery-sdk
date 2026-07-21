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

from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)
from urllib.parse import urljoin, urlparse

import aiohttp

from .. import constants as C
from ..types.exceptions import APIResponseError, PaginationError
from ..types.models import (
    AttributesResponse,
    FileList,
    FiltersResponse,
    GridDataResponse,
    InstrumentsResponse,
    Paginated,
    TimeSeriesResponse,
)
from ..utils import (
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

    from ..types.models import FileInfo, Group, GroupList


__all__ = [
    "DataFrameMixin",
    "GridMixin",
    "InstrumentsMixin",
    "MetadataMixin",
    "PAGINATION_DEFAULT_MAX_PAGES",
    "PaginationMixin",
    "SearchMixin",
    "TimeSeriesMixin",
]


PAGINATION_DEFAULT_MAX_PAGES = 1000

P = TypeVar("P", bound=Paginated)


class _RequestProto:
    """Static typing shim for HTTP helpers the query mixins call on ``self``.

    The concrete ``DataQueryClient`` provides real implementations; the stubs
    here exist only so ``mypy`` can resolve attribute access inside the mixin
    method bodies.
    """

    def _build_api_url(self, endpoint: str) -> str:  # pragma: no cover - stub
        raise NotImplementedError

    def _build_files_api_url(self, endpoint: str) -> str:  # pragma: no cover - stub
        raise NotImplementedError

    async def _enter_request_cm(  # pragma: no cover - stub
        self, method: str, url: str, **kwargs: object
    ) -> aiohttp.ClientResponse:
        raise NotImplementedError

    async def _handle_response(  # pragma: no cover - stub
        self, response: aiohttp.ClientResponse
    ) -> None:
        raise NotImplementedError


class PaginationMixin(_RequestProto):
    """Client-driven and SDK-driven pagination over any ``Paginated`` response.

    The DataQuery v2 API returns ``links[].next`` cursor URLs on every paged
    endpoint. Two styles are supported:

    - **Client-driven** (:meth:`get_next_page_async`): the caller owns the
      loop. Fetch the first page with the normal single-page method, read
      ``page.next_link`` (surfaced to you), then hand the page back to
      ``get_next_page_async`` to fetch the next one. Nothing is fetched until
      you ask, so the client decides how far to page.
    - **SDK-driven** (:meth:`iter_pages`): the SDK walks every page for you,
      with loop detection and a page cap. The ``iter_*`` / ``*_all_*`` helpers
      build on this.
    """

    @staticmethod
    def _build_page(model_cls: Type[P], payload: Any) -> P:
        """Construct a paginated response, handling DataQuery's non-data envelopes.

        A paged endpoint can answer with data, or with an out-of-band envelope.
        An envelope is recognised structurally: it carries **none** of the
        model's own fields (by name or wire alias).

        - ``{"errors": [{"code", "message", "description"}, ...]}`` (or a single
          ``{"error": {...}}``) — a real failure returned inside a 2xx response,
          such as ``498 Unrecognized Page Token``. Raised as
          :class:`APIResponseError`.
        - ``{"info": {"code": "204", "description": "...no content available."}}``
          — a "no content" signal (``info`` is a declared field). Built into an
          empty page (no records, no ``links``) so pagination simply stops.
        - Any other unrecognisable body (``{}``, ``{"message": "..."}``) also
          raises :class:`APIResponseError` — a malformed 2xx must fail loudly
          rather than masquerade as an empty page.

        A data payload that merely *carries* an extra ``errors`` field alongside
        recognised fields is built normally (models allow extra fields).
        """
        if isinstance(payload, dict):
            known: set = set()
            for name, field in model_cls.model_fields.items():
                known.add(name)
                if field.alias:
                    known.add(field.alias)
            if not (known & payload.keys()):
                raw_errors = payload.get("errors") or payload.get("error")
                if raw_errors:
                    err_items = raw_errors if isinstance(raw_errors, list) else [raw_errors]
                    first = next((e for e in err_items if isinstance(e, dict)), {})
                    code = first.get("code")
                    description = first.get("description") or first.get("message") or "API returned an error response"
                    message = f"[{code}] {description}" if code is not None else description
                    raise APIResponseError(message, code=code, details={"errors": err_items})
                raise APIResponseError(
                    "Unrecognized response shape from paginated endpoint",
                    details={"keys": sorted(payload.keys())},
                )
        return model_cls(**payload)

    async def get_next_page_async(self, page: P) -> Optional[P]:
        """Fetch the page after ``page``, or ``None`` if it is the last page.

        This is the manual, client-driven counterpart to :meth:`iter_pages`.
        The returned page is the same type as ``page`` and carries its own
        ``links`` / ``items`` / ``page_size`` plus ``next_link`` for the page
        after it, so the caller can keep paging::

            page = await client.list_instruments_async(group_id)
            while page is not None:
                handle(page.instruments)
                page = await client.get_next_page_async(page)

        Args:
            page: The page whose ``links[].next`` should be followed. Passing a
                page with no next link returns ``None``.

        Returns:
            The next page (same type as ``page``), or ``None`` when ``page`` is
            the last page.

        Raises:
            APIResponseError: If the server answers with an error envelope or
                an unrecognizable body (see :meth:`_build_page`).
            PaginationError: If the ``next`` link points at a different host
                than the page's API surface — authenticated requests are never
                sent off-host.
        """
        next_url = page.get_next_link()
        if not next_url:
            return None

        # Resolve against the surface the page came from (files vs JSON API).
        # urljoin handles base-relative ("groups?page=2"), host-absolute
        # ("/research/.../groups?page=2"), and fully absolute links alike.
        base = self._page_base_url(page)
        absolute = urljoin(base, next_url)
        if urlparse(absolute).netloc.lower() != urlparse(base).netloc.lower():
            raise PaginationError(
                "Refusing to follow next link pointing at a different host",
                pages_fetched=1,
                items_collected=_page_item_count(page),
                url=absolute,
            )

        async with await self._enter_request_cm("GET", absolute) as response:
            await self._handle_response(response)
            payload = await response.json()
            return self._build_page(type(page), payload)

    def _page_base_url(self, page: Paginated) -> str:
        """Base URL a page's relative links resolve against.

        ``FileList`` pages come from the File Delivery surface
        (``files_api_base_url``); every other paginated response comes from the
        JSON Data API (``api_base_url``).
        """
        if isinstance(page, FileList):
            return self._build_files_api_url("")
        return self._build_api_url("")

    async def iter_pages(
        self,
        fetch_first: Callable[[], Awaitable[P]],
        *,
        max_pages: int = PAGINATION_DEFAULT_MAX_PAGES,
        raise_on_cap: bool = True,
    ) -> AsyncIterator[P]:
        """Yield each page of a paginated endpoint, following ``links[].next``.

        Args:
            fetch_first: Zero-arg coroutine returning the first page. Subsequent
                pages are fetched by GET-ing each ``next`` URL directly.
            max_pages: Hard cap on pages walked. Guards against pathological
                servers that loop ``next`` indefinitely.
            raise_on_cap: When ``True`` (default), raise :class:`PaginationError`
                if ``max_pages`` is reached. Set ``False`` to silently truncate.

        Yields:
            Each page response in order.

        Raises:
            PaginationError: If ``raise_on_cap`` is ``True`` and ``max_pages``
                is reached, or if the server returns a previously-seen ``next``
                URL (loop detected).
        """
        page = await fetch_first()
        page_count = 1
        items_so_far = _page_item_count(page)
        yield page

        visited: set = set()
        next_url = page.get_next_link()

        while next_url:
            if next_url in visited:
                raise PaginationError(
                    "Pagination loop detected — server returned a previously seen next link",
                    pages_fetched=page_count,
                    items_collected=items_so_far,
                    url=next_url,
                )
            visited.add(next_url)

            if page_count >= max_pages:
                if raise_on_cap:
                    raise PaginationError(
                        f"Pagination cap hit after {max_pages} pages",
                        pages_fetched=page_count,
                        items_collected=items_so_far,
                        cap=max_pages,
                    )
                return

            nxt = await self.get_next_page_async(page)
            if nxt is None:
                return
            page = nxt

            page_count += 1
            items_so_far += _page_item_count(page)
            yield page
            next_url = page.get_next_link()


def _page_item_count(page: Paginated) -> int:
    """Best-effort count of records in a page across all known response shapes."""
    for attr in ("groups", "instruments", "filters", "file_group_ids"):
        value = getattr(page, attr, None)
        if isinstance(value, list):
            return len(value)
    return 0


class InstrumentsMixin(PaginationMixin):
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
            return self._build_page(InstrumentsResponse, data)

    async def iter_instruments_async(
        self,
        group_id: str,
        instrument_id: Optional[str] = None,
        *,
        max_pages: int = PAGINATION_DEFAULT_MAX_PAGES,
    ) -> AsyncIterator[Any]:
        """Yield every instrument across all pages, lazily."""

        async def _first() -> InstrumentsResponse:
            return await self.list_instruments_async(group_id, instrument_id)

        async for page in self.iter_pages(_first, max_pages=max_pages):
            for inst in page.instruments:
                yield inst

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
            return self._build_page(InstrumentsResponse, data)

    async def iter_search_instruments_async(
        self,
        group_id: str,
        keywords: str,
        *,
        max_pages: int = PAGINATION_DEFAULT_MAX_PAGES,
    ) -> AsyncIterator[Any]:
        """Yield every instrument matching ``keywords`` across all pages."""

        async def _first() -> InstrumentsResponse:
            return await self.search_instruments_async(group_id, keywords)

        async for page in self.iter_pages(_first, max_pages=max_pages):
            for inst in page.instruments:
                yield inst


class MetadataMixin(PaginationMixin):
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
            return self._build_page(FiltersResponse, payload)

    async def iter_group_filters_async(
        self,
        group_id: str,
        *,
        max_pages: int = PAGINATION_DEFAULT_MAX_PAGES,
    ) -> AsyncIterator[Any]:
        """Yield every filter dimension across all pages."""

        async def _first() -> FiltersResponse:
            return await self.get_group_filters_async(group_id)

        async for page in self.iter_pages(_first, max_pages=max_pages):
            for f in page.filters:
                yield f

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
            return self._build_page(AttributesResponse, payload)

    async def iter_group_attributes_async(
        self,
        group_id: str,
        instrument_id: Optional[str] = None,
        *,
        max_pages: int = PAGINATION_DEFAULT_MAX_PAGES,
    ) -> AsyncIterator[Any]:
        """Yield every instrument-with-attributes across all pages."""

        async def _first() -> AttributesResponse:
            return await self.get_group_attributes_async(group_id, instrument_id)

        async for page in self.iter_pages(_first, max_pages=max_pages):
            for inst in page.instruments:
                yield inst


class TimeSeriesMixin(PaginationMixin):
    """Time-series retrieval by instrument, expression, or group."""

    async def get_instrument_time_series_async(
        self,
        instruments: List[str],
        attributes: List[str],
        data: str = "ALL",
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
            return self._build_page(TimeSeriesResponse, payload)

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
        data: str = "ALL",
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
            return self._build_page(TimeSeriesResponse, payload)

    async def get_group_time_series_async(
        self,
        group_id: str,
        attributes: List[str],
        filter: Optional[str] = None,
        data: str = "ALL",
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
            return self._build_page(TimeSeriesResponse, payload)

    async def iter_instrument_time_series_async(
        self,
        instruments: List[str],
        attributes: List[str],
        *,
        data: str = "ALL",
        format: str = "JSON",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        calendar: str = "CAL_USBANK",
        frequency: str = "FREQ_DAY",
        conversion: str = "CONV_LASTBUS_ABS",
        nan_treatment: str = "NA_NOTHING",
        max_pages: int = PAGINATION_DEFAULT_MAX_PAGES,
    ) -> AsyncIterator[Any]:
        """Yield every instrument-with-time-series across all pages."""

        async def _first() -> TimeSeriesResponse:
            return await self.get_instrument_time_series_async(
                instruments=instruments,
                attributes=attributes,
                data=data,
                format=format,
                start_date=start_date,
                end_date=end_date,
                calendar=calendar,
                frequency=frequency,
                conversion=conversion,
                nan_treatment=nan_treatment,
            )

        async for page in self.iter_pages(_first, max_pages=max_pages):
            for inst in page.instruments:
                yield inst

    async def iter_expressions_time_series_async(
        self,
        expressions: List[str],
        *,
        format: str = "JSON",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        calendar: str = "CAL_USBANK",
        frequency: str = "FREQ_DAY",
        conversion: str = "CONV_LASTBUS_ABS",
        nan_treatment: str = "NA_NOTHING",
        data: str = "ALL",
        max_pages: int = PAGINATION_DEFAULT_MAX_PAGES,
    ) -> AsyncIterator[Any]:
        """Yield every instrument-with-time-series across all pages of an expression query."""

        async def _first() -> TimeSeriesResponse:
            return await self.get_expressions_time_series_async(
                expressions=expressions,
                format=format,
                start_date=start_date,
                end_date=end_date,
                calendar=calendar,
                frequency=frequency,
                conversion=conversion,
                nan_treatment=nan_treatment,
                data=data,
            )

        async for page in self.iter_pages(_first, max_pages=max_pages):
            for inst in page.instruments:
                yield inst

    async def iter_group_time_series_async(
        self,
        group_id: str,
        attributes: List[str],
        *,
        filter: Optional[str] = None,
        data: str = "ALL",
        format: str = "JSON",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        calendar: str = "CAL_USBANK",
        frequency: str = "FREQ_DAY",
        conversion: str = "CONV_LASTBUS_ABS",
        nan_treatment: str = "NA_NOTHING",
        max_pages: int = PAGINATION_DEFAULT_MAX_PAGES,
    ) -> AsyncIterator[Any]:
        """Yield every instrument-with-time-series across all pages of a group query."""

        async def _first() -> TimeSeriesResponse:
            return await self.get_group_time_series_async(
                group_id=group_id,
                attributes=attributes,
                filter=filter,
                data=data,
                format=format,
                start_date=start_date,
                end_date=end_date,
                calendar=calendar,
                frequency=frequency,
                conversion=conversion,
                nan_treatment=nan_treatment,
            )

        async for page in self.iter_pages(_first, max_pages=max_pages):
            for inst in page.instruments:
                yield inst


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


class SearchMixin(_RequestProto):
    """Natural-language catalog search via POST /search."""

    async def search_async(self, query: str) -> Dict[str, Any]:
        """Search the DataQuery catalog using a natural-language query.

        POSTs ``{"query": query}`` to ``/search`` and returns the parsed JSON body.
        """
        if not query or not query.strip():
            raise ValueError("query must be a non-empty string")

        url = self._build_api_url(C.API_SEARCH)
        async with await self._enter_request_cm(
            "POST",
            url,
            json={"query": query},
        ) as response:
            await self._handle_response(response)
            return await response.json()


class DataFrameMixin:
    """Pandas conversion methods for API response objects.

    Pure data transformation — no dependency on the HTTP client, auth, or
    rate limiter. Consumers must expose ``self.logger`` (a ``structlog``
    bound logger).
    """

    logger: "structlog.BoundLogger"

    # Canonical column order for the tidy (long-format) time-series frame.
    _TS_CORE_COLUMNS = [
        "date",
        "value",
        "instrument_id",
        "instrument_name",
        "attribute_id",
        "attribute_name",
        "expression",
        "label",
    ]
    _TS_METADATA_COLUMNS = [
        "instrument_cusip",
        "instrument_isin",
        "group_id",
        "group_name",
        "last_published",
        "message",
    ]

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
        """Convert a time-series response into a tidy (long-format) DataFrame.

        Produces one row per (instrument, attribute, observation) with a parsed
        datetime ``date`` and numeric ``value``, plus the instrument and
        attribute identifiers needed to disambiguate rows. With
        ``include_metadata=True`` additional columns (CUSIP/ISIN, group,
        last-published, message) are added.

        Handles the nested ``TimeSeriesResponse`` shape
        (``instruments -> attributes -> time_series``) whether passed as a
        Pydantic model, an aliased dict, or a snake_case dict. A flat list of
        ``{"date": ..., "value": ...}`` records is also accepted. An empty
        response yields an empty DataFrame with the expected columns.
        """
        if not HAS_PANDAS:
            raise ImportError("pandas is required for DataFrame conversion. Install it with: pip install pandas")

        import pandas as pd

        payload = self._unwrap_time_series(time_series)
        instruments = self._as_instrument_list(payload)

        if instruments is not None:
            rows = self._build_time_series_rows(instruments, include_metadata)
            if rows:
                df = pd.DataFrame(rows)
            else:
                columns = list(self._TS_CORE_COLUMNS)
                if include_metadata:
                    columns += self._TS_METADATA_COLUMNS
                df = pd.DataFrame(columns=columns)
        else:
            records = list(payload) if isinstance(payload, (list, tuple)) else [payload]
            df = pd.DataFrame(records)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df

    @staticmethod
    def _ts_get(obj: Any, *names: str) -> Any:
        """Read the first present field from a model or dict, trying each name."""
        for name in names:
            if isinstance(obj, dict):
                if name in obj:
                    return obj[name]
            elif hasattr(obj, name):
                return getattr(obj, name)
        return None

    def _unwrap_time_series(self, time_series: Any) -> Any:
        """Pull the data payload out of a response wrapper.

        Prefers the nested ``instruments`` container; otherwise unwraps the
        legacy ``data``/``series``/``time_series`` carriers.
        """
        instruments = self._ts_get(time_series, "instruments")
        if instruments is not None:
            return instruments
        for attr in ("data", "series", "time_series"):
            value = self._ts_get(time_series, attr)
            if value is not None:
                return value
        return time_series

    def _as_instrument_list(self, payload: Any) -> Optional[List[Any]]:
        """Return the payload as a list of instruments, or ``None`` if it is flat.

        Instruments are recognised by carrying an ``attributes`` member; a flat
        list of observation records (no ``attributes``) returns ``None`` so the
        caller builds the frame directly.
        """
        if payload is None:
            return []
        items = list(payload) if isinstance(payload, (list, tuple)) else [payload]
        if not items:
            return []
        first = items[0]
        has_attributes = (isinstance(first, dict) and "attributes" in first) or (
            not isinstance(first, dict) and hasattr(first, "attributes")
        )
        return items if has_attributes else None

    def _build_time_series_rows(self, instruments: List[Any], include_metadata: bool) -> List[Dict[str, Any]]:
        """Flatten instruments -> attributes -> observations into tidy rows."""
        rows: List[Dict[str, Any]] = []
        for inst in instruments:
            inst_id = self._ts_get(inst, "instrument_id", "instrument-id")
            inst_name = self._ts_get(inst, "instrument_name", "instrument-name")
            group = self._ts_get(inst, "group") or {}
            group_id = self._ts_get(group, "group_id", "group-id")
            group_name = self._ts_get(group, "group_name", "group-name")
            cusip = self._ts_get(inst, "instrument_cusip", "instrument-cusip")
            isin = self._ts_get(inst, "instrument_isin", "instrument-isin")

            for attr in self._ts_get(inst, "attributes") or []:
                attr_id = self._ts_get(attr, "attribute_id", "attribute-id")
                attr_name = self._ts_get(attr, "attribute_name", "attribute-name")
                expression = self._ts_get(attr, "expression")
                label = self._ts_get(attr, "label")
                last_published = self._ts_get(attr, "last_published", "last-published")
                message = self._ts_get(attr, "message")

                for point in self._ts_get(attr, "time_series", "time-series") or []:
                    date, value = self._split_point(point)
                    row: Dict[str, Any] = {
                        "date": date,
                        "value": value,
                        "instrument_id": inst_id,
                        "instrument_name": inst_name,
                        "attribute_id": attr_id,
                        "attribute_name": attr_name,
                        "expression": expression,
                        "label": label,
                    }
                    if include_metadata:
                        row.update(
                            {
                                "instrument_cusip": cusip,
                                "instrument_isin": isin,
                                "group_id": group_id,
                                "group_name": group_name,
                                "last_published": last_published,
                                "message": message,
                            }
                        )
                    rows.append(row)
        return rows

    @staticmethod
    def _split_point(point: Any) -> tuple:
        """Split a single observation into ``(date, value)``.

        Accepts ``[date, value]`` pairs, ``{"date": ..., "value": ...}`` dicts,
        and ``TimeSeriesDataPoint`` models.
        """
        if isinstance(point, (list, tuple)):
            date = point[0] if len(point) > 0 else None
            value = point[1] if len(point) > 1 else None
            return date, value
        if isinstance(point, dict):
            return point.get("date"), point.get("value")
        return getattr(point, "date", None), getattr(point, "value", None)

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
            # pandas >= 3.0 stores text as the "str" dtype, not object — accept both.
            col_dtype = df[column].dtype
            if not (col_dtype == "object" or pd.api.types.is_string_dtype(col_dtype)):
                continue

            column_lower = column.lower()

            if any(hint in column_lower for hint in _DATE_HINTS):
                try:
                    sample_values = df[column].dropna().head(3)
                    if len(sample_values) > 0:
                        pd.to_datetime(sample_values.iloc[0])
                        df[column] = pd.to_datetime(df[column], errors="coerce")
                        continue
                except (ValueError, TypeError, AttributeError):
                    pass

            if any(hint in column_lower for hint in _NUMERIC_HINTS):
                try:
                    numeric_series = pd.to_numeric(df[column], errors="coerce")
                    if len(df) > 0 and numeric_series.notna().sum() / len(df) > 0.7:
                        df[column] = numeric_series
                except (ValueError, TypeError, AttributeError):
                    pass

        return df
