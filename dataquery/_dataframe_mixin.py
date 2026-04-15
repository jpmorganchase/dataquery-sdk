"""
DataFrame conversion helpers used by ``DataQueryClient``.

Kept in its own module because this logic is pure data transformation — it has
no dependency on the HTTP client, auth, or rate limiter. Extracting it lets
``client.py`` focus on API calls.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    pd = None  # type: ignore[assignment]
    HAS_PANDAS = False

if TYPE_CHECKING:
    import structlog

    from .models import FileInfo, FileList, Group, GroupList


class DataFrameMixin:
    """Mixin providing pandas conversion methods for API response objects.

    Consumers of this mixin must expose ``self.logger`` (a ``structlog`` logger).
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
