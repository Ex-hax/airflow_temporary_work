import logging
from typing import Any, Dict, Optional
import json
import pandas as pd
import pendulum

from siampiwat.common.logging.get_logger import Logger

logger = Logger.get_logger()


def load_json_config(config_path: str) -> Dict[str, Any]:
    """
    Loads JSON configuration from a file path.

    Args:
    -----
    :config_path (str): The path to the JSON configuration file.

    Returns:
    --------
    :Dict[str, Any]: The loaded configuration dictionary.
    """
    with open(config_path, "r") as f:
        return json.load(f)

def extract_and_correct_timezone(column: pd.Series, original_tz: str, target_tz: str) -> pd.Series:
    """
    Extracts 'member0' from a dictionary within the series, localizes to the original timezone, and converts to the target timezone.

    Args:
    -----
    :column (pd.Series): The pandas Series containing dictionary entries with datetime information.
    :original_tz (str): The original timezone as a string.
    :target_tz (str): The target timezone to which datetime will be converted.

    Returns:
    --------
    :pd.Series: A Series with datetime objects converted to the target timezone.
    """
    def extract_timestamp(value: Any) -> pd.Timestamp:
        if pd.isna(value):
            return pd.NaT
        if isinstance(value, dict) and "member0" in value:
            try:
                dt = pd.to_datetime(value["member0"])
                if dt.tzinfo is not None:
                    dt = dt.tz_convert(None)  # Remove timezone
                dt = dt.tz_localize(original_tz).tz_convert(target_tz)
                return dt
            except Exception as e:
                logger.error("Error converting value: %s, Error: %s", value, str(e))
        return pd.NaT
    return column.apply(extract_timestamp)

def convert_to_nullable_type(column: pd.Series, dtype: str, nullable_type_mapping: Dict[str, str]) -> pd.Series:
    """
    Converts a column to a nullable type if applicable.

    Args:
    -----
    :column (pd.Series): The pandas Series to be converted.
    :dtype (str): The target data type as a string.
    :nullable_type_mapping (Dict[str, str]): Mapping of standard types to their nullable equivalents.

    Returns:
    --------
    :pd.Series: The converted Series.
    """
    if dtype in nullable_type_mapping:
        try:
            return column.astype(nullable_type_mapping[dtype])
        except Exception as e:
            logger.error("Error casting column to nullable type: %s, Error: %s", dtype, str(e))
            return column
    return column.astype(dtype)

def schema_mapping(dataframe: pd.DataFrame, table_schema_mapping: Dict[str, str], original_tz: str, target_tz: str) -> pd.DataFrame:
    """
    Applies data type transformations based on schema mapping and timezone correction.

    Args:
    -----
    :dataframe (pd.DataFrame): The original DataFrame to transform.
    :table_schema_mapping (Dict[str, str]): A mapping of column names to desired data types.
    :original_tz (str): The original timezone of datetime entries.
    :target_tz (str): The target timezone for the conversion.

    Returns:
    --------
    :pd.DataFrame: The transformed DataFrame with corrected timezones and data types.
    """
    dataframe_copy = dataframe.copy()

    # Define mapping for nullable types
    nullable_type_mapping = {
        "int64": "Int64",  # Nullable integer
        "bool": "boolean",  # Nullable boolean
        "float64": "Float64",  # Nullable float
        "string": "string",  # Nullable string
    }

    for column, dtype in table_schema_mapping.items():
        logger.info("Processing column: %s, dtype: %s", column, dtype)
        
        if dtype == "timestamp":
            dataframe_copy[column] = pd.to_datetime(dataframe_copy[column])
            # Handle timezone localization and conversion
            if dataframe_copy[column].dt.tz is None:
                dataframe_copy[column] = dataframe_copy[column].dt.tz_localize(original_tz)
            else:
                dataframe_copy[column] = dataframe_copy[column].dt.tz_convert(target_tz)

        elif dtype == "member0_timestamp":
            dataframe_copy[column] = extract_and_correct_timezone(dataframe_copy[column], original_tz, target_tz)
        
        elif dtype in ["bool", "int64", "float64", "string"]:
            dataframe_copy[column] = convert_to_nullable_type(dataframe_copy[column], dtype, nullable_type_mapping)

    # Generate 'loaddt' column with the current date-time in the Asia/Bangkok timezone formatted as YYYYMMDDHHMM
    if "loaddt" not in dataframe_copy.columns:
        current_time_bangkok = pendulum.now("Asia/Bangkok")
        loaddt_value = current_time_bangkok.format("YYYYMMDDHHmm")
        dataframe_copy["loaddt"] = loaddt_value

    return dataframe_copy


def extract_loaddt_values(
    target_type: str,
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    endpoint: Optional[str] = None,
    conf: Optional[Dict[str, Any]] = None,
    conn_id: Optional[str] = None,
    ds: Optional[str] = None
) -> Dict[str, Optional[str]]:
    """
    Extracts the latest 'loaddt' values for each table from the OBS.

    Args:
    -----
    :param target_type: str: The type of the target (e.g. OBS, postgres_onprem).
    :param access_key_id: str: The access key ID for the target (e.g. obs_default, postgres_onprem_default).
    :param secret_access_key: str: The secret access key for the target (e.g. obs_default, postgres_onprem_default).
    :param endpoint: str: The endpoint for the target (e.g. obs_default, postgres_onprem_default).
    :param conf: Dict[str, Any]: The configuration for the DAG.
    :param conn_id: str: The connection ID for the target (e.g. obs_default, postgres_onprem_default).
    :param ds: str: The date string (e.g. 2024-06-01).

    Returns:
    --------
    :return: Dict[str, Optional[str]]: The latest 'loaddt' values for each table.
    """
    logger.info("Extracting latest 'loaddt' values for each table from the %s.", target_type)

    if target_type == "OBS":
        from siampiwat.common.repositories import OBSRepository

        logger.info("Extracting latest 'loaddt' values for each table from the %s.", target_type)
        obs_repo = OBSRepository(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            endpoint=endpoint,
            input_file_format=None,
            output_file_format=None,
        )

        loaddts = obs_repo.get_latest_loaddt_per_table(
            bucket_name=conf.get("bucket_name"),
            tables=conf.get("tables"),
            domain=conf.get("domain"),
            source=conf.get("source"),
        )
        logger.info("Extracted `loaddt` values: %s", loaddts)
        return loaddts

    elif target_type == "postgres_onprem":
        logger.info("Extracting `loaddt` values from the %s database.", target_type)

        schema = conf.get("postgres_onprem_schema")
        tables = conf.get("postgres_onprem_tables")
        loaddts = {}

        for table in tables:
            loaddts[table] = ds

        logger.info("Extracted `loaddt` values: %s", loaddts)
        return loaddts
    else:
        raise NotImplementedError(f"Target type: {target_type} is not implemented.")