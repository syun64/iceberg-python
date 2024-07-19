# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# pylint:disable=redefined-outer-name

import inspect
from copy import copy
from enum import Enum
from tempfile import TemporaryDirectory
from typing import Any

import pytest
from fastavro import reader

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import DataFile, write_manifest
from pyiceberg.table import Table
from pyiceberg.utils.lazydict import LazyDict


# helper function to serialize our objects to dicts to enable
# direct comparison with the dicts returned by fastavro
def todict(obj: Any) -> Any:
    if isinstance(obj, dict) or isinstance(obj, LazyDict):
        data = []
        for k, v in obj.items():
            data.append({"key": k, "value": v})
        return data
    elif isinstance(obj, Enum):
        return obj.value
    elif hasattr(obj, "__iter__") and not isinstance(obj, str) and not isinstance(obj, bytes):
        return [todict(v) for v in obj]
    elif hasattr(obj, "__dict__"):
        return {key: todict(value) for key, value in inspect.getmembers(obj) if not callable(value) and not key.startswith("_")}
    else:
        return obj


@pytest.fixture()
def catalog() -> Catalog:
    return load_catalog(
        "local",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )


@pytest.fixture()
def table_test_all_types(catalog: Catalog) -> Table:
    return catalog.load_table("default.test_all_types")


@pytest.mark.integration
def test_write_sample_manifest(table_test_all_types: Table) -> None:
    test_snapshot = table_test_all_types.current_snapshot()
    if test_snapshot is None:
        raise ValueError("Table has no current snapshot, check the docker environment")
    io = table_test_all_types.io
    test_manifest_file = test_snapshot.manifests(io)[0]
    test_manifest_entries = test_manifest_file.fetch_manifest_entry(io)
    entry = test_manifest_entries[0]
    test_schema = table_test_all_types.schema()
    test_spec = table_test_all_types.spec()
    wrapped_data_file_v2_debug = DataFile(
        format_version=2,
        content=entry.data_file.content,
        file_path=entry.data_file.file_path,
        file_format=entry.data_file.file_format,
        partition=entry.data_file.partition,
        record_count=entry.data_file.record_count,
        file_size_in_bytes=entry.data_file.file_size_in_bytes,
        column_sizes=entry.data_file.column_sizes,
        value_counts=entry.data_file.value_counts,
        null_value_counts=entry.data_file.null_value_counts,
        nan_value_counts=entry.data_file.nan_value_counts,
        lower_bounds=entry.data_file.lower_bounds,
        upper_bounds=entry.data_file.upper_bounds,
        key_metadata=entry.data_file.key_metadata,
        split_offsets=entry.data_file.split_offsets,
        equality_ids=entry.data_file.equality_ids,
        sort_order_id=entry.data_file.sort_order_id,
        spec_id=entry.data_file.spec_id,
    )
    wrapped_entry_v2 = copy(entry)
    wrapped_entry_v2.data_file = wrapped_data_file_v2_debug
    wrapped_entry_v2_dict = todict(wrapped_entry_v2)
    # This one should not be written
    del wrapped_entry_v2_dict["data_file"]["spec_id"]

    with TemporaryDirectory() as tmpdir:
        tmp_avro_file = tmpdir + "/test_write_manifest.avro"
        output = PyArrowFileIO().new_output(tmp_avro_file)
        with write_manifest(
            format_version=2,
            spec=test_spec,
            schema=test_schema,
            output_file=output,
            snapshot_id=test_snapshot.snapshot_id,
        ) as manifest_writer:
            # For simplicity, try one entry first
            manifest_writer.add_entry(test_manifest_entries[0])

        with open(tmp_avro_file, "rb") as fo:
            r = reader(fo=fo)
            it = iter(r)
            fa_entry = next(it)

            assert fa_entry == wrapped_entry_v2_dict


from pyspark.sql import SparkSession

@pytest.mark.integration
def test_write_round_trip(catalog: Catalog, spark: SparkSession) -> None:
    from pyiceberg.exceptions import NoSuchTableError
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, UUIDType, FixedType
    schema = Schema(
        NestedField(field_id=1, name="uuid_col", field_type=UUIDType(), required=False),
        NestedField(field_id=2, name="fixed_col", field_type=FixedType(25), required=False),
    )

    identifier = "default.test_table_write_round_trip"
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    catalog.create_table(
        identifier=identifier, schema=schema
    )

    # write to table with spark sql
    spark.sql(
        f"""
        INSERT INTO integration.{identifier} VALUES
        ('102cb62f-e6f8-4eb0-9973-d9b012ff0967', CAST('1234567890123456789012345' AS BINARY)),
        ('ec33e4b2-a834-4cc3-8c4a-a1d3bfc2f226', CAST('1231231231231231231231231' AS BINARY)),
        ('639cccce-c9d2-494a-a78c-278ab234f024', CAST('12345678901234567ass12345' AS BINARY)),
        ('c1b0d8e0-0b0e-4b1e-9b0a-0e0b0d0c0a0b', CAST('asdasasdads12312312312111' AS BINARY)),
        ('923dae77-83d6-47cd-b4b0-d383e64ee57e', CAST('qweeqwwqq1231231231231111' AS BINARY));
        """
    )

    tbl = catalog.load_table(identifier)
    assert tbl.schema() == schema
    df = tbl.scan().to_arrow().to_pandas()
    assert len(df) == 5
    assert b"1234567890123456789012345" in df["fixed_col"].to_list()