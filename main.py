#!/usr/bin/env python

# Helpful resources:
#     https://arrow.apache.org/docs/python/index.html
#     https://easydata.engineering/how-to-write-parquet-file-in-python

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime


def main():
    # https://arrow.apache.org/docs/python/api/datatypes.html
    my_schema = pa.schema([
        # skip null

        ('c_bool', pa.bool_()),

        ('c_int8', pa.int8()),
        ('c_int16', pa.int16()),
        ('c_int32', pa.int32()),
        ('c_int64', pa.int64()),

        ('c_uint8', pa.uint8()),
        ('c_uint16', pa.uint16()),
        ('c_uint32', pa.uint32()),
        ('c_uint64', pa.uint64()),

        # skip ('c_float16', pa.float16()),
        ('c_float32', pa.float32()),
        ('c_float64', pa.float64()),

        ('c_time32', pa.time32('ms')),
        ('c_time64', pa.time64('ns')),
        ('c_timestamp', pa.timestamp('ms')),
        ('c_date32', pa.date32()),
        ('c_date64', pa.date64()),

        # skip binary

        ('c_string', pa.string()),

        # skip utf8
        # skip large_binary
        # skip large_string
        # skip large_utf8

        ('c_decimal128_8_3', pa.decimal128(8, 3))

        # skip list_
        # skip  large_list
        # skip struct
        # skip dictionary
        # skip field
        # skip schema
        # skip from_numpy_dtype
    ])

    c_bool = pa.array([False, True, False], type=pa.bool_())

    c_int8 = pa.array([1, 2, 3], type=pa.int8())
    c_int16 = pa.array([1, 2, 3], type=pa.int16())
    c_int32 = pa.array([1, 2, 3], type=pa.int32())
    c_int64 = pa.array([1, 2, 3], type=pa.int64())

    c_uint8 = pa.array([1, 2, 3], type=pa.uint8())
    c_uint16 = pa.array([1, 2, 3], type=pa.uint16())
    c_uint32 = pa.array([1, 2, 3], type=pa.uint32())
    c_uint64 = pa.array([1, 2, 3], type=pa.uint64())

    # c_float16 = pa.array([np.float16(1.0), np.float16(2.0), np.float16(3.0)], type=pa.float16())
    c_float32 = pa.array([1.0, 2.0, 3.0], type=pa.float32())
    c_float64 = pa.array([1.0, 2.0, 3.0], type=pa.float64())

    c_time32 = pa.array([1, 2, 3], type=pa.time32('ms'))
    c_time64 = pa.array([1, 2, 3], type=pa.time64('ns'))
    c_timestamp = pa.array([
        datetime(2019, 9, 3, 9, 0, 0),
        datetime(2019, 9, 3, 10, 0, 0),
        datetime(2019, 9, 3, 11, 0, 0)
    ], type=pa.timestamp('ms'))
    c_date32 = pa.array([
        datetime(2019, 9, 3, 9, 0, 0),
        datetime(2019, 9, 3, 10, 0, 0),
        datetime(2019, 9, 3, 11, 0, 0)
    ], type=pa.date32())
    c_date64 = pa.array([
        datetime(2019, 9, 3, 9, 0, 0),
        datetime(2019, 9, 3, 10, 0, 0),
        datetime(2019, 9, 3, 11, 0, 0)
    ], type=pa.date64())

    c_string = pa.array(
        ['first@example.com', 'second@example.com', 'third@example.com'],
        type=pa.string()
    )

    c_decimal128_8_3 = pa.array([1, 2, 3], type=pa.decimal128(8, 3))

    batch = pa.RecordBatch.from_arrays(
        [c_bool,
         c_int8, c_int16, c_int32, c_int64,
         c_uint8, c_uint16, c_uint32, c_uint64,
         # c_float16,
         c_float32, c_float64,
         c_time32, c_time64, c_timestamp, c_date32, c_date64,
         c_string,
         c_decimal128_8_3
         ],
        schema=my_schema
    )

    table = pa.Table.from_batches([batch])
    pq.write_table(table, 'example.parquet')


if __name__ == "__main__":
    main()

