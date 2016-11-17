package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;

public interface VectorizedParquetColumnReader {
  void readBatch(
    int total,
    ColumnVector column,
    TypeInfo columnType) throws IOException;
}
