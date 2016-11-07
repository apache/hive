package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;
import java.util.List;

public class VectorizedStructReader implements VectorizedParquetColumnReader{

  List<VectorizedParquetColumnReader> fieldReaders;

  public VectorizedStructReader(List<VectorizedParquetColumnReader> fieldReaders) {
    this.fieldReaders = fieldReaders;
  }

  @Override
  public void readBatch(
    int total,
    ColumnVector column,
    TypeInfo columnType) throws IOException {
    StructColumnVector structColumnVector = (StructColumnVector) column;
    StructTypeInfo structTypeInfo = (StructTypeInfo) columnType;
    ColumnVector[] vectors = structColumnVector.fields;
    for (int i = 0; i < vectors.length; i++) {
      fieldReaders.get(i)
        .readBatch(total, vectors[i], structTypeInfo.getAllStructFieldTypeInfos().get(i));
    }
  }
}
