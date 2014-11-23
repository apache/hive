package org.apache.hadoop.hive.ql.io.parquet.convert;

import org.apache.hadoop.io.Writable;

interface ConverterParent {
  void set(int index, Writable value);
}
