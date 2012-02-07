package org.apache.hadoop.hive.serde2.lazy.objectinspector;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class CassandraLazyObjectInspectorFactory {
  public static ObjectInspector getLazyObjectInspector(
      AbstractType validator) {
    return new CassandraValidatorObjectInspector(validator);
  }

  private CassandraLazyObjectInspectorFactory() {
    // prevent instantiation
  }
}
