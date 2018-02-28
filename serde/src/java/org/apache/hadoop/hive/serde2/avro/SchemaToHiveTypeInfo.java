package org.apache.hadoop.hive.serde2.avro;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class SchemaToHiveTypeInfo extends SchemaToTypeInfo {
  private static final SchemaToHiveTypeInfo instance = new SchemaToHiveTypeInfo();

  private SchemaToHiveTypeInfo() {
    //use getInstance to get this object. The base class uses cache to reuse
    //Types when available
    super(TypeInfoFactory.getInstance());
  }

  public static final SchemaToHiveTypeInfo getInstance() {
    return instance;
  }
}
