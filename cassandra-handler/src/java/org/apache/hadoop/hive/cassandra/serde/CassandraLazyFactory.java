package org.apache.hadoop.hive.cassandra.serde;

import org.apache.hadoop.hive.cassandra.serde.lazy.CassandraLazyInteger;
import org.apache.hadoop.hive.cassandra.serde.lazy.CassandraLazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyBoolean;
import org.apache.hadoop.hive.serde2.lazy.LazyByte;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyFloat;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyShort;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyByteObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyFloatObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyIntObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyLongObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyShortObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

/**
 * A LazyFactory class used by cassandra integration into Hive.
 *
 *
 */
public class CassandraLazyFactory {

  /**
   * Create a lazy primitive class given the type name. For Long and INT we use CassandraLazyLong and CassandraLazyInt
   * instead of the LazyObject from Hive.
   */
  public static LazyObject createLazyPrimitiveClass(
      PrimitiveObjectInspector oi) {
    PrimitiveCategory p = oi.getPrimitiveCategory();
    //TODO: We might need to handle FLOAT and DOUBLE differently as well.
    switch (p) {
      case BOOLEAN:
        return new LazyBoolean((LazyBooleanObjectInspector) oi);
      case BYTE:
        return new LazyByte((LazyByteObjectInspector) oi);
      case SHORT:
        return new LazyShort((LazyShortObjectInspector) oi);
      case INT:
        return new CassandraLazyInteger((LazyIntObjectInspector) oi);
      case LONG:
        return new CassandraLazyLong((LazyLongObjectInspector) oi);
      case FLOAT:
        return new LazyFloat((LazyFloatObjectInspector) oi);
      case DOUBLE:
        return new LazyDouble((LazyDoubleObjectInspector) oi);
      case STRING:
        return new LazyString((LazyStringObjectInspector) oi);
      default:
        throw new RuntimeException("Internal error: no LazyObject for " + p);
    }
  }

  /**
   * Create a hierarchical LazyObject based on the given typeInfo.
   */
  public static LazyObject createLazyObject(ObjectInspector oi) {
    ObjectInspector.Category c = oi.getCategory();
    switch (c) {
      case PRIMITIVE:
        return createLazyPrimitiveClass((PrimitiveObjectInspector) oi);
      case MAP:
      case LIST:
      case STRUCT:
      case UNION:
        return LazyFactory.createLazyObject(oi);
    }

    throw new RuntimeException("Hive LazySerDe Internal error.");
  }

}
