package org.apache.hadoop.hive.cassandra.serde;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.hadoop.hive.serde2.lazy.CassandraLazyInteger;
import org.apache.hadoop.hive.serde2.lazy.CassandraLazyLong;
import org.apache.hadoop.hive.serde2.lazy.CassandraLazyValidator;
import org.apache.hadoop.hive.serde2.lazy.LazyBoolean;
import org.apache.hadoop.hive.serde2.lazy.LazyByte;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyFloat;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyShort;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.CassandraLazyObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.CassandraValidatorObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyObjectInspectorFactory;
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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

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
    if (oi instanceof CassandraValidatorObjectInspector) {
      return new CassandraLazyValidator((CassandraValidatorObjectInspector) oi);
    }

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

  /**
   * Create a hierarchical ObjectInspector for LazyObject with the given
   * typeInfo.
   *
   * @param typeInfo
   *          The type information for the LazyObject
   * @param separator
   *          The array of separators for delimiting each level
   * @param separatorIndex
   *          The current level (for separators). List(array), struct uses 1
   *          level of separator, and map uses 2 levels: the first one for
   *          delimiting entries, the second one for delimiting key and values.
   * @param nullSequence
   *          The sequence of bytes representing NULL.
   * @return The ObjectInspector
   */
  public static ObjectInspector createLazyObjectInspector(AbstractType validator,
      byte[] separator, int separatorIndex, Text nullSequence, boolean escaped,
      byte escapeChar) {
    return CassandraLazyObjectInspectorFactory.getLazyObjectInspector(validator);
  }

  /**
   * Create a hierarchical ObjectInspector for LazyStruct with the given
   * columnNames and columnTypeInfos.
   *
   * @param lastColumnTakesRest
   *          whether the last column of the struct should take the rest of the
   *          row if there are extra fields.
   * @see LazyFactory#createLazyObjectInspector(TypeInfo, byte[], int, Text,
   *      boolean, byte)
   */
  public static ObjectInspector createLazyStructInspector(
      List<String> columnNames, List<TypeInfo> typeInfos, List<AbstractType> validatorTypes, byte[] separators,
      Text nullSequence, boolean lastColumnTakesRest, boolean escaped,
      byte escapeChar) {
    if (validatorTypes.size() == 0) {
      return LazyFactory.createLazyStructInspector(columnNames,
          typeInfos,
          separators,
          nullSequence,
          lastColumnTakesRest,
          escaped,
          escapeChar);

    }

    ArrayList<ObjectInspector> columnObjectInspectors = new ArrayList<ObjectInspector>(
        validatorTypes.size());
    for (int i = 0; i < validatorTypes.size(); i++) {
      columnObjectInspectors.add(createLazyObjectInspector(
          validatorTypes.get(i), separators, 1, nullSequence, escaped, escapeChar));
    }
    return LazyObjectInspectorFactory.getLazySimpleStructObjectInspector(
        columnNames, columnObjectInspectors, separators[0], nullSequence,
        lastColumnTakesRest, escaped, escapeChar);
  }

}
