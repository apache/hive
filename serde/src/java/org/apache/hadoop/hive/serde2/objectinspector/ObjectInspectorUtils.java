/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.serde2.objectinspector;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampLocalTZObjectInspector;
import org.apache.hive.common.util.Murmur3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveWritableObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

/**
 * ObjectInspectorFactory is the primary way to create new ObjectInspector
 * instances.
 *
 * SerDe classes should call the static functions in this library to create an
 * ObjectInspector to return to the caller of SerDe2.getObjectInspector().
 */
public final class ObjectInspectorUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ObjectInspectorUtils.class.getName());

  /**
   * This enum controls how we copy primitive objects.
   *
   * DEFAULT means choosing the most efficient way between JAVA and WRITABLE.
   * JAVA means converting all primitive objects to java primitive objects.
   * WRITABLE means converting all primitive objects to writable objects.
   *
   */
  public enum ObjectInspectorCopyOption {
    DEFAULT, JAVA, WRITABLE
  }

  /**
   * This enum controls how we interpret null value when compare two objects.
   *
   * MINVALUE means treating null value as the minimum value.
   * MAXVALUE means treating null value as the maximum value.
   *
   */
  public enum NullValueOption {
	MINVALUE, MAXVALUE
  }

  /**
   * This class can be used to wrap Hive objects and put in HashMap or HashSet.
   * The objects will be compared using ObjectInspectors.
   *
   */
  public static class ObjectInspectorObject {
    private final Object[] objects;
    private final ObjectInspector[] oi;

    public ObjectInspectorObject(Object object, ObjectInspector oi) {
      this.objects = new Object[] { object };
      this.oi = new ObjectInspector[] { oi };
    }

    public ObjectInspectorObject(Object[] objects, ObjectInspector[] oi) {
      this.objects = objects;
      this.oi = oi;
    }

    public Object[] getValues() {
      return objects;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || obj.getClass() != this.getClass()) { return false; }

      ObjectInspectorObject comparedObject = (ObjectInspectorObject)obj;
      return ObjectInspectorUtils.compare(objects, oi, comparedObject.objects, comparedObject.oi) == 0;
    }

    @Override
    public int hashCode() {
      return ObjectInspectorUtils.getBucketHashCodeOld(objects, oi);
    }
  }

  /**
   * Calculates the hash code for array of Objects that contains writables. This is used
   * to work around the buggy Hadoop DoubleWritable hashCode implementation. This should
   * only be used for process-local hash codes; don't replace stored hash codes like bucketing.
   */
  public static int writableArrayHashCode(Object[] keys) {
    if (keys == null) return 0;
    int hashcode = 1;
    for (Object element : keys) {
      hashcode = 31 * hashcode;
      if (element == null) {
        // nothing
      } else if (element instanceof LazyDouble) {
        long v = Double.doubleToLongBits(((LazyDouble) element).getWritableObject().get());
        hashcode = hashcode + (int) (v ^ (v >>> 32));
      } else if (element instanceof DoubleWritable) {
        long v = Double.doubleToLongBits(((DoubleWritable) element).get());
        hashcode = hashcode + (int) (v ^ (v >>> 32));
      } else if (element instanceof Object[]) {
        // use deep hashcode for arrays
        hashcode = hashcode + Arrays.deepHashCode((Object[]) element);
      } else {
        hashcode = hashcode + element.hashCode();
      }
    }
    return hashcode;
  }

  /**
   * Ensures that an ObjectInspector is Writable.
   */
  public static ObjectInspector getWritableObjectInspector(ObjectInspector oi) {
    // All non-primitive OIs are writable so we need only check this case.
    if (oi.getCategory() == Category.PRIMITIVE) {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      if (!(poi instanceof AbstractPrimitiveWritableObjectInspector)) {
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            poi.getTypeInfo());
      }
    }
    return oi;
  }

  /**
   * Get the corresponding standard ObjectInspector array for an array of ObjectInspector.
   */
  public static ObjectInspector[] getStandardObjectInspector(ObjectInspector[] ois,
      ObjectInspectorCopyOption objectInspectorOption) {
    if (ois == null) return null;

    ObjectInspector[] result = new ObjectInspector[ois.length];
    for (int i = 0; i < ois.length; i++) {
      result[i] = getStandardObjectInspector(ois[i], objectInspectorOption);
    }

    return result;
  }

  /**
   * Get the corresponding standard ObjectInspector for an ObjectInspector.
   *
   * The returned ObjectInspector can be used to inspect the standard object.
   */
  public static ObjectInspector getStandardObjectInspector(ObjectInspector oi) {
    return getStandardObjectInspector(oi, ObjectInspectorCopyOption.DEFAULT);
  }

  public static ObjectInspector getStandardObjectInspector(ObjectInspector oi,
      ObjectInspectorCopyOption objectInspectorOption) {
    ObjectInspector result = null;
    switch (oi.getCategory()) {
    case PRIMITIVE: {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      switch (objectInspectorOption) {
      case DEFAULT: {
        if (poi.preferWritable()) {
          result = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
              poi.getTypeInfo());
        } else {
          result = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
              poi.getTypeInfo());
        }
        break;
      }
      case JAVA: {
        result = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
            poi.getTypeInfo());
        break;
      }
      case WRITABLE:
        result = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            poi.getTypeInfo());
        break;
      }
      break;
    }
    case LIST: {
      ListObjectInspector loi = (ListObjectInspector) oi;
      result = ObjectInspectorFactory
          .getStandardListObjectInspector(getStandardObjectInspector(loi
          .getListElementObjectInspector(), objectInspectorOption));
      break;
    }
    case MAP: {
      MapObjectInspector moi = (MapObjectInspector) oi;
      result = ObjectInspectorFactory.getStandardMapObjectInspector(
          getStandardObjectInspector(moi.getMapKeyObjectInspector(),
          objectInspectorOption), getStandardObjectInspector(moi
          .getMapValueObjectInspector(), objectInspectorOption));
      break;
    }
    case STRUCT: {
      StructObjectInspector soi = (StructObjectInspector) oi;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      List<String> fieldNames = new ArrayList<String>(fields.size());
      List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(
          fields.size());
      for (StructField f : fields) {
        fieldNames.add(f.getFieldName());
        fieldObjectInspectors.add(getStandardObjectInspector(f
            .getFieldObjectInspector(), objectInspectorOption));
      }
      result = ObjectInspectorFactory.getStandardStructObjectInspector(
          fieldNames, fieldObjectInspectors);
      break;
    }
    case UNION: {
      UnionObjectInspector uoi = (UnionObjectInspector) oi;
      List<ObjectInspector> ois = new ArrayList<ObjectInspector>();
      for (ObjectInspector eoi : uoi.getObjectInspectors()) {
        ois.add(getStandardObjectInspector(eoi, objectInspectorOption));
      }
      result = ObjectInspectorFactory.getStandardUnionObjectInspector(ois);
      break;
    }
    default: {
      throw new RuntimeException("Unknown ObjectInspector category!");
    }
    }
    return result;
  }

  /**
   * Copy specified fields in the input row to the output array of standard objects.
   * @param result output list of standard objects.
   * @param row input row.
   * @param startCol starting column number from the input row.
   * @param numCols number of columns to copy.
   * @param soi Object inspector for the to-be-copied columns.
   */
  public static void partialCopyToStandardObject(List<Object> result, Object row, int startCol,
      int numCols, StructObjectInspector soi,
      ObjectInspectorCopyOption objectInspectorOption) {

    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    int i = 0, j = 0;
    for (StructField f : fields) {
      if (i++ >= startCol) {
        result.add(copyToStandardObject(soi.getStructFieldData(row, f),
            f.getFieldObjectInspector(), objectInspectorOption));
        if (++j == numCols) {
          break;
        }
      }
    }
  }

  /**
   * Copy fields in the input row to the output array of standard objects.
   *
   * @param result
   *          output list of standard objects.
   * @param row
   *          input row.
   * @param soi
   *          Object inspector for the to-be-copied columns.
   * @param objectInspectorOption
   */
  public static void copyToStandardObject(List<Object> result, Object row,
      StructObjectInspector soi, ObjectInspectorCopyOption objectInspectorOption) {
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    for (StructField f : fields) {
      result.add(copyToStandardObject(soi.getStructFieldData(row, f),
          f.getFieldObjectInspector(), objectInspectorOption));
    }
  }

  /**
   * Returns a deep copy of an array of objects
   */
  public static Object[] copyToStandardObject(
      Object[] o, ObjectInspector[] oi, ObjectInspectorCopyOption objectInspectorOption) {
    if (o == null) return null;
    assert(o.length == oi.length);

    Object[] result = new Object[o.length];
    for (int i = 0; i < o.length; i++) {
      result[i] = ObjectInspectorUtils.copyToStandardObject(
          o[i], oi[i], objectInspectorOption);
    }

    return result;
  }

  /**
   * Returns a deep copy of the Object o that can be scanned by a
   * StandardObjectInspector returned by getStandardObjectInspector(oi).
   */
  public static Object copyToStandardObject(Object o, ObjectInspector oi) {
    return copyToStandardObject(o, oi, ObjectInspectorCopyOption.DEFAULT);
  }

  public static Object copyToStandardJavaObject(Object o, ObjectInspector oi) {
    return copyToStandardObject(o, oi, ObjectInspectorCopyOption.JAVA);
  }

  public static int getStructSize(ObjectInspector oi) throws SerDeException {
    if (oi.getCategory() != Category.STRUCT) {
      throw new SerDeException("Unexpected category " + oi.getCategory());
    }
    return ((StructObjectInspector)oi).getAllStructFieldRefs().size();
  }

  public static void copyStructToArray(Object o, ObjectInspector oi,
      ObjectInspectorCopyOption objectInspectorOption, Object[] dest, int offset)
          throws SerDeException {
    if (o == null) {
      return;
    }

    if (oi.getCategory() != Category.STRUCT) {
      throw new SerDeException("Unexpected category " + oi.getCategory());
    }

    StructObjectInspector soi = (StructObjectInspector) oi;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    for (int i = 0; i < fields.size(); ++i) {
      StructField f = fields.get(i);
      dest[offset + i] = copyToStandardObject(soi.getStructFieldData(o, f), f
          .getFieldObjectInspector(), objectInspectorOption);
    }
  }

  public static Object copyToStandardObject(
      Object o, ObjectInspector oi, ObjectInspectorCopyOption objectInspectorOption) {
    if (o == null) {
      return null;
    }

    Object result = null;
    switch (oi.getCategory()) {
    case PRIMITIVE: {
      PrimitiveObjectInspector loi = (PrimitiveObjectInspector) oi;
      if (objectInspectorOption == ObjectInspectorCopyOption.DEFAULT) {
        objectInspectorOption = loi.preferWritable() ?
            ObjectInspectorCopyOption.WRITABLE : ObjectInspectorCopyOption.JAVA;
      }
      switch (objectInspectorOption) {
      case JAVA:
        result = loi.getPrimitiveJavaObject(o);
        if (loi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP) {
          result = PrimitiveObjectInspectorFactory.javaTimestampObjectInspector.copyObject(result);
        } else if (loi.getPrimitiveCategory() ==
            PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMPLOCALTZ) {
          result = PrimitiveObjectInspectorFactory.javaTimestampTZObjectInspector.
              copyObject(result);
        }
        break;
      case WRITABLE:
        result = loi.getPrimitiveWritableObject(loi.copyObject(o));
        break;
      }
      break;
    }
    case LIST: {
      ListObjectInspector loi = (ListObjectInspector) oi;
      int length = loi.getListLength(o);
      ArrayList<Object> list = new ArrayList<Object>(length);
      for (int i = 0; i < length; i++) {
        list.add(copyToStandardObject(loi.getListElement(o, i), loi
            .getListElementObjectInspector(), objectInspectorOption));
      }
      result = list;
      break;
    }
    case MAP: {
      MapObjectInspector moi = (MapObjectInspector) oi;
      Map<Object, Object> map = new LinkedHashMap<Object, Object>();
      Map<? extends Object, ? extends Object> omap = moi.getMap(o);
      for (Map.Entry<? extends Object, ? extends Object> entry : omap
          .entrySet()) {
        map.put(copyToStandardObject(entry.getKey(), moi
            .getMapKeyObjectInspector(), objectInspectorOption),
            copyToStandardObject(entry.getValue(), moi
            .getMapValueObjectInspector(), objectInspectorOption));
      }
      result = map;
      break;
    }
    case STRUCT: {
      StructObjectInspector soi = (StructObjectInspector) oi;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      ArrayList<Object> struct = new ArrayList<Object>(fields.size());
      for (StructField f : fields) {
        struct.add(copyToStandardObject(soi.getStructFieldData(o, f), f
            .getFieldObjectInspector(), objectInspectorOption));
      }
      result = struct;
      break;
    }
    case UNION: {
      UnionObjectInspector uoi = (UnionObjectInspector)oi;
      List<ObjectInspector> objectInspectors = uoi.getObjectInspectors();
      Object object = copyToStandardObject(
              uoi.getField(o),
              objectInspectors.get(uoi.getTag(o)),
              objectInspectorOption);
      result = object;
      break;
    }
    default: {
      throw new RuntimeException("Unknown ObjectInspector category!");
    }
    }
    return result;
  }

  public static String getStandardStructTypeName(StructObjectInspector soi) {
    StringBuilder sb = new StringBuilder();
    sb.append("struct<");
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(fields.get(i).getFieldName());
      sb.append(":");
      sb.append(fields.get(i).getFieldObjectInspector().getTypeName());
    }
    sb.append(">");
    return sb.toString();
  }

  public static String getStandardUnionTypeName(UnionObjectInspector uoi) {
    StringBuilder sb = new StringBuilder();
    sb.append(serdeConstants.UNION_TYPE_NAME + "<");
    List<ObjectInspector> ois = uoi.getObjectInspectors();
    for(int i = 0; i < ois.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(ois.get(i).getTypeName());
    }
    sb.append(">");
    return sb.toString();
  }

  public static StructField getStandardStructFieldRef(String fieldName,
      List<? extends StructField> fields) {
    fieldName = fieldName.toLowerCase();
    for (int i = 0; i < fields.size(); i++) {
      if (fields.get(i).getFieldName().equals(fieldName)) {
        return fields.get(i);
      }
    }
    // For backward compatibility: fieldNames can also be integer Strings.
    try {
      int i = Integer.parseInt(fieldName);
      if (i >= 0 && i < fields.size()) {
        return fields.get(i);
      }
    } catch (NumberFormatException e) {
      // ignore
    }
    throw new RuntimeException("cannot find field " + fieldName + " from "
        + fields);
    // return null;
  }

  /**
   * Get all the declared non-static fields of Class c.
   */
  public static Field[] getDeclaredNonStaticFields(Class<?> c) {
    Field[] f = c.getDeclaredFields();
    ArrayList<Field> af = new ArrayList<Field>();
    for (int i = 0; i < f.length; ++i) {
      if (!Modifier.isStatic(f[i].getModifiers())) {
        af.add(f[i]);
      }
    }
    Field[] r = new Field[af.size()];
    for (int i = 0; i < af.size(); ++i) {
      r[i] = af.get(i);
    }
    return r;
  }

  /**
   * Get the class names of the ObjectInspector hierarchy. Mainly used for
   * debugging.
   */
  public static String getObjectInspectorName(ObjectInspector oi) {
    switch (oi.getCategory()) {
    case PRIMITIVE: {
      return oi.getClass().getSimpleName();
    }
    case LIST: {
      ListObjectInspector loi = (ListObjectInspector) oi;
      return oi.getClass().getSimpleName() + "<"
          + getObjectInspectorName(loi.getListElementObjectInspector()) + ">";
    }
    case MAP: {
      MapObjectInspector moi = (MapObjectInspector) oi;
      return oi.getClass().getSimpleName() + "<"
          + getObjectInspectorName(moi.getMapKeyObjectInspector()) + ","
          + getObjectInspectorName(moi.getMapValueObjectInspector()) + ">";
    }
    case STRUCT: {
      StringBuilder result = new StringBuilder();
      result.append(oi.getClass().getSimpleName() + "<");
      StructObjectInspector soi = (StructObjectInspector) oi;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      for (int i = 0; i < fields.size(); i++) {
        result.append(fields.get(i).getFieldName());
        result.append(":");
        result.append(getObjectInspectorName(fields.get(i)
            .getFieldObjectInspector()));
        if (i == fields.size() - 1) {
          result.append(">");
        } else {
          result.append(",");
        }
      }
      return result.toString();
    }
    case UNION: {
      StringBuilder result = new StringBuilder();
      result.append(oi.getClass().getSimpleName() + "<");
      UnionObjectInspector uoi = (UnionObjectInspector)oi;
      List<ObjectInspector> ois = uoi.getObjectInspectors();
      for (int i = 0; i < ois.size(); i++) {
        if (i > 0) {
          result.append(",");
        }
        result.append(getObjectInspectorName(ois.get(i)));
      }
      result.append(">");
      return result.toString();
    }
    default: {
      throw new RuntimeException("Unknown ObjectInspector category!");
    }
    }
  }

  /**
   * Computes the bucket number to which the bucketFields belong to
   * @param bucketFields  the bucketed fields of the row
   * @param bucketFieldInspectors  the ObjectInpsectors for each of the bucketed fields
   * @param totalBuckets the number of buckets in the table
   * @return the bucket number using Murmur hash
   */
  public static int getBucketNumber(Object[] bucketFields, ObjectInspector[] bucketFieldInspectors, int totalBuckets) {
    return getBucketNumber(getBucketHashCode(bucketFields, bucketFieldInspectors), totalBuckets);
  }

  /**
   * Computes the bucket number to which the bucketFields belong to
   * @param bucketFields  the bucketed fields of the row
   * @param bucketFieldInspectors  the ObjectInpsectors for each of the bucketed fields
   * @param totalBuckets the number of buckets in the table
   * @return the bucket number
   */
  @Deprecated
  public static int getBucketNumberOld(Object[] bucketFields, ObjectInspector[] bucketFieldInspectors, int totalBuckets) {
    return getBucketNumber(getBucketHashCodeOld(bucketFields, bucketFieldInspectors), totalBuckets);
  }

  /**
   * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL+BucketedTables
   * @param hashCode as produced by {@link #getBucketHashCode(Object[], ObjectInspector[])}
   */
  public static int getBucketNumber(int hashCode, int numberOfBuckets) {
    if(numberOfBuckets <= 0) {
      //note that (X % 0) is illegal and (X % -1) = 0
      // -1 is a common default when the value is missing
      throw new IllegalArgumentException("Number of Buckets must be > 0");
    }
    return (hashCode & Integer.MAX_VALUE) % numberOfBuckets;
  }

  /**
   * Computes the hash code for the given bucketed fields
   * @param bucketFields
   * @param bucketFieldInspectors
   * @return
   */
  @Deprecated
  public static int getBucketHashCodeOld(Object[] bucketFields, ObjectInspector[] bucketFieldInspectors) {
    int hashCode = 0;
    for (int i = 0; i < bucketFields.length; i++) {
      int fieldHash = ObjectInspectorUtils.hashCode(bucketFields[i], bucketFieldInspectors[i]);
      hashCode = 31 * hashCode + fieldHash;
    }
    return hashCode;
  }

  public static int hashCode(Object o, ObjectInspector objIns) {
    if (o == null) {
      return 0;
    }
    switch (objIns.getCategory()) {
    case PRIMITIVE: {
      PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) objIns);
      switch (poi.getPrimitiveCategory()) {
      case VOID:
        return 0;
      case BOOLEAN:
        return ((BooleanObjectInspector) poi).get(o) ? 1 : 0;
      case BYTE:
        return ((ByteObjectInspector) poi).get(o);
      case SHORT:
        return ((ShortObjectInspector) poi).get(o);
      case INT:
        return ((IntObjectInspector) poi).get(o);
      case LONG: {
        long a = ((LongObjectInspector) poi).get(o);
        return (int) ((a >>> 32) ^ a);
      }
      case FLOAT:
        return Float.floatToIntBits(((FloatObjectInspector) poi).get(o));
      case DOUBLE: {
        // This hash function returns the same result as Double.hashCode()
        // while DoubleWritable.hashCode returns a different result.
        long a = Double.doubleToLongBits(((DoubleObjectInspector) poi).get(o));
        return (int) ((a >>> 32) ^ a);
      }
      case STRING: {
        // This hash function returns the same result as String.hashCode() when
        // all characters are ASCII, while Text.hashCode() always returns a
        // different result.
        Text t = ((StringObjectInspector) poi).getPrimitiveWritableObject(o);
        int r = 0;
        for (int i = 0; i < t.getLength(); i++) {
          r = r * 31 + t.getBytes()[i];
        }
        return r;
      }
      case CHAR:
        return ((HiveCharObjectInspector) poi).getPrimitiveWritableObject(o).hashCode();
      case VARCHAR:
        return ((HiveVarcharObjectInspector)poi).getPrimitiveWritableObject(o).hashCode();
      case BINARY:
        return ((BinaryObjectInspector) poi).getPrimitiveWritableObject(o).hashCode();

      case DATE:
        return ((DateObjectInspector) poi).getPrimitiveWritableObject(o).hashCode();
      case TIMESTAMP:
        TimestampWritableV2 t = ((TimestampObjectInspector) poi)
            .getPrimitiveWritableObject(o);
        return t.hashCode();
      case TIMESTAMPLOCALTZ:
        TimestampLocalTZWritable tstz = ((TimestampLocalTZObjectInspector) poi).getPrimitiveWritableObject(o);
        return tstz.hashCode();
      case INTERVAL_YEAR_MONTH:
        HiveIntervalYearMonthWritable intervalYearMonth = ((HiveIntervalYearMonthObjectInspector) poi)
            .getPrimitiveWritableObject(o);
        return intervalYearMonth.hashCode();
      case INTERVAL_DAY_TIME:
        HiveIntervalDayTimeWritable intervalDayTime = ((HiveIntervalDayTimeObjectInspector) poi)
            .getPrimitiveWritableObject(o);
        return intervalDayTime.hashCode();
      case DECIMAL:
        // Since getBucketHashCode uses this, HiveDecimal return the old (much slower) but
        // compatible hash code.
        return ((HiveDecimalObjectInspector) poi).getPrimitiveWritableObject(o).hashCode();

      default: {
        throw new RuntimeException("Unknown type: "
            + poi.getPrimitiveCategory());
      }
      }
    }
    case LIST: {
      int r = 0;
      ListObjectInspector listOI = (ListObjectInspector)objIns;
      ObjectInspector elemOI = listOI.getListElementObjectInspector();
      for (int ii = 0; ii < listOI.getListLength(o); ++ii) {
        r = 31 * r + hashCode(listOI.getListElement(o, ii), elemOI);
      }
      return r;
    }
    case MAP: {
      int r = 0;
      MapObjectInspector mapOI = (MapObjectInspector)objIns;
      ObjectInspector keyOI = mapOI.getMapKeyObjectInspector();
      ObjectInspector valueOI = mapOI.getMapValueObjectInspector();
      Map<?, ?> map = mapOI.getMap(o);
      for (Map.Entry<?,?> entry : map.entrySet()) {
        r += hashCode(entry.getKey(), keyOI) ^
             hashCode(entry.getValue(), valueOI);
      }
      return r;
    }
    case STRUCT:
      int r = 0;
      StructObjectInspector structOI = (StructObjectInspector)objIns;
      List<? extends StructField> fields = structOI.getAllStructFieldRefs();
      for (StructField field : fields) {
        r = 31 * r + hashCode(structOI.getStructFieldData(o, field),
            field.getFieldObjectInspector());
      }
      return r;

    case UNION:
      UnionObjectInspector uOI = (UnionObjectInspector)objIns;
      byte tag = uOI.getTag(o);
      return hashCode(uOI.getField(o), uOI.getObjectInspectors().get(tag));

    default:
      throw new RuntimeException("Unknown type: "+ objIns.getTypeName());
    }
  }

  public static int getBucketHashCode(Object[] bucketFields, ObjectInspector[] bucketFieldInspectors) {
    int hashCode = 0;
    ByteBuffer b = ByteBuffer.allocate(8); // To be used with primitive types
    for (int i = 0; i < bucketFields.length; i++) {
      int fieldHash = ObjectInspectorUtils.hashCodeMurmur(
          bucketFields[i], bucketFieldInspectors[i], b);
      hashCode = 31 * hashCode + fieldHash;
    }
    return hashCode;
  }

  public static int hashCodeMurmur(Object o, ObjectInspector objIns, ByteBuffer byteBuffer) {
    if (o == null) {
      return 0;
    }
    // Reset the bytebuffer
    byteBuffer.clear();
    switch (objIns.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) objIns);
        switch (poi.getPrimitiveCategory()) {
          case VOID:
            return 0;
          case BOOLEAN:
            return (((BooleanObjectInspector) poi).get(o) ? 1 : 0);
          case BYTE:
            return ((ByteObjectInspector) poi).get(o);
          case SHORT: {
            byteBuffer.putShort(((ShortObjectInspector) poi).get(o));
            return Murmur3.hash32(byteBuffer.array(), 2);
          }
          case INT: {
            byteBuffer.putInt(((IntObjectInspector) poi).get(o));
            return Murmur3.hash32(byteBuffer.array(), 4);
          }
          case LONG: {
            byteBuffer.putLong(((LongObjectInspector) poi).get(o));
            return Murmur3.hash32(byteBuffer.array(), 8);
          }
          case FLOAT: {
            byteBuffer.putFloat(Float.floatToIntBits(((FloatObjectInspector) poi).get(o)));
            return Murmur3.hash32(byteBuffer.array(), 4);
          }
          case DOUBLE: {
            // This hash function returns the same result as Double.hashCode()
            // while DoubleWritable.hashCode returns a different result.
            byteBuffer.putDouble(Double.doubleToLongBits(((DoubleObjectInspector) poi).get(o)));
            return Murmur3.hash32(byteBuffer.array(), 8);
          }
          case STRING: {
            // This hash function returns the same result as String.hashCode() when
            // all characters are ASCII, while Text.hashCode() always returns a
            // different result.
            Text text = ((StringObjectInspector) poi).getPrimitiveWritableObject(o);
            return Murmur3.hash32(text.getBytes(), text.getLength());
          }
          case CHAR: {
            Text text = ((HiveCharObjectInspector) poi).getPrimitiveWritableObject(o).getStrippedValue();
            return Murmur3.hash32(text.getBytes(), text.getLength());
          }
          case VARCHAR: {
            Text text = ((HiveVarcharObjectInspector)poi).getPrimitiveWritableObject(o).getTextValue();
            return Murmur3.hash32(text.getBytes(), text.getLength());
          }
          case BINARY:
            return Murmur3.hash32(((BinaryObjectInspector) poi).getPrimitiveWritableObject(o).getBytes());

          case DATE:
            byteBuffer.putInt(((DateObjectInspector) poi).getPrimitiveWritableObject(o).getDays());
            return Murmur3.hash32(byteBuffer.array(), 4);
          case TIMESTAMP: {
            TimestampWritableV2 t = ((TimestampObjectInspector) poi)
                    .getPrimitiveWritableObject(o);
            return Murmur3.hash32(t.getBytes());
          }
          case TIMESTAMPLOCALTZ:
            return Murmur3.hash32((((TimestampLocalTZObjectInspector) poi).getPrimitiveWritableObject(o)).getBytes());
          case INTERVAL_YEAR_MONTH:
            byteBuffer.putInt(((HiveIntervalYearMonthObjectInspector) poi)
                    .getPrimitiveWritableObject(o).hashCode());
            return Murmur3.hash32(byteBuffer.array(), 4);
          case INTERVAL_DAY_TIME:
            byteBuffer.putInt(((HiveIntervalDayTimeObjectInspector) poi)
                    .getPrimitiveWritableObject(o).hashCode());
            return Murmur3.hash32(byteBuffer.array(), 4);
          case DECIMAL:
            // Since getBucketHashCode uses this, HiveDecimal return the old (much slower) but
            // compatible hash code.
            return Murmur3.hash32(((HiveDecimalObjectInspector) poi).getPrimitiveWritableObject(o).getInternalStorage());

          default: {
            throw new RuntimeException("Unknown type: "
                    + poi.getPrimitiveCategory());
          }
        }
      }
      case LIST: {
        int r = 0;
        ListObjectInspector listOI = (ListObjectInspector)objIns;
        ObjectInspector elemOI = listOI.getListElementObjectInspector();
        for (int ii = 0; ii < listOI.getListLength(o); ++ii) {
          //r = 31 * r + hashCode(listOI.getListElement(o, ii), elemOI);
          r = 31 * r + hashCodeMurmur(listOI.getListElement(o, ii), elemOI, byteBuffer);
        }
        return r;
      }
      case MAP: {
        int r = 0;
        MapObjectInspector mapOI = (MapObjectInspector)objIns;
        ObjectInspector keyOI = mapOI.getMapKeyObjectInspector();
        ObjectInspector valueOI = mapOI.getMapValueObjectInspector();
        Map<?, ?> map = mapOI.getMap(o);
        for (Map.Entry<?,?> entry : map.entrySet()) {
          r += hashCodeMurmur(entry.getKey(), keyOI, byteBuffer) ^
                  hashCode(entry.getValue(), valueOI);
        }
        return r;
      }
      case STRUCT:
        int r = 0;
        StructObjectInspector structOI = (StructObjectInspector)objIns;
        List<? extends StructField> fields = structOI.getAllStructFieldRefs();
        for (StructField field : fields) {
          r = 31 * r + hashCodeMurmur(structOI.getStructFieldData(o, field),
                  field.getFieldObjectInspector(), byteBuffer);
        }
        return r;

      case UNION:
        UnionObjectInspector uOI = (UnionObjectInspector)objIns;
        byte tag = uOI.getTag(o);
        return hashCodeMurmur(uOI.getField(o), uOI.getObjectInspectors().get(tag), byteBuffer);

      default:
        throw new RuntimeException("Unknown type: "+ objIns.getTypeName());
    }
  }

  /**
   * Compare two arrays of objects with their respective arrays of
   * ObjectInspectors.
   */
  public static int compare(Object[] o1, ObjectInspector[] oi1, Object[] o2,
      ObjectInspector[] oi2) {
    assert (o1.length == oi1.length);
    assert (o2.length == oi2.length);
    assert (o1.length == o2.length);

    for (int i = 0; i < o1.length; i++) {
      int r = compare(o1[i], oi1[i], o2[i], oi2[i]);
      if (r != 0) {
        return r;
      }
    }
    return 0;
  }

  /**
   * Whether comparison is supported for this type.
   * Currently all types that references any map are not comparable.
   */
  public static boolean compareSupported(ObjectInspector oi) {
    switch (oi.getCategory()) {
    case PRIMITIVE:
      return true;
    case LIST:
      ListObjectInspector loi = (ListObjectInspector) oi;
      return compareSupported(loi.getListElementObjectInspector());
    case STRUCT:
      StructObjectInspector soi = (StructObjectInspector) oi;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      for (int f = 0; f < fields.size(); f++) {
        if (!compareSupported(fields.get(f).getFieldObjectInspector())) {
          return false;
        }
      }
      return true;
    case MAP:
      return false;
    case UNION:
      UnionObjectInspector uoi = (UnionObjectInspector) oi;
      for (ObjectInspector eoi : uoi.getObjectInspectors()) {
        if (!compareSupported(eoi)) {
          return false;
        }
      }
      return true;
    default:
      return false;
    }
  }

  /**
   * Compare two objects with their respective ObjectInspectors.
   */
  public static int compare(Object o1, ObjectInspector oi1, Object o2,
      ObjectInspector oi2) {
    return compare(o1, oi1, o2, oi2, new FullMapEqualComparer());
  }

  /**
   * Compare two objects with their respective ObjectInspectors.
   * Treat null as minimum value.
   */
  public static int compare(Object o1, ObjectInspector oi1, Object o2,
      ObjectInspector oi2, MapEqualComparer mapEqualComparer) {
    return compare(o1, oi1, o2, oi2, mapEqualComparer, NullValueOption.MINVALUE);
  }

  /**
   * Compare two objects with their respective ObjectInspectors.
   * if nullValueOpt is MAXVALUE, treat null as maximum value.
   * if nullValueOpt is MINVALUE, treat null as minimum value.
   */
  public static int compare(Object o1, ObjectInspector oi1, Object o2,
      ObjectInspector oi2, MapEqualComparer mapEqualComparer, NullValueOption nullValueOpt) {
    if (oi1.getCategory() != oi2.getCategory()) {
      return oi1.getCategory().compareTo(oi2.getCategory());
    }

    int nullCmpRtn = -1;
    switch (nullValueOpt) {
    case MAXVALUE:
      nullCmpRtn = 1;
      break;
    case MINVALUE:
      nullCmpRtn = -1;
      break;
    }

    if (o1 == null) {
      return o2 == null ? 0 : nullCmpRtn;
    } else if (o2 == null) {
      return -nullCmpRtn;
    }

    switch (oi1.getCategory()) {
    case PRIMITIVE: {
      PrimitiveObjectInspector poi1 = ((PrimitiveObjectInspector) oi1);
      PrimitiveObjectInspector poi2 = ((PrimitiveObjectInspector) oi2);
      if (poi1.getPrimitiveCategory() != poi2.getPrimitiveCategory()) {
        return poi1.getPrimitiveCategory().compareTo(
            poi2.getPrimitiveCategory());
      }
      switch (poi1.getPrimitiveCategory()) {
      case VOID:
        return 0;
      case BOOLEAN: {
        int v1 = ((BooleanObjectInspector) poi1).get(o1) ? 1 : 0;
        int v2 = ((BooleanObjectInspector) poi2).get(o2) ? 1 : 0;
        return v1 - v2;
      }
      case BYTE: {
        int v1 = ((ByteObjectInspector) poi1).get(o1);
        int v2 = ((ByteObjectInspector) poi2).get(o2);
        return v1 - v2;
      }
      case SHORT: {
        int v1 = ((ShortObjectInspector) poi1).get(o1);
        int v2 = ((ShortObjectInspector) poi2).get(o2);
        return v1 - v2;
      }
      case INT: {
        int v1 = ((IntObjectInspector) poi1).get(o1);
        int v2 = ((IntObjectInspector) poi2).get(o2);
        return v1 > v2 ? 1 : (v1 < v2 ? -1 : 0);
      }
      case LONG: {
        long v1 = ((LongObjectInspector) poi1).get(o1);
        long v2 = ((LongObjectInspector) poi2).get(o2);
        return v1 > v2 ? 1 : (v1 < v2 ? -1 : 0);
      }
      case FLOAT: {
        float v1 = ((FloatObjectInspector) poi1).get(o1);
        float v2 = ((FloatObjectInspector) poi2).get(o2);

        // The IEEE 754 floating point spec specifies that signed -0.0 and 0.0 should be treated as equal.
        if (v1 == 0.0f && v2 == 0.0f) {
          return 0;
        } else {
          // Float.compare() treats -0.0 and 0.0 as different
          return Float.compare(v1, v2);
        }
      }
      case DOUBLE: {
        double v1 = ((DoubleObjectInspector) poi1).get(o1);
        double v2 = ((DoubleObjectInspector) poi2).get(o2);

        // The IEEE 754 floating point spec specifies that signed -0.0 and 0.0 should be treated as equal.
        if (v1 == 0.0d && v2 == 0.0d) {
          return 0;
        } else {
          // Double.compare() treats -0.0 and 0.0 as different
          return Double.compare(v1, v2);
        }
      }
      case STRING: {
        if (poi1.preferWritable() || poi2.preferWritable()) {
          Text t1 = (Text) poi1.getPrimitiveWritableObject(o1);
          Text t2 = (Text) poi2.getPrimitiveWritableObject(o2);
          return t1 == null ? (t2 == null ? 0 : -1) : (t2 == null ? 1
              : t1.compareTo(t2));
        } else {
          String s1 = (String) poi1.getPrimitiveJavaObject(o1);
          String s2 = (String) poi2.getPrimitiveJavaObject(o2);
          return s1 == null ? (s2 == null ? 0 : -1) : (s2 == null ? 1 : s1
              .compareTo(s2));
        }
      }
      case CHAR: {
        HiveCharWritable t1 = ((HiveCharObjectInspector)poi1).getPrimitiveWritableObject(o1);
        HiveCharWritable t2 = ((HiveCharObjectInspector)poi2).getPrimitiveWritableObject(o2);
        return t1.compareTo(t2);
      }
      case VARCHAR: {
        HiveVarcharWritable t1 = ((HiveVarcharObjectInspector)poi1).getPrimitiveWritableObject(o1);
        HiveVarcharWritable t2 = ((HiveVarcharObjectInspector)poi2).getPrimitiveWritableObject(o2);
        return t1.compareTo(t2);
      }
      case BINARY: {
        BytesWritable bw1 = ((BinaryObjectInspector) poi1).getPrimitiveWritableObject(o1);
        BytesWritable bw2 = ((BinaryObjectInspector) poi2).getPrimitiveWritableObject(o2);
        return bw1.compareTo(bw2);
      }

      case DATE: {
        DateWritableV2 d1 = ((DateObjectInspector) poi1)
            .getPrimitiveWritableObject(o1);
        DateWritableV2 d2 = ((DateObjectInspector) poi2)
            .getPrimitiveWritableObject(o2);
        return d1.compareTo(d2);
      }
      case TIMESTAMP: {
        TimestampWritableV2 t1 = ((TimestampObjectInspector) poi1)
            .getPrimitiveWritableObject(o1);
        TimestampWritableV2 t2 = ((TimestampObjectInspector) poi2)
            .getPrimitiveWritableObject(o2);
        return t1.compareTo(t2);
      }
      case TIMESTAMPLOCALTZ: {
        TimestampLocalTZWritable tstz1 = ((TimestampLocalTZObjectInspector) poi1).
            getPrimitiveWritableObject(o1);
        TimestampLocalTZWritable tstz2 = ((TimestampLocalTZObjectInspector) poi2).
            getPrimitiveWritableObject(o2);
        return tstz1.compareTo(tstz2);
      }
      case INTERVAL_YEAR_MONTH: {
        HiveIntervalYearMonthWritable i1 = ((HiveIntervalYearMonthObjectInspector) poi1)
            .getPrimitiveWritableObject(o1);
        HiveIntervalYearMonthWritable i2 = ((HiveIntervalYearMonthObjectInspector) poi2)
            .getPrimitiveWritableObject(o2);
        return i1.compareTo(i2);
      }
      case INTERVAL_DAY_TIME: {
        HiveIntervalDayTimeWritable i1 = ((HiveIntervalDayTimeObjectInspector) poi1)
            .getPrimitiveWritableObject(o1);
        HiveIntervalDayTimeWritable i2 = ((HiveIntervalDayTimeObjectInspector) poi2)
            .getPrimitiveWritableObject(o2);
        return i1.compareTo(i2);
      }
      case DECIMAL: {
        HiveDecimalWritable t1 = ((HiveDecimalObjectInspector) poi1)
            .getPrimitiveWritableObject(o1);
        HiveDecimalWritable t2 = ((HiveDecimalObjectInspector) poi2)
            .getPrimitiveWritableObject(o2);
        return t1.compareTo(t2);
      }
      default: {
        throw new RuntimeException("Unknown type: "
            + poi1.getPrimitiveCategory());
      }
      }
    }
    case STRUCT: {
      StructObjectInspector soi1 = (StructObjectInspector) oi1;
      StructObjectInspector soi2 = (StructObjectInspector) oi2;
      List<? extends StructField> fields1 = soi1.getAllStructFieldRefs();
      List<? extends StructField> fields2 = soi2.getAllStructFieldRefs();
      int minimum = Math.min(fields1.size(), fields2.size());
      for (int i = 0; i < minimum; i++) {
        int r = compare(soi1.getStructFieldData(o1, fields1.get(i)), fields1
            .get(i).getFieldObjectInspector(), soi2.getStructFieldData(o2,
            fields2.get(i)), fields2.get(i).getFieldObjectInspector(),
            mapEqualComparer, nullValueOpt);
        if (r != 0) {
          return r;
        }
      }
      return fields1.size() - fields2.size();
    }
    case LIST: {
      ListObjectInspector loi1 = (ListObjectInspector) oi1;
      ListObjectInspector loi2 = (ListObjectInspector) oi2;
      int minimum = Math.min(loi1.getListLength(o1), loi2.getListLength(o2));
      for (int i = 0; i < minimum; i++) {
        int r = compare(loi1.getListElement(o1, i), loi1
            .getListElementObjectInspector(), loi2.getListElement(o2, i), loi2
            .getListElementObjectInspector(),
            mapEqualComparer, nullValueOpt);
        if (r != 0) {
          return r;
        }
      }
      return loi1.getListLength(o1) - loi2.getListLength(o2);
    }
    case MAP: {
      if (mapEqualComparer == null) {
        throw new RuntimeException("Compare on map type not supported!");
      } else {
        return mapEqualComparer.compare(o1, (MapObjectInspector)oi1, o2, (MapObjectInspector)oi2);
      }
    }
    case UNION: {
      UnionObjectInspector uoi1 = (UnionObjectInspector) oi1;
      UnionObjectInspector uoi2 = (UnionObjectInspector) oi2;
      byte tag1 = uoi1.getTag(o1);
      byte tag2 = uoi2.getTag(o2);
      if (tag1 != tag2) {
        return tag1 - tag2;
      }
      return compare(uoi1.getField(o1),
          uoi1.getObjectInspectors().get(tag1),
          uoi2.getField(o2), uoi2.getObjectInspectors().get(tag2),
          mapEqualComparer, nullValueOpt);
    }
    default:
      throw new RuntimeException("Compare on unknown type: "
          + oi1.getCategory());
    }
  }

  /**
   * Get the list of field names as csv from a StructObjectInspector.
   */
  public static String getFieldNames(StructObjectInspector soi) {
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(fields.get(i).getFieldName());
    }
    return sb.toString();
  }

  /**
   * Get the list of field type as csv from a StructObjectInspector.
   */
  public static String getFieldTypes(StructObjectInspector soi) {
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        sb.append(":");
      }
      sb.append(TypeInfoUtils.getTypeInfoFromObjectInspector(
          fields.get(i).getFieldObjectInspector()).getTypeName());
    }
    return sb.toString();
  }

  /**
   * Get the type name of the Java class.
   */
  public static String getTypeNameFromJavaClass(Type t) {
    try {
      ObjectInspector oi = ObjectInspectorFactory.getReflectionObjectInspector(t,
          ObjectInspectorOptions.JAVA);
      return oi.getTypeName();
    } catch (Throwable e) {
      LOG.info(StringUtils.stringifyException(e));
      return "unknown";
    }
  }

  /**
   * Compares two types identified by the given object inspectors. This method
   * compares the types as follows:
   * <ol>
   * <li>If the given inspectors do not belong to same category, the result is
   * negative.</li>
   * <li>If the given inspectors are for <code>PRIMITIVE</code> type, the result
   * is the comparison of their type names.</li>
   * <li>If the given inspectors are for <code>LIST</code> type, then the result
   * is recursive call to compare the type of list elements.</li>
   * <li>If the given inspectors are <code>MAP</code> type, then the result is a
   * recursive call to compare the map key and value types.</li>
   * <li>If the given inspectors are <code>STRUCT</code> type, then the result
   * is negative if they do not have the same number of fields. If they do have
   * the same number of fields, the result is a recursive call to compare each
   * of the field types.</li>
   * <li>If none of the above, the result is negative.</li>
   * </ol>
   * @param o1
   * @param o2
   * @return true if the given object inspectors represent the same types.
   */
  public static boolean compareTypes(ObjectInspector o1, ObjectInspector o2) {
    Category c1 = o1.getCategory();
    Category c2 = o2.getCategory();

    // Return false if categories are not equal
    if (!c1.equals(c2)) {
      return false;
    }

    // If both categories are primitive return the comparison of type names.
    if (c1.equals(Category.PRIMITIVE)) {
      return o1.getTypeName().equals(o2.getTypeName());
    }

    // If lists, recursively compare the list element types
    if (c1.equals(Category.LIST)) {
      ObjectInspector child1 =
        ((ListObjectInspector) o1).getListElementObjectInspector();
      ObjectInspector child2 =
        ((ListObjectInspector) o2).getListElementObjectInspector();
      return compareTypes(child1, child2);
    }

    // If maps, recursively compare the key and value types
    if (c1.equals(Category.MAP)) {
      MapObjectInspector mapOI1 = (MapObjectInspector) o1;
      MapObjectInspector mapOI2 = (MapObjectInspector) o2;

      ObjectInspector childKey1 = mapOI1.getMapKeyObjectInspector();
      ObjectInspector childKey2 = mapOI2.getMapKeyObjectInspector();

      if (compareTypes(childKey1, childKey2)) {
        ObjectInspector childVal1 = mapOI1.getMapValueObjectInspector();
        ObjectInspector childVal2 = mapOI2.getMapValueObjectInspector();

        if (compareTypes(childVal1, childVal2)) {
          return true;
        }
      }

      return false;
    }

    // If structs, recursively compare the fields
    if (c1.equals(Category.STRUCT)) {
      StructObjectInspector structOI1 = (StructObjectInspector) o1;
      StructObjectInspector structOI2 = (StructObjectInspector) o2;

      List<? extends StructField> childFieldsList1
        = structOI1.getAllStructFieldRefs();
      List<? extends StructField> childFieldsList2
        = structOI2.getAllStructFieldRefs();

      if (childFieldsList1 == null && childFieldsList2 == null) {
        return true;
      } else if (childFieldsList1 == null || childFieldsList2 == null) {
        return false;
      } else if (childFieldsList1.size() != childFieldsList2.size()) {
        return false;
      }

      Iterator<? extends StructField> it1 = childFieldsList1.iterator();
      Iterator<? extends StructField> it2 = childFieldsList2.iterator();
      while (it1.hasNext()) {
        StructField field1 = it1.next();
        StructField field2 = it2.next();

        if (!compareTypes(field1.getFieldObjectInspector(),
              field2.getFieldObjectInspector())) {
          return false;
        }
      }

      return true;
    }

    if (c1.equals(Category.UNION)) {
      UnionObjectInspector uoi1 = (UnionObjectInspector) o1;
      UnionObjectInspector uoi2 = (UnionObjectInspector) o2;
      List<ObjectInspector> ois1 = uoi1.getObjectInspectors();
      List<ObjectInspector> ois2 = uoi2.getObjectInspectors();

      if (ois1 == null && ois2 == null) {
        return true;
      } else if (ois1 == null || ois2 == null) {
        return false;
      } else if (ois1.size() != ois2.size()) {
        return false;
      }
      Iterator<? extends ObjectInspector> it1 = ois1.iterator();
      Iterator<? extends ObjectInspector> it2 = ois2.iterator();
      while (it1.hasNext()) {
        if (!compareTypes(it1.next(), it2.next())) {
          return false;
        }
      }
      return true;
    }

    // Unknown category
    throw new RuntimeException("Unknown category encountered: " + c1);
  }

  public static ConstantObjectInspector getConstantObjectInspector(ObjectInspector oi, Object value) {
    if (oi instanceof ConstantObjectInspector) {
      return (ConstantObjectInspector) oi;
    }
    ObjectInspector writableOI = getStandardObjectInspector(oi, ObjectInspectorCopyOption.WRITABLE);
    Object writableValue = value == null ? value :
      ObjectInspectorConverters.getConverter(oi, writableOI).convert(value);
    switch (writableOI.getCategory()) {
      case PRIMITIVE:
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
            poi.getTypeInfo(), writableValue);
      case LIST:
        ListObjectInspector loi = (ListObjectInspector) oi;
        return ObjectInspectorFactory.getStandardConstantListObjectInspector(
            getStandardObjectInspector(
              loi.getListElementObjectInspector(),
              ObjectInspectorCopyOption.WRITABLE
            ),
            (List<?>)writableValue);
      case MAP:
        MapObjectInspector moi = (MapObjectInspector) oi;
        return ObjectInspectorFactory.getStandardConstantMapObjectInspector(
            getStandardObjectInspector(
              moi.getMapKeyObjectInspector(),
              ObjectInspectorCopyOption.WRITABLE
            ),
            getStandardObjectInspector(
              moi.getMapValueObjectInspector(),
              ObjectInspectorCopyOption.WRITABLE
            ),
            (Map<?, ?>)writableValue);
      case STRUCT:
          StructObjectInspector soi = (StructObjectInspector) oi;
          List<? extends StructField> fields = soi.getAllStructFieldRefs();
          List<String> fieldNames = new ArrayList<String>(fields.size());
          List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(
            fields.size());
          for (StructField f : fields) {
            fieldNames.add(f.getFieldName());
            fieldObjectInspectors.add(getStandardObjectInspector(f
            .getFieldObjectInspector(), ObjectInspectorCopyOption.WRITABLE));
          }
          if (value != null && (writableValue.getClass().isArray())) {
            writableValue = java.util.Arrays.asList((Object[])writableValue);
          }
          return ObjectInspectorFactory.getStandardConstantStructObjectInspector(
            fieldNames,
            fieldObjectInspectors,
            (List<?>)writableValue);
      default:
       throw new IllegalArgumentException(
           writableOI.getCategory() + " not yet supported for constant OI");
    }
  }

  public static Object getWritableConstantValue(ObjectInspector oi) {
    return ((ConstantObjectInspector)oi).getWritableConstantValue();
  }

  public static boolean supportsConstantObjectInspector(ObjectInspector oi) {
    switch (oi.getCategory()) {
      case PRIMITIVE:
      case LIST:
      case MAP:
      case STRUCT:
        return true;
      default:
        return false;
    }
  }

  public static boolean isConstantObjectInspector(ObjectInspector oi) {
    return (oi instanceof ConstantObjectInspector);
  }

  private static boolean setOISettablePropertiesMap(ObjectInspector oi,
      Map<ObjectInspector, Boolean> oiSettableProperties, boolean value) {
    // Cache if the client asks for it, else just return the value
    if (!(oiSettableProperties == null)) {
      oiSettableProperties.put(oi, value);
    }
    return value;
  }

  private static boolean isInstanceOfSettablePrimitiveOI(PrimitiveObjectInspector oi) {
    switch (oi.getPrimitiveCategory()) {
    case BOOLEAN:
      return oi instanceof SettableBooleanObjectInspector;
    case BYTE:
      return oi instanceof SettableByteObjectInspector;
    case SHORT:
      return oi instanceof SettableShortObjectInspector;
    case INT:
      return oi instanceof SettableIntObjectInspector;
    case LONG:
      return oi instanceof SettableLongObjectInspector ;
    case FLOAT:
      return oi instanceof SettableFloatObjectInspector;
    case DOUBLE:
      return oi instanceof SettableDoubleObjectInspector;
    case STRING:
      return oi instanceof WritableStringObjectInspector ||
          oi instanceof JavaStringObjectInspector;
    case CHAR:
      return oi instanceof SettableHiveCharObjectInspector;
    case VARCHAR:
      return oi instanceof SettableHiveVarcharObjectInspector;
    case DATE:
      return oi instanceof SettableDateObjectInspector;
    case TIMESTAMP:
      return oi instanceof SettableTimestampObjectInspector;
    case TIMESTAMPLOCALTZ:
      return oi instanceof SettableTimestampLocalTZObjectInspector;
    case INTERVAL_YEAR_MONTH:
      return oi instanceof SettableHiveIntervalYearMonthObjectInspector;
    case INTERVAL_DAY_TIME:
      return oi instanceof SettableHiveIntervalDayTimeObjectInspector;
    case BINARY:
      return oi instanceof SettableBinaryObjectInspector;
    case DECIMAL:
      return oi instanceof SettableHiveDecimalObjectInspector;
    default:
      throw new RuntimeException("Hive internal error inside isAssignableFromSettablePrimitiveOI "
                          + oi.getTypeName() + " not supported yet.");
    }
  }

  private static boolean isInstanceOfSettableOI(ObjectInspector oi)
  {
    switch (oi.getCategory()) {
      case PRIMITIVE:
        return isInstanceOfSettablePrimitiveOI((PrimitiveObjectInspector)oi);
      case STRUCT:
        return oi instanceof SettableStructObjectInspector;
      case LIST:
        return oi instanceof SettableListObjectInspector;
      case MAP:
        return oi instanceof SettableMapObjectInspector;
      case UNION:
        return oi instanceof SettableUnionObjectInspector;
      default:
        throw new RuntimeException("Hive internal error inside isAssignableFromSettableOI : "
            + oi.getTypeName() + " not supported yet.");
    }
  }

  /*
   * hasAllFieldsSettable without any caching.
   */
  public static Boolean hasAllFieldsSettable(ObjectInspector oi) {
    return hasAllFieldsSettable(oi, null);
  }

  /**
   *
   * @param oi - Input object inspector
   * @param oiSettableProperties - Lookup map to cache the result.(If no caching, pass null)
   * @return - true if : (1) oi is an instance of settable&lt;DataType&gt;OI.
   *                     (2) All the embedded object inspectors are instances of settable&lt;DataType&gt;OI.
   *           If (1) or (2) is false, return false.
   */
  public static boolean hasAllFieldsSettable(ObjectInspector oi,
      Map<ObjectInspector, Boolean> oiSettableProperties) {
    // If the result is already present in the cache, return it.
    if (!(oiSettableProperties == null) &&
        oiSettableProperties.containsKey(oi)) {
      return oiSettableProperties.get(oi).booleanValue();
    }
    // If the top-level object inspector is non-settable return false
    if (!(isInstanceOfSettableOI(oi))) {
      return setOISettablePropertiesMap(oi, oiSettableProperties, false);
    }

    Boolean returnValue = true;

    switch (oi.getCategory()) {
    case PRIMITIVE:
      break;
    case STRUCT:
      StructObjectInspector structOutputOI = (StructObjectInspector) oi;
      List<? extends StructField> listFields = structOutputOI.getAllStructFieldRefs();
      for (StructField listField : listFields) {
        if (!hasAllFieldsSettable(listField.getFieldObjectInspector(), oiSettableProperties)) {
          returnValue = false;
          break;
        }
      }
      break;
    case LIST:
      ListObjectInspector listOutputOI = (ListObjectInspector) oi;
      returnValue = hasAllFieldsSettable(listOutputOI.getListElementObjectInspector(),
          oiSettableProperties);
      break;
    case MAP:
      MapObjectInspector mapOutputOI = (MapObjectInspector) oi;
      returnValue = hasAllFieldsSettable(mapOutputOI.getMapKeyObjectInspector(), oiSettableProperties) &&
          hasAllFieldsSettable(mapOutputOI.getMapValueObjectInspector(), oiSettableProperties);
      break;
    case UNION:
      UnionObjectInspector unionOutputOI = (UnionObjectInspector) oi;
      List<ObjectInspector> unionListFields = unionOutputOI.getObjectInspectors();
      for (ObjectInspector listField : unionListFields) {
        if (!hasAllFieldsSettable(listField, oiSettableProperties)) {
          returnValue = false;
          break;
        }
      }
      break;
    default:
      throw new RuntimeException("Hive internal error inside hasAllFieldsSettable : "
          + oi.getTypeName() + " not supported yet.");
    }
    return setOISettablePropertiesMap(oi, oiSettableProperties, returnValue);
  }

  private ObjectInspectorUtils() {
    // prevent instantiation
  }
}
