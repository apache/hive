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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatContext;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

/**
 * SerDe class for serializing to and from HCatRecord.
 */
@SerDeSpec(schemaProps = { serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES })
public class HCatRecordSerDe extends AbstractSerDe {

  public HCatRecordSerDe() throws SerDeException {
  }

  private StructTypeInfo rowTypeInfo;

  private HCatRecordObjectInspector cachedObjectInspector;

  @Override
  public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties)
      throws SerDeException {
    super.initialize(configuration, tableProperties, partitionProperties);

    log.debug("Initializing HCatRecordSerDe");

    rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(getColumnNames(), getColumnTypes());
    cachedObjectInspector = HCatRecordObjectInspectorFactory.getHCatRecordObjectInspector(rowTypeInfo);
  }

  public void initialize(HCatSchema hsch) throws SerDeException {

    log.debug("Initializing HCatRecordSerDe through HCatSchema {}", hsch);

    rowTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(hsch.getSchemaAsTypeString());
    cachedObjectInspector = HCatRecordObjectInspectorFactory.getHCatRecordObjectInspector(rowTypeInfo);
  }


  /**
   * The purpose of a deserialize method is to turn a data blob
   * which is a writable representation of the data into an
   * object that can then be parsed using the appropriate
   * ObjectInspector. In this case, since HCatRecord is directly
   * already the Writable object, there's no extra work to be done
   * here. Most of the logic resides in the ObjectInspector to be
   * able to return values from within the HCatRecord to hive when
   * it wants it.
   */
  @Override
  public Object deserialize(Writable data) throws SerDeException {
    if (!(data instanceof HCatRecord)) {
      throw new SerDeException(getClass().getName() + ": expects HCatRecord!");
    }

    return data;
  }

  /**
   * The purpose of the serialize method is to turn an object-representation
   * with a provided ObjectInspector into a Writable format, which
   * the underlying layer can then use to write out.
   *
   * In this case, it means that Hive will call this method to convert
   * an object with appropriate objectinspectors that it knows about,
   * to write out a HCatRecord.
   */
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
    throws SerDeException {
    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
        + " can only serialize struct types, but we got: "
        + objInspector.getTypeName());
    }
    return new DefaultHCatRecord((List<Object>) serializeStruct(obj, (StructObjectInspector) objInspector));
  }


  /**
   * Return serialized HCatRecord from an underlying
   * object-representation, and readable by an ObjectInspector
   * @param obj : Underlying object-representation
   * @param soi : StructObjectInspector
   * @return HCatRecord
   */
  private static List<?> serializeStruct(Object obj, StructObjectInspector soi)
    throws SerDeException {

    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(obj);

    if (list == null) {
      return null;
    }

    List<Object> l = new ArrayList<Object>(fields.size());

    if (fields != null) {
      for (int i = 0; i < fields.size(); i++) {

        // Get the field objectInspector and the field object.
        ObjectInspector foi = fields.get(i).getFieldObjectInspector();
        Object f = list.get(i);
        Object res = serializeField(f, foi);
        l.add(i, res);
      }
    }
    return l;
  }

  /**
   * Return underlying Java Object from an object-representation
   * that is readable by a provided ObjectInspector.
   */
  public static Object serializeField(Object field, ObjectInspector fieldObjectInspector)
    throws SerDeException {

    Object res;
    if (fieldObjectInspector.getCategory() == Category.PRIMITIVE) {
      res = serializePrimitiveField(field, fieldObjectInspector);
    } else if (fieldObjectInspector.getCategory() == Category.STRUCT) {
      res = serializeStruct(field, (StructObjectInspector) fieldObjectInspector);
    } else if (fieldObjectInspector.getCategory() == Category.LIST) {
      res = serializeList(field, (ListObjectInspector) fieldObjectInspector);
    } else if (fieldObjectInspector.getCategory() == Category.MAP) {
      res = serializeMap(field, (MapObjectInspector) fieldObjectInspector);
    } else {
      throw new SerDeException(HCatRecordSerDe.class.toString()
        + " does not know what to do with fields of unknown category: "
        + fieldObjectInspector.getCategory() + " , type: " + fieldObjectInspector.getTypeName());
    }
    return res;
  }

  /**
   * Helper method to return underlying Java Map from
   * an object-representation that is readable by a provided
   * MapObjectInspector
   */
  private static Map<?, ?> serializeMap(Object f, MapObjectInspector moi) throws SerDeException {
    ObjectInspector koi = moi.getMapKeyObjectInspector();
    ObjectInspector voi = moi.getMapValueObjectInspector();
    Map<Object, Object> m = new LinkedHashMap<Object, Object>();

    Map<?, ?> readMap = moi.getMap(f);
    if (readMap == null) {
      return null;
    } else {
      for (Map.Entry<?, ?> entry : readMap.entrySet()) {
        m.put(serializeField(entry.getKey(), koi), serializeField(entry.getValue(), voi));
      }
    }
    return m;
  }

  private static List<?> serializeList(Object f, ListObjectInspector loi) throws SerDeException {
    List l = loi.getList(f);
    if (l == null) {
      return null;
    }

    ObjectInspector eloi = loi.getListElementObjectInspector();
    if (eloi.getCategory() == Category.PRIMITIVE) {
      List<Object> list = new ArrayList<Object>(l.size());
      for (int i = 0; i < l.size(); i++) {
        list.add(((PrimitiveObjectInspector) eloi).getPrimitiveJavaObject(l.get(i)));
      }
      return list;
    } else if (eloi.getCategory() == Category.STRUCT) {
      List<List<?>> list = new ArrayList<List<?>>(l.size());
      for (int i = 0; i < l.size(); i++) {
        list.add(serializeStruct(l.get(i), (StructObjectInspector) eloi));
      }
      return list;
    } else if (eloi.getCategory() == Category.LIST) {
      List<List<?>> list = new ArrayList<List<?>>(l.size());
      for (int i = 0; i < l.size(); i++) {
        list.add(serializeList(l.get(i), (ListObjectInspector) eloi));
      }
      return list;
    } else if (eloi.getCategory() == Category.MAP) {
      List<Map<?, ?>> list = new ArrayList<Map<?, ?>>(l.size());
      for (int i = 0; i < l.size(); i++) {
        list.add(serializeMap(l.get(i), (MapObjectInspector) eloi));
      }
      return list;
    } else {
      throw new SerDeException(HCatRecordSerDe.class.toString()
        + " does not know what to do with fields of unknown category: "
        + eloi.getCategory() + " , type: " + eloi.getTypeName());
    }
  }

  private static Object serializePrimitiveField(Object field,
      ObjectInspector fieldObjectInspector) {
    if (field == null) {
      return null;
    }
    Object f = ((PrimitiveObjectInspector) fieldObjectInspector).getPrimitiveJavaObject(field);
    if (f != null && HCatContext.INSTANCE.getConf().isPresent()) {
      Configuration conf = HCatContext.INSTANCE.getConf().get();

      if (f instanceof Boolean &&
        conf.getBoolean(
          HCatConstants.HCAT_DATA_CONVERT_BOOLEAN_TO_INTEGER,
          HCatConstants.HCAT_DATA_CONVERT_BOOLEAN_TO_INTEGER_DEFAULT)) {
        return ((Boolean) f) ? 1 : 0;
      } else if (f instanceof Short &&
        conf.getBoolean(
          HCatConstants.HCAT_DATA_TINY_SMALL_INT_PROMOTION,
          HCatConstants.HCAT_DATA_TINY_SMALL_INT_PROMOTION_DEFAULT)) {
        return Integer.valueOf((Short) f);
      } else if (f instanceof Byte &&
        conf.getBoolean(
          HCatConstants.HCAT_DATA_TINY_SMALL_INT_PROMOTION,
          HCatConstants.HCAT_DATA_TINY_SMALL_INT_PROMOTION_DEFAULT)) {
        return Integer.valueOf((Byte) f);
      }
    }

    return f;
  }

  /**
   * Return an object inspector that can read through the object
   * that we return from deserialize(). To wit, that means we need
   * to return an ObjectInspector that can read HCatRecord, given
   * the type info for it during initialize(). This also means
   * that this method cannot and should not be called before initialize()
   */
  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return HCatRecord.class;
  }

}
