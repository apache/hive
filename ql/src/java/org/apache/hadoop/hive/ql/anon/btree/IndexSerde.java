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

package org.apache.hadoop.hive.ql.anon.btree;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.anon.ex.BtreeException;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SchemaInference;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.*;

public class IndexSerde extends AbstractSerDe implements SchemaInference {

  private static final Logger LOG = LoggerFactory.getLogger(IndexSerde.class);
  private ObjectInspector inspector = null;
  private Configuration conf;

  public IndexSerde() {
    LOG.info("constructor");
  }

  @Override
  public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties) throws SerDeException {
    super.initialize(configuration, tableProperties, partitionProperties);
    conf = configuration;

    List<String> columnNames = getColumnNames();
    List<TypeInfo> columnTypes = getColumnTypes();

    StructTypeInfo completeTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    inspector = new BtreeObjectInspector(completeTypeInfo);
  }

  @Override
  public Writable serialize(final Object obj, final ObjectInspector objInspector) throws SerDeException {
    if (!(objInspector instanceof StructObjectInspector)) {
      throw new BtreeException("bad soi");
    }
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<?> lst = getList(obj);

    KeyValueStruct struct = new KeyValueStruct();

    List<? extends StructField> lstFields = soi.getAllStructFieldRefs();
    StructField field0 = lstFields.get(0);
    StructField field1 = lstFields.get(1);
    struct.setKey(getKey(field0.getFieldObjectInspector(), lst.get(0)));
    struct.setValue(getValue(field1.getFieldObjectInspector(), lst.get(1)));

    return struct;
  }

  private List getList(final Object obj) {
    if (obj instanceof ArrayList) {
      return (ArrayList) obj;
    } else if (obj instanceof Object[]) {
      Object[] arr = (Object[]) obj;
      return new ArrayList(Arrays.asList(arr));
    } else {
      throw new BtreeException("ser");
    }
  }

  private WritableComparable getKey(final ObjectInspector oi, final Object rawItem) {
    ObjectInspector.Category category = oi.getCategory();
    WritableComparable key;
    if (Objects.requireNonNull(category) == ObjectInspector.Category.PRIMITIVE) {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      switch (poi.getPrimitiveCategory()) {
        case INT: {
          IntObjectInspector ioi = (IntObjectInspector) poi;
          int value = ioi.get(rawItem);
          key = new IntWritable(value);
          break;
        }
        case LONG: {
          LongObjectInspector loi = (LongObjectInspector) poi;
          long value = loi.get(rawItem);
          key = new LongWritable(value);
          break;
        }
        case STRING: {
          StringObjectInspector stringObjectInspector = (StringObjectInspector) poi;
          String value = stringObjectInspector.getPrimitiveJavaObject(rawItem);
          key = new Text(value);
          break;
        }
        default: {
          throw new BtreeException("unhandled primitive type: " + category);
        }
      }
      String type = getType(poi);
      conf.set(INDEX_KEY_TYPE, type);
    } else {
      throw new BtreeException("unhandled primitive type: " + category);
    }
    return key;
  }

  private StructValueList getValue(final ObjectInspector oi, final Object rawItem) {
    StructValueList value = new StructValueList();
    ObjectInspector.Category category = oi.getCategory();
    if (Objects.requireNonNull(category) != ObjectInspector.Category.LIST) {
      throw new BtreeException("unhandled category: " + category);
    }

    ListObjectInspector loi = (ListObjectInspector) oi;
    int listSize = loi.getListLength(rawItem);
    ObjectInspector elementObjectInspector = loi.getListElementObjectInspector();
    StructObjectInspector soi = (StructObjectInspector) elementObjectInspector;
    String types = getValueTypes(soi);
    conf.set(INDEX_VALUE_TYPES, types);

    for (int i = 0; i < listSize; i++) {
      Object element = loi.getListElement(rawItem, i);
      List<? extends StructField> lstElemStructFields = soi.getAllStructFieldRefs();
      ValueItem valueItem = new ValueItem();
      for (StructField structField : lstElemStructFields) {
        Object structFieldData = soi.getStructFieldData(element, structField);
        switch (structField.getFieldID()) {
          case 0: {
            valueItem.filePath = (Writable) structFieldData;
            break;
          }
          case 1: {
            getLocatorSchemaItem(structField, structFieldData, valueItem);
            break;
          }
          default: {
            throw new BtreeException("unhandled structField: " + structField);
          }
        }
      }
      value.add(valueItem);
    }
    return value;
  }

  private void getLocatorSchemaItem(final StructField structField, final Object structFieldData, final ValueItem valueItem) {
    ObjectInspector foi = structField.getFieldObjectInspector();
    ListObjectInspector loi = (ListObjectInspector) foi;
    ObjectInspector eoi = loi.getListElementObjectInspector();
    int listSize = loi.getListLength(structFieldData);
    StructObjectInspector soi = (StructObjectInspector) eoi;

    if (listSize == 0) {
      return;
    }

    for (int i = 0; i < listSize; i++) {
      Object element = loi.getListElement(structFieldData, i);
      LocatorSchemaItem locatorSchemaItem = new LocatorSchemaItem();
      List<? extends StructField> structFields = soi.getAllStructFieldRefs();
      for (StructField structField2 : structFields) {
        Object fieldData = soi.getStructFieldData(element, structField2);
        switch (structField2.getFieldID()) {
          case 0: {
            locatorSchemaItem.rowLocator = (Writable) fieldData;
            break;
          }
          case 1: {
            locatorSchemaItem.schemaId = (Writable) fieldData;
            break;
          }
        }
      }
      valueItem.add(locatorSchemaItem);
    }
  }

  private String getValueTypes(final StructObjectInspector soi) {
    List<String> valueTypes = new ArrayList<>();
    List<? extends StructField> lstStructFields = soi.getAllStructFieldRefs();

    for (StructField structField : lstStructFields) {
      String type = getType(structField.getFieldObjectInspector());
      valueTypes.add(type);
    }

    return String.join("", valueTypes);
  }

  private String getType(final ObjectInspector oi) {
    ObjectInspector.Category category = oi.getCategory();
    if (category == ObjectInspector.Category.LIST) {
      return getTypeFromList(oi);
    } else if (category == ObjectInspector.Category.PRIMITIVE) {
      return getTypeFromPrimitive(oi);
    }
    throw new BtreeException("unhandled category: " + category);
  }

  private String getTypeFromList(final ObjectInspector oi) {
    ListObjectInspector loi = (ListObjectInspector) oi;
    ObjectInspector eoi = loi.getListElementObjectInspector();
    StructObjectInspector soi = (StructObjectInspector) eoi;
    List<? extends StructField> structFields = soi.getAllStructFieldRefs();

    StringBuilder ret = new StringBuilder();
    for (StructField structField : structFields) {
      String type = getType(structField.getFieldObjectInspector());
      ret.append(type);
    }
    return ret.toString();
  }

  private String getTypeFromPrimitive(final ObjectInspector oi) {
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
    PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = poi.getPrimitiveCategory();
    byte type = 0;
    switch (primitiveCategory) {
      case INT: {
        type = BTREE_INT_TYPE;
        break;
      }
      case LONG: {
        type = BTREE_LONG_TYPE;
        break;
      }
      case STRING: {
        type = BTREE_TEXT_TYPE;
        break;
      }
      case BINARY: {
        type = BTREE_BINARY_TYPE;
        throw new BtreeException("unsupported");
      }
      default: {
        throw new BtreeException("unhandled primitive type: " + primitiveCategory);
      }
    }
    return new String(new byte[]{type});
  }

  @Override
  public Object deserialize(final Writable data) throws SerDeException {
    if (data instanceof KeyValueStruct) {
      return data;
    }
    throw new BtreeException("deser");
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return KeyValueStruct.class;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }

  @Override
  public List<FieldSchema> readSchema(Configuration conf, String file) throws SerDeException {
    return null;
  }
}
