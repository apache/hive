/**
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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;

import com.google.common.annotations.VisibleForTesting;

@Description(
    name = "extract_union",
    value = "_FUNC_(union[, tag])" + " - Recursively explodes unions into structs or simply extracts the given tag.",
    extended = "  > SELECT _FUNC_({0:\"foo\"}).tag_0 FROM src;\n  foo\n"
        + "  > SELECT _FUNC_({0:\"foo\"}).tag_1 FROM src;\n  null\n"
        + "  > SELECT _FUNC_({0:\"foo\"}, 0) FROM src;\n  foo\n"
        + "  > SELECT _FUNC_({0:\"foo\"}, 1) FROM src;\n  null")
public class GenericUDFExtractUnion extends GenericUDF {

  private static final int ALL_TAGS = -1;

  private final ObjectInspectorConverter objectInspectorConverter;
  private final ValueConverter valueConverter;

  private int tag = ALL_TAGS;
  private UnionObjectInspector unionOI;
  private ObjectInspector sourceOI;

  public GenericUDFExtractUnion() {
    this(new ObjectInspectorConverter(), new ValueConverter());
  }

  @VisibleForTesting
  GenericUDFExtractUnion(ObjectInspectorConverter objectInspectorConverter, ValueConverter valueConverter) {
    this.objectInspectorConverter = objectInspectorConverter;
    this.valueConverter = valueConverter;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length == 1) {
      sourceOI = arguments[0];
      return objectInspectorConverter.convert(sourceOI);
    }

    if (arguments.length == 2 && (arguments[0] instanceof UnionObjectInspector)
        && (arguments[1] instanceof WritableConstantIntObjectInspector)) {
      tag = ((WritableConstantIntObjectInspector) arguments[1]).getWritableConstantValue().get();
      unionOI = (UnionObjectInspector) arguments[0];
      List<ObjectInspector> fieldOIs = ((UnionObjectInspector) arguments[0]).getObjectInspectors();
      if (tag < 0 || tag >= fieldOIs.size()) {
        throw new UDFArgumentException(
            "int constant must be a valid union tag for " + unionOI.getTypeName() + ". Expected 0-"
                + (fieldOIs.size() - 1) + " got: " + tag);
      }
      return fieldOIs.get(tag);
    }

    String argumentTypes = "nothing";
    if (arguments.length > 0) {
      List<String> typeNames = new ArrayList<>();
      for (ObjectInspector oi : arguments) {
        typeNames.add(oi.getTypeName());
      }
      argumentTypes = typeNames.toString();
    }
    throw new UDFArgumentException(
        "Unsupported arguments. Expected a type containing a union or a union and an int constant, got: "
            + argumentTypes);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object value = arguments[0].get();
    if (tag == ALL_TAGS) {
      return valueConverter.convert(value, sourceOI);
    } else if (tag == unionOI.getTag(value)) {
      return unionOI.getField(value);
    } else {
      return null;
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("extract_union(");
    for (int i = 0; i < children.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(children[i]);
    }
    sb.append(')');
    return sb.toString();
  }

  static class ObjectInspectorConverter {

    private static final String TAG_FIELD_PREFIX = "tag_";

    ObjectInspector convert(ObjectInspector inspector) {
      AtomicBoolean foundUnion = new AtomicBoolean(false);
      ObjectInspector result = convert(inspector, foundUnion);
      if (!foundUnion.get()) {
        throw new IllegalArgumentException("No unions found in " + inspector.getTypeName());
      }
      return result;
    }

    private ObjectInspector convert(ObjectInspector inspector, AtomicBoolean foundUnion) {
      Category category = inspector.getCategory();
      switch (category) {
      case PRIMITIVE:
        return inspector;
      case LIST:
        return convertList(inspector, foundUnion);
      case MAP:
        return convertMap(inspector, foundUnion);
      case STRUCT:
        return convertStruct(inspector, foundUnion);
      case UNION:
        foundUnion.set(true);
        return convertUnion(inspector, foundUnion);
      default:
        throw new IllegalStateException("Unknown category: " + category);
      }
    }

    private ObjectInspector convertList(ObjectInspector inspector, AtomicBoolean foundUnion) {
      ListObjectInspector listOI = (ListObjectInspector) inspector;
      ObjectInspector elementOI = convert(listOI.getListElementObjectInspector(), foundUnion);
      return ObjectInspectorFactory.getStandardListObjectInspector(elementOI);
    }

    private ObjectInspector convertMap(ObjectInspector inspector, AtomicBoolean foundUnion) {
      MapObjectInspector mapOI = (MapObjectInspector) inspector;
      ObjectInspector keyOI = convert(mapOI.getMapKeyObjectInspector(), foundUnion);
      ObjectInspector valueOI = convert(mapOI.getMapValueObjectInspector(), foundUnion);
      return ObjectInspectorFactory.getStandardMapObjectInspector(keyOI, valueOI);
    }

    private ObjectInspector convertStruct(ObjectInspector inspector, AtomicBoolean foundUnion) {
      StructObjectInspector structOI = (StructObjectInspector) inspector;
      List<? extends StructField> fields = structOI.getAllStructFieldRefs();
      List<String> names = new ArrayList<>(fields.size());
      List<ObjectInspector> inspectors = new ArrayList<>(fields.size());
      for (StructField field : fields) {
        names.add(field.getFieldName());
        inspectors.add(convert(field.getFieldObjectInspector(), foundUnion));
      }
      return ObjectInspectorFactory.getStandardStructObjectInspector(names, inspectors);
    }

    private ObjectInspector convertUnion(ObjectInspector inspector, AtomicBoolean foundUnion) {
      UnionObjectInspector unionOI = (UnionObjectInspector) inspector;
      List<ObjectInspector> fieldOIs = unionOI.getObjectInspectors();
      int tags = fieldOIs.size();
      List<String> names = new ArrayList<>(tags);
      List<ObjectInspector> inspectors = new ArrayList<>(tags);
      for (int i = 0; i < tags; i++) {
        names.add(TAG_FIELD_PREFIX + i);
        inspectors.add(convert(fieldOIs.get(i), foundUnion));
      }
      return ObjectInspectorFactory.getStandardStructObjectInspector(names, inspectors);
    }

  }

  static class ValueConverter {

    Object convert(Object value, ObjectInspector inspector) {
      Category category = inspector.getCategory();
      switch (category) {
      case PRIMITIVE:
        return value;
      case LIST:
        return convertList(value, inspector);
      case MAP:
        return convertMap(value, inspector);
      case STRUCT:
        return convertStruct(value, inspector);
      case UNION:
        return convertUnion(value, inspector);
      default:
        throw new IllegalStateException("Unknown category: " + category);
      }
    }

    private Object convertList(Object list, ObjectInspector inspector) {
      SettableListObjectInspector listOI = (SettableListObjectInspector) inspector;
      int size = listOI.getListLength(list);
      Object result = listOI.create(size);
      for (int i = 0; i < size; i++) {
        listOI.set(result, i, convert(listOI.getListElement(list, i), listOI.getListElementObjectInspector()));
      }
      return result;
    }

    private Object convertMap(Object map, ObjectInspector inspector) {
      SettableMapObjectInspector mapOI = (SettableMapObjectInspector) inspector;
      Object result = mapOI.create();
      for (Object key : mapOI.getMap(map).keySet()) {
        Object value = mapOI.getMapValueElement(map, key);
        mapOI.put(
            result,
            convert(key, mapOI.getMapKeyObjectInspector()),
            convert(value, mapOI.getMapValueObjectInspector()));
      }
      return result;
    }

    private Object convertStruct(Object struct, ObjectInspector inspector) {
      SettableStructObjectInspector structOI = (SettableStructObjectInspector) inspector;
      Object result = structOI.create();
      for (StructField field : structOI.getAllStructFieldRefs()) {
        Object value = structOI.getStructFieldData(struct, field);
        structOI.setStructFieldData(result, field, convert(value, field.getFieldObjectInspector()));
      }
      return result;
    }

    private Object convertUnion(Object union, ObjectInspector inspector) {
      UnionObjectInspector unionOI = (UnionObjectInspector) inspector;
      List<ObjectInspector> childOIs = unionOI.getObjectInspectors();
      byte tag = unionOI.getTag(union);
      Object value = unionOI.getField(union);
      List<Object> result = new ArrayList<>(childOIs.size());
      for (int i = 0; i < childOIs.size(); i++) {
        if (i == tag) {
          result.add(convert(value, childOIs.get(i)));
        } else {
          result.add(null);
        }
      }
      return result;
    }
  }

}
