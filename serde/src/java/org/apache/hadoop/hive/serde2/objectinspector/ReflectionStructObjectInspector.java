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
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.util.ReflectionUtils;

/**
 * ReflectionStructObjectInspector works on struct data that is stored as a
 * native Java object. It will drill down into the Java class to get the fields
 * and construct ObjectInspectors for the fields, if they are not specified.
 * 
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects,
 * instead of directly creating an instance of this class.
 * 
 */
public class ReflectionStructObjectInspector extends
    SettableStructObjectInspector {

  /**
   * MyField.
   *
   */
  public static class MyField implements StructField {
    protected int fieldID;
    protected Field field;

    protected ObjectInspector fieldObjectInspector;

    protected MyField() {
      super();
    }

    public MyField(int fieldID, Field field, ObjectInspector fieldObjectInspector) {
      this.fieldID = fieldID;
      this.field = field;
      this.fieldObjectInspector = fieldObjectInspector;
    }

    public String getFieldName() {
      return field.getName().toLowerCase();
    }

    public ObjectInspector getFieldObjectInspector() {
      return fieldObjectInspector;
    }

    public int getFieldID() {
      return fieldID;
    }

    public String getFieldComment() {
      return null;
    }

    @Override
    public String toString() {
      return field.toString();
    }
  }

  Class<?> objectClass;
  List<MyField> fields;
  volatile boolean inited = false;
  volatile Type type;

  public Category getCategory() {
    return Category.STRUCT;
  }

  public String getTypeName() {
    StringBuilder sb = new StringBuilder("struct<");
    boolean first = true;
    for (StructField structField : getAllStructFieldRefs()) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(structField.getFieldName()).append(":")
          .append(structField.getFieldObjectInspector().getTypeName());
    }
    sb.append(">");
    return sb.toString();
  }

  /**
   * This method is only intended to be used by the Utilities class in this
   * package. This creates an uninitialized ObjectInspector so the Utilities
   * class can put it into a cache before it initializes when it might look up
   * the cache for member fields that might be of the same type (e.g. recursive
   * type like linked list and trees).
   */
  ReflectionStructObjectInspector() {
  }

  /**
   * Check if this inspector and all its field inspectors are initialized.
   */
  protected boolean isFullyInited(Set<Type> checkedTypes) {
    if (type != null && // when type is not set, init hasn't been called yet
        ObjectInspectorFactory.objectInspectorCache.asMap().get(type) != this) {
      // This object should be the same as in cache, otherwise, it must be removed due to init error
      throw new RuntimeException("Cached object inspector is gone while waiting for it to initialize");
    }

    if (!inited) {
      return false;
    }

    // We don't want to check types already checked
    checkedTypes.add(type);

    // This inspector is initialized, we still need
    // to check if all field inspectors are initialized
    for (StructField field: getAllStructFieldRefs()) {
      ObjectInspector oi = field.getFieldObjectInspector();
      if (oi instanceof ReflectionStructObjectInspector) {
        ReflectionStructObjectInspector soi = (ReflectionStructObjectInspector) oi;
        if (!checkedTypes.contains(soi.type) && !soi.isFullyInited(checkedTypes)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * This method is only intended to be used by Utilities class in this package.
   * The reason that this method is not recursive by itself is because we want
   * to allow recursive types.
   */
  protected void init(Type type, Class<?> objectClass,
      ObjectInspectorFactory.ObjectInspectorOptions options) {
    this.type = type;

    verifyObjectClassType(objectClass);
    this.objectClass = objectClass;
    final List<? extends ObjectInspector> structFieldObjectInspectors = extractFieldObjectInspectors(objectClass, options);

    Field[] reflectionFields = ObjectInspectorUtils
        .getDeclaredNonStaticFields(objectClass);
    synchronized (this) {
      fields = new ArrayList<MyField>(structFieldObjectInspectors.size());
      int used = 0;
      for (int i = 0; i < reflectionFields.length; i++) {
        if (!shouldIgnoreField(reflectionFields[i].getName())) {
          reflectionFields[i].setAccessible(true);
          fields.add(new MyField(i, reflectionFields[i], structFieldObjectInspectors
              .get(used++)));
        }
      }
      assert (fields.size() == structFieldObjectInspectors.size());
      inited = true;
      notifyAll();
    }
  }

  // ThriftStructObjectInspector will override and ignore __isset fields.
  public boolean shouldIgnoreField(String name) {
    return false;
  }

  // Without Data
  @Override
  public StructField getStructFieldRef(String fieldName) {
    return ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields);
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }

  // With Data
  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data == null) {
      return null;
    }
    if (!(fieldRef instanceof MyField)) {
      throw new RuntimeException("fieldRef has to be of MyField");
    }
    MyField f = (MyField) fieldRef;
    try {
      Object r = f.field.get(data);
      return r;
    } catch (Exception e) {
      throw new RuntimeException("cannot get field " + f.field + " from "
          + data.getClass() + " " + data, e);
    }
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    try {
      ArrayList<Object> result = new ArrayList<Object>(fields.size());
      for (int i = 0; i < fields.size(); i++) {
        result.add(fields.get(i).field.get(data));
      }
      return result;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object create() {
    return ReflectionUtils.newInstance(objectClass, null);
  }

  @Override
  public Object setStructFieldData(Object struct, StructField field,
      Object fieldValue) {
    MyField myField = (MyField) field;
    try {
      myField.field.set(struct, fieldValue);
    } catch (Exception e) {
      throw new RuntimeException("cannot set field " + myField.field + " of "
          + struct.getClass() + " " + struct, e);
    }
    return struct;
  }

  protected List<? extends ObjectInspector> extractFieldObjectInspectors(Class<?> clazz,
    ObjectInspectorFactory.ObjectInspectorOptions options) {
    Field[] fields = ObjectInspectorUtils.getDeclaredNonStaticFields(clazz);
    ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>(
      fields.length);
    for (int i = 0; i < fields.length; i++) {
      if (!shouldIgnoreField(fields[i].getName())) {
        structFieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(fields[i]
          .getGenericType(), options, false));
      }
    }
    return structFieldObjectInspectors;
  }


  protected void verifyObjectClassType(Class<?> objectClass) {
    assert (!List.class.isAssignableFrom(objectClass));
    assert (!Map.class.isAssignableFrom(objectClass));
  }
}
