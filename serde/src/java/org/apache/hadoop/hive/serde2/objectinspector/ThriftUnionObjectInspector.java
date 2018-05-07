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

import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TUnion;
import org.apache.thrift.meta_data.FieldMetaData;

import com.google.common.primitives.UnsignedBytes;

/**
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects,
 * instead of directly creating an instance of this class.
 */
@SuppressWarnings("unchecked")
public class ThriftUnionObjectInspector extends ReflectionStructObjectInspector implements UnionObjectInspector {

  private static final String FIELD_METADATA_MAP = "metaDataMap";
  private List<ObjectInspector> ois;
  private List<StandardStructObjectInspector.MyField> fields;

  @Override
  public boolean shouldIgnoreField(String name) {
    return name.startsWith("__isset");
  }

  @Override
  public List<ObjectInspector> getObjectInspectors() {
    return ois;
  }

  @Override
  public byte getTag(final Object o) {
    if (o == null) {
      return -1;
    }
    final TFieldIdEnum setField = ((TUnion<? extends TUnion<?, ?>, ? extends TFieldIdEnum>) o).getSetField();
    return UnsignedBytes.checkedCast((setField.getThriftFieldId() - 1));
  }

  @Override
  public Object getField(final Object o) {
    if (o == null) {
      return null;
    }
    return ((TUnion<? extends TUnion<?, ?>, ? extends TFieldIdEnum>) o).getFieldValue();
  }

  /**
   * This method is only intended to be used by Utilities class in this package.
   * The reason that this method is not recursive by itself is because we want
   * to allow recursive types.
   */
  @Override
  protected void init(Type type, Class<?> objectClass,
                      ObjectInspectorFactory.ObjectInspectorOptions options) {
    this.type = type;

    verifyObjectClassType(objectClass);
    this.objectClass = objectClass;
    final Field fieldMetaData;

    try {
      fieldMetaData = objectClass.getDeclaredField(FIELD_METADATA_MAP);
      assert(Map.class.isAssignableFrom(fieldMetaData.getType()));
      fieldMetaData.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("Unable to find field metadata for thrift union field " , e);
    }

    try {
      final Map<? extends TFieldIdEnum, FieldMetaData> fieldMap = (Map<? extends TFieldIdEnum, FieldMetaData>) fieldMetaData.get(null);
      synchronized (this) {
        fields = new ArrayList<StandardStructObjectInspector.MyField>(fieldMap.size());
        this.ois = new ArrayList<ObjectInspector>();
        for(Map.Entry<? extends TFieldIdEnum, FieldMetaData> metadata : fieldMap.entrySet()) {
          int fieldId = metadata.getKey().getThriftFieldId();
          String fieldName = metadata.getValue().fieldName;
          final Type fieldType = ThriftObjectInspectorUtils.getFieldType(objectClass, fieldName);
          final ObjectInspector reflectionObjectInspector = ObjectInspectorFactory.getReflectionObjectInspector(fieldType, options, false);
          fields.add(new StandardStructObjectInspector.MyField(fieldId, fieldName, reflectionObjectInspector));
          this.ois.add(reflectionObjectInspector);
        }
        inited = true;
      }
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Unable to find field metadata for thrift union field ", e);
    }
  }

  @Override
  public Category getCategory() {
    return Category.UNION;
  }

  @Override
  public synchronized List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }

  public String getTypeName() {
    return ObjectInspectorUtils.getStandardUnionTypeName(this);
  }
}

