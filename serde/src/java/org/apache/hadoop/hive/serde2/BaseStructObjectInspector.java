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
package org.apache.hadoop.hive.serde2;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseStructObjectInspector extends StructObjectInspector {

  protected static class MyField implements StructField {

    protected final int fieldID;
    protected final String fieldName;
    protected final String fieldComment;
    protected final ObjectInspector fieldObjectInspector;

    public MyField(int fieldID, String fieldName,
                   ObjectInspector fieldObjectInspector, String fieldComment) {
      this.fieldID = fieldID;
      this.fieldName = fieldName.toLowerCase();
      this.fieldObjectInspector = fieldObjectInspector;
      this.fieldComment = fieldComment;
    }

    public MyField(int fieldID, StructField field) {
      this.fieldID = fieldID;
      this.fieldName = field.getFieldName().toLowerCase();
      this.fieldObjectInspector = field.getFieldObjectInspector();
      this.fieldComment = field.getFieldComment();
    }

    public int getFieldID() {
      return fieldID;
    }

    public String getFieldName() {
      return fieldName;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return fieldObjectInspector;
    }

    @Override
    public String getFieldComment() {
      return fieldComment;
    }

    @Override
    public String toString() {
      return fieldID + ":" + fieldName;
    }
  }

  protected final List<MyField> fields = new ArrayList<MyField>();

  protected BaseStructObjectInspector() {
    super();
  }

  /**
   * Call ObjectInspectorFactory.getLazySimpleStructObjectInspector instead.
   */
  public BaseStructObjectInspector(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors) {
    init(structFieldNames, structFieldObjectInspectors, null);
  }

  public BaseStructObjectInspector(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors,
      List<String> structFieldComments) {
    init(structFieldNames, structFieldObjectInspectors, structFieldComments);
  }

  protected void init(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors,
      List<String> structFieldComments) {
    assert (structFieldNames.size() == structFieldObjectInspectors.size());
    assert (structFieldComments == null ||
        (structFieldNames.size() == structFieldComments.size()));

    for (int i = 0; i < structFieldNames.size(); i++) {
      fields.add(createField(i,
          structFieldNames.get(i), structFieldObjectInspectors.get(i),
          structFieldComments == null ? null : structFieldComments.get(i)));
    }
  }

  protected void init(List<StructField> structFields) {
    for (int i = 0; i < structFields.size(); i++) {
      fields.add(new MyField(i, structFields.get(i)));
    }
  }

  // override this for using extended FieldObject
  protected MyField createField(int index, String fieldName, ObjectInspector fieldOI, String comment) {
    return new MyField(index, fieldName, fieldOI, comment);
  }

  @Override
  public String getTypeName() {
    return ObjectInspectorUtils.getStandardStructTypeName(this);
  }

  @Override
  public final Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public StructField getStructFieldRef(String fieldName) {
    return ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields);
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }
}
