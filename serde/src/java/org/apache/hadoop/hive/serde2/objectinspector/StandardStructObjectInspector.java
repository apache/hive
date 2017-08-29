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

package org.apache.hadoop.hive.serde2.objectinspector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ListStructObjectInspector works on struct data that is stored as a Java List
 * or Java Array object. Basically, the fields are stored sequentially in the
 * List object.
 *
 * The names of the struct fields and the internal structure of the struct
 * fields are specified in the ctor of the StructObjectInspector.
 *
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects,
 * instead of directly creating an instance of this class.
 */
public class StandardStructObjectInspector extends
    SettableStructObjectInspector {

  public static final Logger LOG = LoggerFactory
      .getLogger(StandardStructObjectInspector.class.getName());

  protected static class MyField implements StructField {
    protected int fieldID;
    protected String fieldName;
    protected ObjectInspector fieldObjectInspector;
    protected String fieldComment;

    protected MyField() {
      super();
    }
    
    public MyField(int fieldID, String fieldName,
        ObjectInspector fieldObjectInspector) {
      this.fieldID = fieldID;
      this.fieldName = fieldName.toLowerCase().intern();
      this.fieldObjectInspector = fieldObjectInspector;
    }

    public MyField(int fieldID, String fieldName,
        ObjectInspector fieldObjectInspector, String fieldComment) {
      this(fieldID, fieldName, fieldObjectInspector);
      this.fieldComment = fieldComment;
    }

    public int getFieldID() {
      return fieldID;
    }

    public String getFieldName() {
      return fieldName;
    }

    public ObjectInspector getFieldObjectInspector() {
      return fieldObjectInspector;
    }

    public String getFieldComment() {
      return fieldComment;
    }

    @Override
    public String toString() {
      return "" + fieldID + ":" + fieldName;
    }
  }

  protected List<MyField> fields;

  protected transient List<String> originalColumnNames;

  protected StandardStructObjectInspector() {
    super();
  }
  /**
   * Call ObjectInspectorFactory.getStandardListObjectInspector instead.
   */
  protected StandardStructObjectInspector(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors) {
    init(structFieldNames, structFieldObjectInspectors, null);
  }

  /**
  * Call ObjectInspectorFactory.getStandardListObjectInspector instead.
  */
  protected StandardStructObjectInspector(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors,
      List<String> structFieldComments) {
    init(structFieldNames, structFieldObjectInspectors, structFieldComments);
  }

  protected void init(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors,
      List<String> structFieldComments) {

    fields = new ArrayList<MyField>(structFieldNames.size());
    originalColumnNames = new ArrayList<String>(structFieldNames.size());
    for (int i = 0; i < structFieldNames.size(); i++) {
      fields.add(new MyField(i, structFieldNames.get(i),
          structFieldObjectInspectors.get(i),
          structFieldComments == null ? null : structFieldComments.get(i)));
      originalColumnNames.add(structFieldNames.get(i));
    }
  }

  protected StandardStructObjectInspector(List<StructField> fields) {
    init(fields);
  }

  protected void init(List<StructField> fields) {
    this.fields = new ArrayList<MyField>(fields.size());
    this.originalColumnNames = new ArrayList<String>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      this.fields.add(new MyField(i, fields.get(i).getFieldName(), fields
          .get(i).getFieldObjectInspector()));
      this.originalColumnNames.add(fields.get(i).getFieldName());
    }
  }

  public String getTypeName() {
    return ObjectInspectorUtils.getStandardStructTypeName(this);
  }

  public final Category getCategory() {
    return Category.STRUCT;
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

  boolean warned = false;

  // With Data
  @Override
  @SuppressWarnings("unchecked")
  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data == null) {
      return null;
    }
    // We support both List<Object> and Object[]
    // so we have to do differently.
    boolean isArray = data.getClass().isArray();
    if (!isArray && !(data instanceof List)) {
      if (!warned) {
        LOG.warn("Invalid type for struct " + data.getClass());
        LOG.warn("ignoring similar errors.");
        warned = true;
      }
      return data;
    }
    int listSize = (isArray ? ((Object[]) data).length : ((List<Object>) data)
        .size());
    MyField f = (MyField) fieldRef;
    if (fields.size() != listSize && !warned) {
      // TODO: remove this
      warned = true;
      LOG.warn("Trying to access " + fields.size()
          + " fields inside a list of " + listSize + " elements: "
          + (isArray ? Arrays.asList((Object[]) data) : (List<Object>) data));
      LOG.warn("ignoring similar errors.");
    }
    int fieldID = f.getFieldID();

    if (fieldID >= listSize) {
      return null;
    } else if (isArray) {
      return ((Object[]) data)[fieldID];
    } else {
      return ((List<Object>) data).get(fieldID);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    // We support both List<Object> and Object[]
    // so we have to do differently.
    if (! (data instanceof List)) {
      data = java.util.Arrays.asList((Object[]) data);
    }
    List<Object> list = (List<Object>) data;
    return list;
  }

  public List<String> getOriginalColumnNames() {
    return originalColumnNames;
  }

  // /////////////////////////////
  // SettableStructObjectInspector
  @Override
  public Object create() {
    ArrayList<Object> a = new ArrayList<Object>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      a.add(null);
    }
    return a;
  }

  @Override
  public Object setStructFieldData(Object struct, StructField field,
      Object fieldValue) {
    ArrayList<Object> a = (ArrayList<Object>) struct;
    MyField myField = (MyField) field;
    a.set(myField.fieldID, fieldValue);
    return a;
  }

}
