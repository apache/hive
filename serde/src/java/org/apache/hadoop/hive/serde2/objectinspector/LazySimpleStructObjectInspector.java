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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;

/**
 * LazySimpleStructObjectInspector works on struct data that is stored in LazyStruct.
 * It only supports primitive types as its fields for simplicity and efficiency. 
 * 
 * The names of the struct fields and the internal structure of the struct fields
 * are specified in the ctor of the LazySimpleStructObjectInspector.
 * 
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects, instead
 * of directly creating an instance of this class.
 */
public class LazySimpleStructObjectInspector implements StructObjectInspector {

  public static final Log LOG = LogFactory.getLog(LazySimpleStructObjectInspector.class.getName());
  
  protected static class MyField implements StructField {
    protected int fieldID;
    protected String fieldName;
    protected ObjectInspector fieldObjectInspector;
    
    public MyField(int fieldID, String fieldName, ObjectInspector fieldObjectInspector) {
      this.fieldID = fieldID;
      this.fieldName = fieldName.toLowerCase();
      this.fieldObjectInspector = fieldObjectInspector;
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
    
    public String toString() {
      return "" + fieldID + ":" + fieldName;
    }
  }
  
  protected List<MyField> fields;
  
  public String getTypeName() {
    return ObjectInspectorUtils.getStandardStructTypeName(this);
  }
  
  /** Call ObjectInspectorFactory.getLazySimpleStructObjectInspector instead.
   */
  protected LazySimpleStructObjectInspector(List<String> structFieldNames, List<ObjectInspector> structFieldObjectInspectors) {
    init(structFieldNames, structFieldObjectInspectors);
  }
  protected void init(List<String> structFieldNames, List<ObjectInspector> structFieldObjectInspectors) {
    assert(structFieldNames.size() == structFieldObjectInspectors.size());
    
    fields = new ArrayList<MyField>(structFieldNames.size()); 
    for(int i=0; i<structFieldNames.size(); i++) {
      fields.add(new MyField(i, structFieldNames.get(i), structFieldObjectInspectors.get(i)));
    }
  }
  
  protected LazySimpleStructObjectInspector(List<StructField> fields) {
    init(fields);
  }
  protected void init(List<StructField> fields) {
    this.fields = new ArrayList<MyField>(fields.size()); 
    for(int i=0; i<fields.size(); i++) {
      this.fields.add(new MyField(i, fields.get(i).getFieldName(), fields.get(i).getFieldObjectInspector()));
    }
  }

  
  public final Category getCategory() {
    return Category.STRUCT;
  }

  // Without Data
  public StructField getStructFieldRef(String fieldName) {
    return ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields);
  }
  public List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }

  // With Data
  @SuppressWarnings("unchecked")
  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data == null) {
      return null;
    }
    LazyStruct struct = (LazyStruct)data;
    MyField f = (MyField) fieldRef;
    
    int fieldID = f.getFieldID();
    assert(fieldID >= 0 && fieldID < fields.size());
    
    return struct.getField(fieldID);
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    List<Object> fieldsData = new ArrayList<Object>(fields.size());
    LazyStruct struct = (LazyStruct)data;
    for (int i=0; i<fields.size(); i++) {
      fieldsData.add(struct.getField(i));
    }
    return fieldsData;
  }

}
