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

package org.apache.hadoop.hive.serde2.lazy.objectinspector;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * LazySimpleStructObjectInspector works on struct data that is stored in LazyStruct.
 * 
 * The names of the struct fields and the internal structure of the struct fields
 * are specified in the ctor of the LazySimpleStructObjectInspector.
 * 
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects, instead
 * of directly creating an instance of this class.
 */
public class LazySimpleStructObjectInspector extends StructObjectInspector {

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
  
  @Override
  public String getTypeName() {
    return ObjectInspectorUtils.getStandardStructTypeName(this);
  }
  
  
  byte separator;
  Text nullSequence;  
  boolean lastColumnTakesRest;
  boolean escaped;
  byte escapeChar;
  
  /** Call ObjectInspectorFactory.getLazySimpleStructObjectInspector instead.
   */
  protected LazySimpleStructObjectInspector(List<String> structFieldNames, List<ObjectInspector> structFieldObjectInspectors,
      byte separator, Text nullSequence, boolean lastColumnTakesRest,
      boolean escaped, byte escapeChar) {
    init(structFieldNames, structFieldObjectInspectors, separator, nullSequence, lastColumnTakesRest,
        escaped, escapeChar);
  }
  protected void init(List<String> structFieldNames, List<ObjectInspector> structFieldObjectInspectors,
      byte separator, Text nullSequence, boolean lastColumnTakesRest, boolean escaped, byte escapeChar) {
    assert(structFieldNames.size() == structFieldObjectInspectors.size());
    
    this.separator = separator;
    this.nullSequence = nullSequence;
    this.lastColumnTakesRest = lastColumnTakesRest;
    this.escaped = escaped;
    this.escapeChar = escapeChar;
    
    fields = new ArrayList<MyField>(structFieldNames.size()); 
    for(int i=0; i<structFieldNames.size(); i++) {
      fields.add(new MyField(i, structFieldNames.get(i), structFieldObjectInspectors.get(i)));
    }
  }
  
  protected LazySimpleStructObjectInspector(List<StructField> fields, byte separator, Text nullSequence) {
    init(fields, separator, nullSequence);
  }
  protected void init(List<StructField> fields, byte separator, Text nullSequence) {
    this.separator = separator;
    this.nullSequence = nullSequence;
    
    this.fields = new ArrayList<MyField>(fields.size()); 
    for(int i=0; i<fields.size(); i++) {
      this.fields.add(new MyField(i, fields.get(i).getFieldName(), fields.get(i).getFieldObjectInspector()));
    }
  }

  
  @Override
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

  // With Data
  @Override
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
    LazyStruct struct = (LazyStruct)data;
    return struct.getFieldsAsList();
  }

  // For LazyStruct
  public byte getSeparator() {
    return separator;
  }
  public Text getNullSequence() {
    return nullSequence;
  }
  public boolean getLastColumnTakesRest() {
    return lastColumnTakesRest;
  }
  public boolean isEscaped() {
    return escaped;
  }
  public byte getEscapeChar() {
    return escapeChar;
  }
  
}
