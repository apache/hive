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

import org.apache.hadoop.hive.serde2.BaseStructObjectInspector;
import org.apache.hadoop.hive.serde2.StructObject;
import org.apache.hadoop.hive.serde2.avro.AvroLazyObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;

/**
 * LazySimpleStructObjectInspector works on struct data that is stored in
 * LazyStruct.
 *
 * The names of the struct fields and the internal structure of the struct
 * fields are specified in the ctor of the LazySimpleStructObjectInspector.
 *
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects,
 * instead of directly creating an instance of this class.
 */
public class LazySimpleStructObjectInspector extends BaseStructObjectInspector {

  private byte separator;
  private Text nullSequence;
  private boolean lastColumnTakesRest;
  private boolean escaped;
  private byte escapeChar;

  protected LazySimpleStructObjectInspector() {
    super();
  }

  protected LazySimpleStructObjectInspector(
      List<StructField> fields, byte separator, Text nullSequence) {
    init(fields);
    this.separator = separator;
    this.nullSequence = nullSequence;
  }

  /**
   * Call ObjectInspectorFactory.getLazySimpleStructObjectInspector instead.
   */
  @Deprecated
  protected LazySimpleStructObjectInspector(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors, byte separator,
      Text nullSequence, boolean lastColumnTakesRest, boolean escaped,
      byte escapeChar) {
    init(structFieldNames, structFieldObjectInspectors, null, separator,
        nullSequence, lastColumnTakesRest, escaped, escapeChar);
  }

  public LazySimpleStructObjectInspector(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors,
      List<String> structFieldComments, byte separator, Text nullSequence,
      boolean lastColumnTakesRest, boolean escaped, byte escapeChar) {
    init(structFieldNames, structFieldObjectInspectors, structFieldComments,
        separator, nullSequence, lastColumnTakesRest, escaped, escapeChar);
  }

  protected void init(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors,
      List<String> structFieldComments, byte separator,
      Text nullSequence, boolean lastColumnTakesRest, boolean escaped,
      byte escapeChar) {
    init(structFieldNames, structFieldObjectInspectors, structFieldComments);
    this.separator = separator;
    this.nullSequence = nullSequence;
    this.lastColumnTakesRest = lastColumnTakesRest;
    this.escaped = escaped;
    this.escapeChar = escapeChar;
  }

  // With Data
  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data == null) {
      return null;
    }
    StructObject struct = (StructObject) data;
    MyField f = (MyField) fieldRef;

    int fieldID = f.getFieldID();
    assert (fieldID >= 0 && fieldID < fields.size());

    ObjectInspector oi = f.getFieldObjectInspector();

    if (oi instanceof AvroLazyObjectInspector) {
      return ((AvroLazyObjectInspector) oi).getStructFieldData(data, fieldRef);
    }

    if (oi instanceof MapObjectInspector) {
      ObjectInspector valueOI = ((MapObjectInspector) oi).getMapValueObjectInspector();

      if (valueOI instanceof AvroLazyObjectInspector) {
        return ((AvroLazyObjectInspector) valueOI).getStructFieldData(data, fieldRef);
      }
    }

    return struct.getField(fieldID);
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }

    // Iterate over all the fields picking up the nested structs within them
    List<Object> result = new ArrayList<Object>(fields.size());

    for (MyField myField : fields) {
      result.add(getStructFieldData(data, myField));
    }

    return result;
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
