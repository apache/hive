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

import java.util.List;

import org.apache.hadoop.hive.serde2.BaseStructObjectInspector;
import org.apache.hadoop.hive.serde2.columnar.ColumnarStructBase;

/**
 * ColumnarStructObjectInspector works on struct data that is stored in
 * ColumnarStructBase.
 *
 * The names of the struct fields and the internal structure of the struct
 * fields are specified in the ctor of the ColumnarStructObjectInspector.
 *
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects,
 * instead of directly creating an instance of this class.
 */
class ColumnarStructObjectInspector extends BaseStructObjectInspector {

  protected ColumnarStructObjectInspector() {
    super();
  }

  /**
   * Call ObjectInspectorFactory.getLazySimpleStructObjectInspector instead.
   */
  public ColumnarStructObjectInspector(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors) {
    super(structFieldNames, structFieldObjectInspectors);
  }

  public ColumnarStructObjectInspector(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors,
      List<String> structFieldComments) {
    super(structFieldNames, structFieldObjectInspectors, structFieldComments);
  }

  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data == null) {
      return null;
    }
    ColumnarStructBase struct = (ColumnarStructBase) data;
    MyField f = (MyField) fieldRef;

    int fieldID = f.getFieldID();
    assert (fieldID >= 0 && fieldID < fields.size());

    return struct.getField(fieldID);
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    ColumnarStructBase struct = (ColumnarStructBase) data;
    return struct.getFieldsAsList();
  }
}
