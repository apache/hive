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

package org.apache.hadoop.hive.serde2.columnar;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

/**
 * ColumnarStruct is different from LazyStruct in that ColumnarStruct's field
 * Object get parsed at its initialize time when call
 * {@link #init(BytesRefArrayWritable cols)}, while LazyStruct parse fields in a
 * lazy way.
 * 
 */
public class ColumnarStruct {

  /**
   * The TypeInfo for this LazyNonPrimitive.
   */
  TypeInfo typeInfo;
  List<TypeInfo> fieldTypeInfos;

  /**
   * Whether the data is already parsed or not.
   */
  boolean parsed;

  /**
   * The fields of the struct.
   */
  LazyObject[] fields;

  /**
   * Construct a ColumnarStruct object with the TypeInfo. It creates the first
   * level object at the first place
   * 
   * @param typeInfo
   *          the TypeInfo representing the type of this LazyStruct.
   */
  public ColumnarStruct(TypeInfo typeInfo) {
    this.typeInfo = typeInfo;
    fieldTypeInfos = ((StructTypeInfo) typeInfo).getAllStructFieldTypeInfos();
    int num = fieldTypeInfos.size();
    fields = new LazyObject[num];
    cachedByteArrayRef = new ByteArrayRef[num];
    fieldIsNull = new boolean[num];
    for (int i = 0; i < num; i++) {
      fields[i] = LazyFactory.createLazyObject(fieldTypeInfos.get(i));
      cachedByteArrayRef[i] = new ByteArrayRef();
      fieldIsNull[i] = false;
    }
  }

  /**
   * Get one field out of the struct.
   * 
   * If the field is a primitive field, return the actual object. Otherwise
   * return the LazyObject. This is because PrimitiveObjectInspector does not
   * have control over the object used by the user - the user simply directly
   * use the Object instead of going through Object
   * PrimitiveObjectInspector.get(Object).
   * 
   * NOTE: separator and nullSequence has to be the same each time this method
   * is called. These two parameters are used only once to parse each record.
   * 
   * @param fieldID
   *          The field ID
   * @param nullSequence
   *          The sequence for null value
   * @return The field as a LazyObject
   */
  public Object getField(int fieldID, Text nullSequence) {
    return uncheckedGetField(fieldID, nullSequence);
  }

  /*
   * use an array instead of only one object in case in future hive does not do
   * the byte copy.
   */
  ByteArrayRef[] cachedByteArrayRef = null;
  boolean[] fieldIsNull = null;

  /**
   * Get the field out of the row without checking parsed. This is called by
   * both getField and getFieldsAsList.
   * 
   * @param fieldID
   *          The id of the field starting from 0.
   * @param nullSequence
   *          The sequence representing NULL value.
   * @return The value of the field
   */
  protected Object uncheckedGetField(int fieldID, Text nullSequence) {
    if (fieldIsNull[fieldID])
      return null;
    int fieldLen = cachedByteArrayRef[fieldID].getData().length;
    if (fieldLen == nullSequence.getLength()
        && LazyUtils.compare(cachedByteArrayRef[fieldID].getData(), 0,
            fieldLen, nullSequence.getBytes(), 0, nullSequence.getLength()) == 0) {
      return null;
    }

    return fields[fieldID].getObject();
  }

  public void init(BytesRefArrayWritable cols) {
    int fieldIndex = 0;
    int min = cols.size() < fields.length ? cols.size() : fields.length;

    for (; fieldIndex < min; fieldIndex++) {
      BytesRefWritable passedInField = cols.get(fieldIndex);
      cachedByteArrayRef[fieldIndex].setData(passedInField.getData());
      if (passedInField.length > 0) {
        // if (fields[fieldIndex] == null)
        // fields[fieldIndex] = LazyFactory.createLazyObject(fieldTypeInfos
        // .get(fieldIndex));
        fields[fieldIndex].init(cachedByteArrayRef[fieldIndex], passedInField
            .getStart(), passedInField.getLength());
        fieldIsNull[fieldIndex] = false;
      } else {
        fieldIsNull[fieldIndex] = true;
      }
    }
    for (; fieldIndex < fields.length; fieldIndex++)
      fieldIsNull[fieldIndex] = true;

    parsed = true;
  }

  ArrayList<Object> cachedList;

  /**
   * Get the values of the fields as an ArrayList.
   * 
   * @param nullSequence
   *          The sequence for the NULL value
   * @return The values of the fields as an ArrayList.
   */
  public ArrayList<Object> getFieldsAsList(Text nullSequence) {
    if (cachedList == null) {
      cachedList = new ArrayList<Object>();
    } else {
      cachedList.clear();
    }
    for (int i = 0; i < fields.length; i++) {
      cachedList.add(uncheckedGetField(i, nullSequence));
    }
    return cachedList;
  }
}