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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
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
   * The fields of the struct.
   */
  LazyObject[] fields;

  private static final Log LOG = LogFactory.getLog(ColumnarStruct.class);

  int[] prjColIDs = null; // list of projected column IDs

  /**
   * Construct a ColumnarStruct object with the TypeInfo. It creates the first
   * level object at the first place
   * 
   * @param oi
   *          the ObjectInspector representing the type of this LazyStruct.
   */
  public ColumnarStruct(ObjectInspector oi) {
    this(oi, null);
  }

  /**
   * Construct a ColumnarStruct object with the TypeInfo. It creates the first
   * level object at the first place
   * 
   * @param oi
   *          the ObjectInspector representing the type of this LazyStruct.
   * @param notSkippedColumnIDs
   *          the column ids that should not be skipped
   */
  public ColumnarStruct(ObjectInspector oi,
      ArrayList<Integer> notSkippedColumnIDs) {
    List<? extends StructField> fieldRefs = ((StructObjectInspector) oi)
        .getAllStructFieldRefs();
    int num = fieldRefs.size();
    fields = new LazyObject[num];
    cachedByteArrayRef = new ByteArrayRef[num];
    rawBytesField = new BytesRefWritable[num];
    fieldSkipped = new boolean[num];
    inited = new boolean[num];

    // if no columns is set to be skipped, add all columns in
    // 'notSkippedColumnIDs'
    if (notSkippedColumnIDs == null || notSkippedColumnIDs.size() == 0) {
      for (int i = 0; i < num; i++) {
        notSkippedColumnIDs.add(i);
      }
    }

    for (int i = 0; i < num; i++) {
      fields[i] = LazyFactory.createLazyObject(fieldRefs.get(i)
          .getFieldObjectInspector());
      cachedByteArrayRef[i] = new ByteArrayRef();
      if (!notSkippedColumnIDs.contains(i)) {
        fieldSkipped[i] = true;
        inited[i] = true;
      } else {
        inited[i] = false;
      }
    }

    // maintain a list of non-NULL column IDs
    int min = notSkippedColumnIDs.size() > num ? num : notSkippedColumnIDs
        .size();
    prjColIDs = new int[min];
    for (int i = 0, index = 0; i < notSkippedColumnIDs.size(); ++i) {
      int readCol = notSkippedColumnIDs.get(i).intValue();
      if (readCol < num) {
        prjColIDs[index] = readCol;
        index++;
      }
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
  BytesRefWritable[] rawBytesField = null;
  boolean[] inited = null;
  boolean[] fieldSkipped = null;

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
    if (fieldSkipped[fieldID]) {
      return null;
    }
    if (!inited[fieldID]) {
      BytesRefWritable passedInField = rawBytesField[fieldID];
      try {
        cachedByteArrayRef[fieldID].setData(passedInField.getData());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      fields[fieldID].init(cachedByteArrayRef[fieldID], passedInField
          .getStart(), passedInField.getLength());
      inited[fieldID] = true;
    }

    byte[] data = cachedByteArrayRef[fieldID].getData();
    int fieldLen = rawBytesField[fieldID].length;

    if (fieldLen == nullSequence.getLength()
        && LazyUtils.compare(data, rawBytesField[fieldID].getStart(), fieldLen,
            nullSequence.getBytes(), 0, nullSequence.getLength()) == 0) {
      return null;
    }

    return fields[fieldID].getObject();
  }

  /*
   * ============================ [PERF] ===================================
   * This function is called for every row. Setting up the selected/projected
   * columns at the first call, and don't do that for the following calls.
   * Ideally this should be done in the constructor where we don't need to
   * branch in the function for each row.
   * =========================================================================
   */
  public void init(BytesRefArrayWritable cols) {
    for (int i = 0; i < prjColIDs.length; ++i) {
      int fieldIndex = prjColIDs[i];
      if (fieldIndex < cols.size()) {
        rawBytesField[fieldIndex] = cols.unCheckedGet(fieldIndex);
        inited[fieldIndex] = false;
      } else {
        // select columns that actually do not exist in the file.
        fieldSkipped[fieldIndex] = true;
      }
    }
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
