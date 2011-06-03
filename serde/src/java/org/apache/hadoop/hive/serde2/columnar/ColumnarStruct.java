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
import org.apache.hadoop.hive.serde2.SerDeStatsStruct;
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
public class ColumnarStruct implements SerDeStatsStruct{

  private static final Log LOG = LogFactory.getLog(ColumnarStruct.class);

  int[] prjColIDs = null; // list of projected column IDs

  Text nullSequence;
  int lengthNullSequence;

  /**
   * Construct a ColumnarStruct object with the TypeInfo. It creates the first
   * level object at the first place
   *
   * @param oi
   *          the ObjectInspector representing the type of this LazyStruct.
   */
  public ColumnarStruct(ObjectInspector oi) {
    this(oi, null, null);
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
      ArrayList<Integer> notSkippedColumnIDs, Text nullSequence) {
    List<? extends StructField> fieldRefs = ((StructObjectInspector) oi)
        .getAllStructFieldRefs();
    int num = fieldRefs.size();

    fieldInfoList = new FieldInfo[num];

    if (nullSequence != null) {
      this.nullSequence = nullSequence;
      this.lengthNullSequence = nullSequence.getLength();
    }

    // if no columns is set to be skipped, add all columns in
    // 'notSkippedColumnIDs'
    if (notSkippedColumnIDs == null || notSkippedColumnIDs.size() == 0) {
      for (int i = 0; i < num; i++) {
        notSkippedColumnIDs.add(i);
      }
    }

    for (int i = 0; i < num; i++) {
      fieldInfoList[i] = new FieldInfo(
          LazyFactory.createLazyObject(fieldRefs.get(i)
          .getFieldObjectInspector()),
          !notSkippedColumnIDs.contains(i));
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
  public Object getField(int fieldID) {
    return fieldInfoList[fieldID].uncheckedGetField();
  }

  class FieldInfo {
    LazyObject field;
    /*
     * use an array instead of only one object in case in future hive does not do
     * the byte copy.
     */
    ByteArrayRef cachedByteArrayRef;
    BytesRefWritable rawBytesField;
    boolean inited;
    boolean fieldSkipped;

    public FieldInfo(LazyObject lazyObject, boolean fieldSkipped) {
      field = lazyObject;
      cachedByteArrayRef = new ByteArrayRef();
      if (fieldSkipped) {
        this.fieldSkipped = true;
        inited = true;
      } else {
        inited = false;
      }
    }

    /*
     * ============================ [PERF] ===================================
     * This function is called for every row. Setting up the selected/projected
     * columns at the first call, and don't do that for the following calls.
     * Ideally this should be done in the constructor where we don't need to
     * branch in the function for each row.
     * =========================================================================
     */
    public void init(BytesRefWritable col) {
        if (col != null) {
          rawBytesField= col;
          inited = false;
        } else {
          // select columns that actually do not exist in the file.
          fieldSkipped = true;
        }
    }

    /**
     * Return the uncompressed size of this field
     */
    public long getSerializedSize(){
      if (rawBytesField == null) {
        return 0;
      }
      return rawBytesField.getLength();
    }

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
    protected Object uncheckedGetField() {
      if (fieldSkipped) {
        return null;
      }
      if (!inited) {
        try {
          cachedByteArrayRef.setData(rawBytesField.getData());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        field.init(cachedByteArrayRef, rawBytesField
            .getStart(), rawBytesField.getLength());
        inited = true;
      }


      int fieldLen = rawBytesField.length;
      if (fieldLen == lengthNullSequence) {
        byte[] data = cachedByteArrayRef.getData();

        if (LazyUtils.compare(data, rawBytesField.getStart(), fieldLen,
            nullSequence.getBytes(), 0, lengthNullSequence) == 0) {
          return null;
        }
      }

      return field.getObject();

    }
  }

  FieldInfo[] fieldInfoList = null;


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
        fieldInfoList[fieldIndex].init(cols.unCheckedGet(fieldIndex));
      } else {
        // select columns that actually do not exist in the file.
        fieldInfoList[fieldIndex].init(null);
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
  public ArrayList<Object> getFieldsAsList() {
    if (cachedList == null) {
      cachedList = new ArrayList<Object>();
    } else {
      cachedList.clear();
    }
    for (int i = 0; i < fieldInfoList.length; i++) {
      cachedList.add(fieldInfoList[i].uncheckedGetField());
    }
    return cachedList;
  }

  public long getRawDataSerializedSize() {
    long serializedSize = 0;
    for (int i = 0; i < fieldInfoList.length; ++i) {
      serializedSize += fieldInfoList[i].getSerializedSize();
    }
    return serializedSize;
  }
}
