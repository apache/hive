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

import org.apache.hadoop.hive.serde2.SerDeStatsStruct;
import org.apache.hadoop.hive.serde2.StructObject;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public abstract class ColumnarStructBase implements StructObject, SerDeStatsStruct {

  class FieldInfo {
    LazyObjectBase field;
    /*
     * use an array instead of only one object in case in future hive does not do
     * the byte copy.
     */
    ByteArrayRef cachedByteArrayRef;
    BytesRefWritable rawBytesField;
    boolean inited;
    boolean fieldSkipped;
    ObjectInspector objectInspector;

    public FieldInfo(LazyObjectBase lazyObject, boolean fieldSkipped, ObjectInspector oi) {
      field = lazyObject;
      cachedByteArrayRef = new ByteArrayRef();
      objectInspector = oi;
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
        rawBytesField = col;
        inited = false;
        fieldSkipped = false;
      } else {
        // select columns that actually do not exist in the file.
        fieldSkipped = true;
      }
    }

    /**
     * Return the uncompressed size of this field
     */
    public long getSerializedSize() {
      if (rawBytesField == null) {
        return 0;
      }
      return rawBytesField.getLength();
    }

    /**
     * Get the field out of the row without checking parsed. This is called by
     * both getField and getFieldsAsList.
     *
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
        inited = true;
        int byteLength = getLength(objectInspector, cachedByteArrayRef, rawBytesField.getStart(),
            rawBytesField.getLength());
        if (byteLength == -1) {
          return null;
        }

        field.init(cachedByteArrayRef, rawBytesField.getStart(), byteLength);
        return field.getObject();
      } else {
        if (getLength(objectInspector, cachedByteArrayRef, rawBytesField.getStart(),
            rawBytesField.getLength()) == -1) {
          return null;
        }
        return field.getObject();
      }
    }
  }

  protected int[] prjColIDs = null;
  private FieldInfo[] fieldInfoList = null;
  private ArrayList<Object> cachedList;

  public ColumnarStructBase(ObjectInspector oi, List<Integer> notSkippedColumnIDs) {
    List<? extends StructField> fieldRefs = ((StructObjectInspector) oi)
        .getAllStructFieldRefs();
    int num = fieldRefs.size();

    fieldInfoList = new FieldInfo[num];

    for (int i = 0; i < num; i++) {
      ObjectInspector foi = fieldRefs.get(i).getFieldObjectInspector();
      fieldInfoList[i] = new FieldInfo(
          createLazyObjectBase(foi),
          !notSkippedColumnIDs.contains(i),
          foi);
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
   * @return The field as a LazyObject
   */
  public Object getField(int fieldID) {
    return fieldInfoList[fieldID].uncheckedGetField();
  }

  /**
   * Check if the object is null and return the length of the stream
   *
   * @param objectInspector
   * @param cachedByteArrayRef
   *          the bytes of the object
   * @param start
   *          the start offset
   * @param length
   *          the length
   *
   * @return -1 for null, >=0 for length
   */
  protected abstract int getLength(ObjectInspector objectInspector,
      ByteArrayRef cachedByteArrayRef, int start, int length);

  /**
   * create the lazy object for this field
   *
   * @param objectInspector
   *          the object inspector for the field
   * @return the lazy object for the field
   */
  protected abstract LazyObjectBase createLazyObjectBase(ObjectInspector objectInspector);

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

  /**
   * Get the values of the fields as an ArrayList.
   *
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
