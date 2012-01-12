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
package org.apache.hadoop.hive.serde2.lazybinary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.SerDeStatsStruct;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.RecordInfo;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * LazyBinaryStruct is serialized as follows: start A B A B A B end bytes[] ->
 * |-----|---------|--- ... ---|-----|---------|
 *
 * Section A is one null-byte, corresponding to eight struct fields in Section
 * B. Each bit indicates whether the corresponding field is null (0) or not null
 * (1). Each field is a LazyBinaryObject.
 *
 * Following B, there is another section A and B. This pattern repeats until the
 * all struct fields are serialized.
 */
public class LazyBinaryStruct extends
    LazyBinaryNonPrimitive<LazyBinaryStructObjectInspector> implements SerDeStatsStruct {

  private static Log LOG = LogFactory.getLog(LazyBinaryStruct.class.getName());

  /**
   * Whether the data is already parsed or not.
   */
  boolean parsed;

  /**
   * Size of serialized data
   */
  long serializedSize;

  /**
   * The fields of the struct.
   */
  LazyBinaryObject[] fields;

  /**
   * Whether a field is initialized or not.
   */
  boolean[] fieldInited;

  /**
   * Whether a field is null or not. Because length is 0 does not means the
   * field is null. In particular, a 0-length string is not null.
   */
  boolean[] fieldIsNull;

  /**
   * The start positions and lengths of struct fields. Only valid when the data
   * is parsed.
   */
  int[] fieldStart;
  int[] fieldLength;

  /**
   * Construct a LazyBinaryStruct object with an ObjectInspector.
   */
  protected LazyBinaryStruct(LazyBinaryStructObjectInspector oi) {
    super(oi);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    parsed = false;
    serializedSize = length;
  }

  RecordInfo recordInfo = new LazyBinaryUtils.RecordInfo();
  boolean missingFieldWarned = false;
  boolean extraFieldWarned = false;

  /**
   * Parse the byte[] and fill fieldStart, fieldLength, fieldInited and
   * fieldIsNull.
   */
  private void parse() {

    List<? extends StructField> fieldRefs = ((StructObjectInspector) oi)
        .getAllStructFieldRefs();

    if (fields == null) {
      fields = new LazyBinaryObject[fieldRefs.size()];
      for (int i = 0; i < fields.length; i++) {
        ObjectInspector insp = fieldRefs.get(i).getFieldObjectInspector();
        fields[i] = insp == null ? null : LazyBinaryFactory.createLazyBinaryObject(insp);
      }
      fieldInited = new boolean[fields.length];
      fieldIsNull = new boolean[fields.length];
      fieldStart = new int[fields.length];
      fieldLength = new int[fields.length];
    }

    /**
     * Please note that one null byte is followed by eight fields, then more
     * null byte and fields.
     */

    int fieldId = 0;
    int structByteEnd = start + length;
    byte[] bytes = this.bytes.getData();

    byte nullByte = bytes[start];
    int lastFieldByteEnd = start + 1;
    // Go through all bytes in the byte[]
    for (int i = 0; i < fields.length; i++) {
      fieldIsNull[i] = true;
      if ((nullByte & (1 << (i % 8))) != 0) {
        fieldIsNull[i] = false;
        LazyBinaryUtils.checkObjectByteInfo(fieldRefs.get(i)
            .getFieldObjectInspector(), bytes, lastFieldByteEnd, recordInfo);
        fieldStart[i] = lastFieldByteEnd + recordInfo.elementOffset;
        fieldLength[i] = recordInfo.elementSize;
        lastFieldByteEnd = fieldStart[i] + fieldLength[i];
      }

      // count how many fields are there
      if (lastFieldByteEnd <= structByteEnd) {
        fieldId++;
      }
      // next byte is a null byte if there are more bytes to go
      if (7 == (i % 8)) {
        if (lastFieldByteEnd < structByteEnd) {
          nullByte = bytes[lastFieldByteEnd];
          lastFieldByteEnd++;
        } else {
          // otherwise all null afterwards
          nullByte = 0;
          lastFieldByteEnd++;
        }
      }
    }

    // Extra bytes at the end?
    if (!extraFieldWarned && lastFieldByteEnd < structByteEnd) {
      extraFieldWarned = true;
      LOG.warn("Extra bytes detected at the end of the row! Ignoring similar "
          + "problems.");
    }

    // Missing fields?
    if (!missingFieldWarned && lastFieldByteEnd > structByteEnd) {
      missingFieldWarned = true;
      LOG.info("Missing fields! Expected " + fields.length + " fields but "
          + "only got " + fieldId + "! Ignoring similar problems.");
    }

    Arrays.fill(fieldInited, false);
    parsed = true;
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
   * @param fieldID
   *          The field ID
   * @return The field as a LazyObject
   */
  public Object getField(int fieldID) {
    if (!parsed) {
      parse();
    }
    return uncheckedGetField(fieldID);
  }

  /**
   * Get the field out of the row without checking parsed. This is called by
   * both getField and getFieldsAsList.
   *
   * @param fieldID
   *          The id of the field starting from 0.
   * @return The value of the field
   */
  private Object uncheckedGetField(int fieldID) {
    // Test the length first so in most cases we avoid doing a byte[]
    // comparison.
    if (fieldIsNull[fieldID]) {
      return null;
    }
    if (!fieldInited[fieldID]) {
      fieldInited[fieldID] = true;
      fields[fieldID].init(bytes, fieldStart[fieldID], fieldLength[fieldID]);
    }
    return fields[fieldID].getObject();
  }

  ArrayList<Object> cachedList;

  /**
   * Get the values of the fields as an ArrayList.
   *
   * @return The values of the fields as an ArrayList.
   */
  public ArrayList<Object> getFieldsAsList() {
    if (!parsed) {
      parse();
    }
    if (cachedList == null) {
      cachedList = new ArrayList<Object>();
    } else {
      cachedList.clear();
    }
    for (int i = 0; i < fields.length; i++) {
      cachedList.add(uncheckedGetField(i));
    }
    return cachedList;
  }

  @Override
  public Object getObject() {
    return this;
  }

  public long getRawDataSerializedSize() {
    return serializedSize;
  }
}
