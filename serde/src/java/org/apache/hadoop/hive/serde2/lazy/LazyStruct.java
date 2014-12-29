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
package org.apache.hadoop.hive.serde2.lazy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.primitives.Bytes;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStatsStruct;
import org.apache.hadoop.hive.serde2.StructObject;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

/**
 * LazyObject for storing a struct. The field of a struct can be primitive or
 * non-primitive.
 *
 * LazyStruct does not deal with the case of a NULL struct. That is handled by
 * the parent LazyObject.
 */
public class LazyStruct extends LazyNonPrimitive<LazySimpleStructObjectInspector>
    implements StructObject, SerDeStatsStruct {

  private static Log LOG = LogFactory.getLog(LazyStruct.class.getName());

  /**
   * Whether the data is already parsed or not.
   */
  boolean parsed;

  /**
   * Size of serialized data
   */
  long serializedSize;

  /**
   * The start positions of struct fields. Only valid when the data is parsed.
   * Note that startPosition[arrayLength] = begin + length + 1; that makes sure
   * we can use the same formula to compute the length of each element of the
   * array.
   */
  int[] startPosition;

  /**
   * The fields of the struct.
   */
  LazyObjectBase[] fields;
  /**
   * Whether init() has been called on the field or not.
   */
  boolean[] fieldInited;

  /**
   * Construct a LazyStruct object with the ObjectInspector.
   */
  public LazyStruct(LazySimpleStructObjectInspector oi) {
    super(oi);
  }

  /**
   * Set the row data for this LazyStruct.
   *
   * @see LazyObject#init(ByteArrayRef, int, int)
   */
  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    parsed = false;
    serializedSize = length;
  }

  boolean missingFieldWarned = false;
  boolean extraFieldWarned = false;

  /**
   * Parse the byte[] and fill each field.
   */
  private void parse() {

    byte separator = oi.getSeparator();
    boolean lastColumnTakesRest = oi.getLastColumnTakesRest();
    boolean isEscaped = oi.isEscaped();
    byte escapeChar = oi.getEscapeChar();

    if (fields == null) {
      initLazyFields(oi.getAllStructFieldRefs());
    }

    int structByteEnd = start + length;
    int fieldId = 0;
    int fieldByteBegin = start;
    int fieldByteEnd = start;
    byte[] bytes = this.bytes.getData();

    // Go through all bytes in the byte[]
    while (fieldByteEnd <= structByteEnd) {
      if (fieldByteEnd == structByteEnd || bytes[fieldByteEnd] == separator) {
        // Reached the end of a field?
        if (lastColumnTakesRest && fieldId == fields.length - 1) {
          fieldByteEnd = structByteEnd;
        }
        startPosition[fieldId] = fieldByteBegin;
        fieldId++;
        if (fieldId == fields.length || fieldByteEnd == structByteEnd) {
          // All fields have been parsed, or bytes have been parsed.
          // We need to set the startPosition of fields.length to ensure we
          // can use the same formula to calculate the length of each field.
          // For missing fields, their starting positions will all be the same,
          // which will make their lengths to be -1 and uncheckedGetField will
          // return these fields as NULLs.
          for (int i = fieldId; i <= fields.length; i++) {
            startPosition[i] = fieldByteEnd + 1;
          }
          break;
        }
        fieldByteBegin = fieldByteEnd + 1;
        fieldByteEnd++;
      } else {
        if (isEscaped && bytes[fieldByteEnd] == escapeChar
            && fieldByteEnd + 1 < structByteEnd) {
          // ignore the char after escape_char
          fieldByteEnd += 2;
        } else {
          fieldByteEnd++;
        }
      }
    }

    // Extra bytes at the end?
    if (!extraFieldWarned && fieldByteEnd < structByteEnd) {
      extraFieldWarned = true;
      LOG.warn("Extra bytes detected at the end of the row! Ignoring similar "
          + "problems.");
    }

    // Missing fields?
    if (!missingFieldWarned && fieldId < fields.length) {
      missingFieldWarned = true;
      LOG.info("Missing fields! Expected " + fields.length + " fields but "
          + "only got " + fieldId + "! Ignoring similar problems.");
    }

    Arrays.fill(fieldInited, false);
    parsed = true;
  }

  protected final void initLazyFields(List<? extends StructField> fieldRefs) {
    fields = new LazyObjectBase[fieldRefs.size()];
    for (int i = 0; i < fields.length; i++) {
      try {
        fields[i] = createLazyField(i, fieldRefs.get(i));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    fieldInited = new boolean[fields.length];
    // Extra element to make sure we have the same formula to compute the
    // length of each element of the array.
    startPosition = new int[fields.length + 1];
  }

  protected LazyObjectBase createLazyField(int fieldID, StructField fieldRef) throws SerDeException {
    return LazyFactory.createLazyObject(fieldRef.getFieldObjectInspector());
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
   * @param nullSequence
   *          The sequence representing NULL value.
   * @return The value of the field
   */
  private Object uncheckedGetField(int fieldID) {
    if (fieldInited[fieldID]) {
      return fields[fieldID].getObject();
    }
    fieldInited[fieldID] = true;

    int fieldByteBegin = startPosition[fieldID];
    int fieldLength = startPosition[fieldID + 1] - startPosition[fieldID] - 1;
    if (isNull(oi.getNullSequence(), bytes, fieldByteBegin, fieldLength)) {
      fields[fieldID].setNull();
    } else {
      fields[fieldID].init(bytes, fieldByteBegin, fieldLength);
    }
    return fields[fieldID].getObject();
  }

  private transient List<Object> cachedList;

  /**
   * Get the values of the fields as an ArrayList.
   *
   * @return The values of the fields as an ArrayList.
   */
  public List<Object> getFieldsAsList() {
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

  protected boolean getParsed() {
    return parsed;
  }

  protected void setParsed(boolean parsed) {
    this.parsed = parsed;
  }

  protected LazyObjectBase[] getFields() {
    return fields;
  }

  protected void setFields(LazyObject[] fields) {
    this.fields = fields;
  }

  protected boolean[] getFieldInited() {
    return fieldInited;
  }

  protected void setFieldInited(boolean[] fieldInited) {
    this.fieldInited = fieldInited;
  }

  public long getRawDataSerializedSize() {
    return serializedSize;
  }

  // parse the struct using multi-char delimiter
  public void parseMultiDelimit(byte[] rawRow, byte[] fieldDelimit) {
    if (rawRow == null || fieldDelimit == null) {
      return;
    }
    if (fields == null) {
      List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
      fields = new LazyObject[fieldRefs.size()];
      for (int i = 0; i < fields.length; i++) {
        fields[i] = LazyFactory.createLazyObject(fieldRefs.get(i).getFieldObjectInspector());
      }
      fieldInited = new boolean[fields.length];
      startPosition = new int[fields.length + 1];
    }
    // the indexes of the delimiters
    int[] delimitIndexes = findIndexes(rawRow, fieldDelimit);
    int diff = fieldDelimit.length - 1;
    // first field always starts from 0, even when missing
    startPosition[0] = 0;
    for (int i = 1; i < fields.length; i++) {
      if (delimitIndexes[i - 1] != -1) {
        int start = delimitIndexes[i - 1] + fieldDelimit.length;
        startPosition[i] = start - i * diff;
      } else {
        startPosition[i] = length + 1;
      }
    }
    startPosition[fields.length] = length + 1;
    Arrays.fill(fieldInited, false);
    parsed = true;
  }

  // find all the indexes of the sub byte[]
  private int[] findIndexes(byte[] array, byte[] target) {
    if (fields.length <= 1) {
      return new int[0];
    }
    int[] indexes = new int[fields.length - 1];
    Arrays.fill(indexes, -1);
    indexes[0] = Bytes.indexOf(array, target);
    if (indexes[0] == -1) {
      return indexes;
    }
    int indexInNewArray = indexes[0];
    for (int i = 1; i < indexes.length; i++) {
      array = Arrays.copyOfRange(array, indexInNewArray + target.length, array.length);
      indexInNewArray = Bytes.indexOf(array, target);
      if (indexInNewArray == -1) {
        break;
      }
      indexes[i] = indexInNewArray + indexes[i - 1] + target.length;
    }
    return indexes;
  }

  /**
   * Return the data in bytes corresponding to this given struct. This is useful specifically in
   * cases where the data is stored in serialized formats like protobufs or thrift and would need
   * custom deserializers to be deserialized.
   * */
  public byte[] getBytes() {
    return bytes.getData();
  }
}
