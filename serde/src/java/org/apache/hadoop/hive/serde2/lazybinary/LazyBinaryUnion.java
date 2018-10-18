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
  package org.apache.hadoop.hive.serde2.lazybinary;

  import java.util.ArrayList;
  import java.util.Arrays;
  import java.util.List;

  import org.slf4j.Logger;
  import org.slf4j.LoggerFactory;
  import org.apache.hadoop.hive.serde2.SerDeStatsStruct;
  import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
  import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryUnionObjectInspector;
  import org.apache.hadoop.hive.serde2.objectinspector.*;

/**
 * LazyBinaryUnion is serialized as follows: start TAG FIELD end bytes[] -&gt;
 * |-----|---------|--- ... ---|-----|---------|
 *
 * Section TAG is one byte, corresponding to tag of set union field
 * FIELD is a LazyBinaryObject corresponding to set union field value.
 *
 */
  public class LazyBinaryUnion extends
          LazyBinaryNonPrimitive<LazyBinaryUnionObjectInspector> implements SerDeStatsStruct {

    private static Logger LOG = LoggerFactory.getLogger(LazyBinaryUnion.class.getName());

    /**
     * Whether the data is already parsed or not.
     */
    boolean parsed;

    /**
     * Size of serialized data
     */
    long serializedSize;

    /**
     * The field of the union which contains the value.
     */
    LazyBinaryObject field;

    boolean fieldInited;

    /**
     * The start positions and lengths of union fields. Only valid when the data
     * is parsed.
     */
    int fieldStart;
    int fieldLength;

    byte tag;

    final LazyBinaryUtils.VInt vInt = new LazyBinaryUtils.VInt();

    /**
     * Construct a LazyBinaryUnion object with an ObjectInspector.
     */
    protected LazyBinaryUnion(LazyBinaryUnionObjectInspector oi) {
      super(oi);
    }

    @Override
    public void init(ByteArrayRef bytes, int start, int length) {
      super.init(bytes, start, length);
      parsed = false;
      serializedSize = length;
      fieldInited = false;
      field = null;
      cachedObject = null;
    }

    LazyBinaryUtils.RecordInfo recordInfo = new LazyBinaryUtils.RecordInfo();
    boolean missingFieldWarned = false;
    boolean extraFieldWarned = false;

    /**
     * Parse the byte[] and fill fieldStart, fieldLength, fieldInited and
     * fieldIsNull.
     */
    private void parse() {
      LazyBinaryUnionObjectInspector uoi = (LazyBinaryUnionObjectInspector) oi;

      /**
       * Please note that tag is followed by field
       */
      int unionByteEnd = start + length;
      byte[] byteArr = this.bytes.getData();

      //Tag of union field is the first byte to be parsed
      final int tagEnd = start + 1;
      tag = byteArr[start];
      field = LazyBinaryFactory.createLazyBinaryObject(uoi.getObjectInspectors().get(tag));
      //Check the union field's length and offset
      LazyBinaryUtils.checkObjectByteInfo(uoi.getObjectInspectors().get(tag), byteArr, tagEnd, recordInfo, vInt);
      fieldStart = tagEnd + recordInfo.elementOffset;
      // Add 1 for tag
      fieldLength = recordInfo.elementSize;

      // Extra bytes at the end?
      if (!extraFieldWarned &&  (fieldStart + fieldLength) < unionByteEnd) {
        extraFieldWarned = true;
        LOG.warn("Extra bytes detected at the end of the row! Ignoring similar "
                         + "problems.");
      }

      // Missing fields?
      if (!missingFieldWarned && (fieldStart + fieldLength) > unionByteEnd) {
        missingFieldWarned = true;
        LOG.info("Missing fields! Expected 1 fields but "
                         + "only got " + field + "! Ignoring similar problems.");
      }

      parsed = true;
    }

    /**
     * Get the set field out of the union.
     *
     * If the field is a primitive field, return the actual object. Otherwise
     * return the LazyObject. This is because PrimitiveObjectInspector does not
     * have control over the object used by the user - the user simply directly
     * use the Object instead of going through Object
     * PrimitiveObjectInspector.get(Object).
     * @return The field as a LazyObject
     */
    public Object getField() {
      if (!parsed) {
        parse();
      }
      if(cachedObject == null) {
        return uncheckedGetField();
      }
      return cachedObject;
    }

    /**
     * Get the field out of the row without checking parsed. This is called by
     * both getField and getFieldsAsList.
     *
     * @param fieldID
     *          The id of the field starting from 0.
     * @return The value of the field
     */
    private Object uncheckedGetField() {
      // Test the length first so in most cases we avoid doing a byte[]
      // comparison.
      if (!fieldInited) {
        fieldInited = true;
        field.init(bytes, fieldStart, fieldLength);
      }
      cachedObject = field.getObject();
      return field.getObject();
    }

    Object cachedObject;

    @Override
    public Object getObject() {
      return this;
    }

    public long getRawDataSerializedSize() {
      return serializedSize;
    }

  /**
   * Get the set field's tag
   *
   *
   * @return The tag of the field set in the union
   */
  public byte getTag() {
    if (!parsed) {
      parse();
    }
    return tag;
  }
  }

