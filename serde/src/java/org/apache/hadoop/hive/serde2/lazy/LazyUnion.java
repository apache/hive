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
package org.apache.hadoop.hive.serde2.lazy;

import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyUnionObjectInspector;

/**
 * LazyObject for storing a union. The field of a union can be primitive or
 * non-primitive.
 *
 */
public class LazyUnion extends LazyNonPrimitive<LazyUnionObjectInspector> {
  /**
   * Whether the data is already parsed or not.
   */
  private boolean parsed;

  /**
   * The start position of union field. Only valid when the data is parsed.
   */
  private int startPosition;

  /**
   * The object of the union.
   */
  private Object field;

  /**
   * Tag of the Union
   */
  private byte tag;

  /**
   * Whether init() has been called on the field or not.
   */
  private boolean fieldInited = false;

  /**
   * Whether the field has been set or not
   * */
  private boolean fieldSet = false;

  /**
   * Construct a LazyUnion object with the ObjectInspector.
   */
  public LazyUnion(LazyUnionObjectInspector oi) {
    super(oi);
  }

  // exceptional use case for avro
  public LazyUnion(LazyUnionObjectInspector oi, byte tag, Object field) {
    super(oi);
    this.field = field;
    this.tag = tag;
    fieldSet = true;
  }

  /**
   * Set the row data for this LazyUnion.
   *
   * @see LazyObject#init(ByteArrayRef, int, int)
   */
  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    parsed = false;
  }

  /**
   * Parse the byte[] and fill each field.
   */
  @SuppressWarnings("unchecked")
  private void parse() {

    byte separator = oi.getSeparator();
    boolean isEscaped = oi.isEscaped();
    byte escapeChar = oi.getEscapeChar();
    boolean tagStarted = false;
    boolean tagParsed = false;
    int tagStart = -1;
    int tagEnd = -1;

    int unionByteEnd = start + length;
    int fieldByteEnd = start;
    byte[] bytes = this.bytes.getData();
    // Go through all bytes in the byte[]
    while (fieldByteEnd < unionByteEnd) {
      if (bytes[fieldByteEnd] != separator) {
        if (isEscaped && bytes[fieldByteEnd] == escapeChar
            && fieldByteEnd + 1 < unionByteEnd) {
          // ignore the char after escape_char
          fieldByteEnd += 1;
        } else {
          if (!tagStarted) {
            tagStart = fieldByteEnd;
            tagStarted = true;
          }
        }
      } else { // (bytes[fieldByteEnd] == separator)
        if (!tagParsed) {
          // Reached the end of the tag
          tagEnd = fieldByteEnd - 1;
          startPosition = fieldByteEnd + 1;
          tagParsed = true;
        }
      }
      fieldByteEnd++;
    }

    tag = LazyByte.parseByte(bytes, tagStart, (tagEnd - tagStart) + 1);
    field = LazyFactory.createLazyObject(oi.getObjectInspectors().get(tag));
    fieldInited = false;
    parsed = true;
  }

  /**
   * Get the field out of the row without checking parsed.
   *
   * @return The value of the field
   */
  @SuppressWarnings("rawtypes")
  private Object uncheckedGetField() {
    LazyObject field = (LazyObject) this.field;
    if (fieldInited) {
      return field.getObject();
    }
    fieldInited = true;

    int fieldStart = startPosition;
    int fieldLength = start + length - startPosition;
    if (isNull(oi.getNullSequence(), bytes, fieldStart, fieldLength)) {
      field.setNull();
    } else {
      field.init(bytes, fieldStart, fieldLength);
    }
    return field.getObject();
  }

  /**
   * Get the field out of the union.
   *
   * @return The field as a LazyObject
   */
  public Object getField() {
    if (fieldSet) {
      return field;
    }

    if (!parsed) {
      parse();
    }
    return uncheckedGetField();
  }

  /**
   * Get the tag of the union
   *
   * @return The tag byte
   */
  public byte getTag() {
    if (fieldSet) {
      return tag;
    }

    if (!parsed) {
      parse();
    }
    return tag;
  }
}