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

import java.nio.charset.CharacterCodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;


/**
 * LazyObject for storing a struct.
 * The field of a struct can be primitive or non-primitive.
 * 
 */
public class LazyStruct extends LazyObject {

  
  private static Log LOG = LogFactory.getLog(LazyStruct.class.getName());
  
  LazyObject[] fields;
  boolean[] fieldIsPrimitive;
  
  byte separator;
  Text nullSequence;
  boolean lastColumnTakesAll;
  
  boolean parsed;
  
  /**
   * Create a new LazyStruct Object.
   * @param fields     The field LazyObjects
   * @param separator  The separator for delimiting the fields in the byte[]
   * @param nullSequence  The sequence for null value
   * @param lastColumnTakesAll  whether the additional fields should be all put into the last column
   *                            in case the data contains more columns than the schema.  
   */
  public LazyStruct(LazyObject[] fields, byte separator,
      Text nullSequence, boolean lastColumnTakesAll) {
    this.fields = fields;
    this.separator = separator;
    this.nullSequence = nullSequence;
    this.lastColumnTakesAll = lastColumnTakesAll; 
      
    parsed = false;
    fieldIsPrimitive = new boolean[fields.length];
    for(int i=0; i<fields.length; i++) {
      fieldIsPrimitive[i] = (fields[i] instanceof LazyPrimitive);
    }
  }
  
  /**
   * Set the row data for this LazyStruct.
   */
  protected void setAll(byte[] bytes, int start, int length) {
    super.setAll(bytes, start, length);
    parsed = false;
  }
  
  
  boolean missingFieldWarned = false;
  boolean extraFieldWarned = false;
  /**
   * Parse the byte[] and fill each field.
   */
  private void parse() {
    
    int structByteEnd = start + length;
    int fieldId = 0;
    int fieldByteBegin = start;
    int fieldByteEnd = start;
    
    // Go through all bytes in the byte[]
    while (fieldByteEnd <= structByteEnd) {
      if (fieldByteEnd == structByteEnd || bytes[fieldByteEnd] == separator) {
        // end of field reached
        if (lastColumnTakesAll && fieldId == fields.length - 1) {
          fieldByteEnd = structByteEnd;
        }
        // Test the length first so in most cases we avoid doing a byte[] comparison.
        int fieldLength = fieldByteEnd - fieldByteBegin;
        if (fieldLength == nullSequence.getLength()
            && LazyUtils.compare(bytes, fieldByteBegin, fieldLength,
            nullSequence.getBytes(), 0, nullSequence.getLength()) == 0) {
          fields[fieldId].setAll(null, 0, 0);
        } else {
          fields[fieldId].setAll(bytes, fieldByteBegin,
              fieldByteEnd - fieldByteBegin);
        }
        fieldId ++;
        if (fieldId == fields.length || fieldByteEnd == structByteEnd) {
          // all fields have been parsed, or all bytes have been parsed 
          break;
        }
        fieldByteBegin = fieldByteEnd + 1;
      }
      fieldByteEnd++;
    }
    
    // Extra bytes at the end?
    if (!extraFieldWarned && fieldByteEnd < structByteEnd) {
      extraFieldWarned = true;
      LOG.warn("Extra bytes detected at the end of the row! Ignoring similar problems.");
    }
    
    // Missing fields?
    if (!missingFieldWarned && fieldId < fields.length) {
      missingFieldWarned = true;
      LOG.warn("Missing fields! Expected " + fields.length + " fields but only got "
          + fieldId + "! Ignoring similar problems.");
    }
    
    // Fill all missing fields with nulls.
    for(; fieldId < fields.length; fieldId ++) {
      fields[fieldId].setAll(null, 0, 0);
    }
    
    parsed = true;
  }
  
  /**
   * Get one field out of the struct.
   * 
   * If the field is a primitive field, return the actual object.
   * Otherwise return the LazyObject.  This is because PrimitiveObjectInspector
   * does not have control over the object used by the user - the user simply
   * directly use the Object instead of going through 
   * Object PrimitiveObjectInspector.get(Object).  
   * 
   * @param i  the field ID
   * @return   the field as a LazyObject
   */
  public Object getField(int i) {
    if (!parsed) {
      parse();
    }
    if (!fieldIsPrimitive[i]) {
      return fields[i];
    } else {
      return ((LazyPrimitive)fields[i]).getPrimitiveObject();
    }
  }
}
