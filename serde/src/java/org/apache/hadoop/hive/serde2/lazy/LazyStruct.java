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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;


/**
 * LazyObject for storing a struct.
 * The field of a struct can be primitive or non-primitive.
 *
 * LazyStruct does not deal with the case of a NULL struct. That is handled
 * by LazySimpleStructObjectInspector.
 */
public class LazyStruct extends LazyNonPrimitive {

  private static Log LOG = LogFactory.getLog(LazyStruct.class.getName());

  /**
   * Whether the data is already parsed or not.
   */
  boolean parsed;

  /**
   * The start positions of struct fields.
   * Only valid when the data is parsed.
   * Note that startPosition[arrayLength] = begin + length + 1;
   * that makes sure we can use the same formula to compute the
   * length of each element of the array.
   */
  int[] startPosition;
  
  /**
   * The fields of the struct.
   */
  LazyObject[] fields;
  /**
   * Whether init() has been called on the field or not.
   */
  boolean[] fieldInited;
  
  /**
   * Construct a LazyStruct object with the TypeInfo.
   * @param typeInfo  the TypeInfo representing the type of this LazyStruct.
   */
  public LazyStruct(TypeInfo typeInfo) {
    super(typeInfo);
  }
  
  /**
   * Set the row data for this LazyStruct.
   * @see LazyObject#init(ByteArrayRef, int, int)
   */
  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    parsed = false;
  }

  boolean missingFieldWarned = false;
  boolean extraFieldWarned = false;
  /**
   * Parse the byte[] and fill each field.
   * @param separator  The separator for delimiting the fields in the byte[]
   * @param nullSequence  The sequence for the NULL value
   * @param lastColumnTakesRest  Whether the additional fields should be all 
   *                             put into the last column in case the data 
   *                             contains more columns than the schema.  
   */
  private void parse(byte separator, Text nullSequence, 
      boolean lastColumnTakesRest) {
    
    if (fields == null) {
      List<TypeInfo> fieldTypeInfos = ((StructTypeInfo)typeInfo).getAllStructFieldTypeInfos();
      fields = new LazyObject[fieldTypeInfos.size()];
      for (int i = 0 ; i < fields.length; i++) {
        fields[i] = LazyFactory.createLazyObject(fieldTypeInfos.get(i));
      }
      fieldInited = new boolean[fields.length];      
      // Extra element to make sure we have the same formula to compute the 
      // length of each element of the array. 
      startPosition = new int[fields.length+1];
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
        fieldId ++;
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
      }
      fieldByteEnd++;
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
      LOG.warn("Missing fields! Expected " + fields.length + " fields but "
          + "only got " + fieldId + "! Ignoring similar problems.");
    }
    
    Arrays.fill(fieldInited, false);
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
   * NOTE: separator and nullSequence has to be the same each time 
   * this method is called.  These two parameters are used only once to parse
   * each record.
   * 
   * @param fieldID  The field ID
   * @param separator  The separator for delimiting the fields in the byte[]
   * @param nullSequence  The sequence for null value
   * @param lastColumnTakesRest  Whether the additional fields should be all 
   *                             put into the last column in case the data 
   *                             contains more columns than the schema.  
   * @return         The field as a LazyObject
   */
  public Object getField(int fieldID, byte separator, Text nullSequence, 
      boolean lastColumnTakesRest) {
    if (!parsed) {
      parse(separator, nullSequence, lastColumnTakesRest);
    }
    return uncheckedGetField(fieldID, nullSequence);
  }

  /**
   * Get the field out of the row without checking parsed.
   * This is called by both getField and getFieldsAsList.
   * @param fieldID  The id of the field starting from 0.
   * @param nullSequence  The sequence representing NULL value.
   * @return  The value of the field 
   */
  private Object uncheckedGetField(int fieldID, Text nullSequence) {
    // Test the length first so in most cases we avoid doing a byte[] 
    // comparison.
    int fieldByteBegin = startPosition[fieldID];
    int fieldLength = startPosition[fieldID+1] - startPosition[fieldID] - 1;
    if ((fieldLength < 0)
        || (fieldLength == nullSequence.getLength()
           && LazyUtils.compare(bytes.getData(), fieldByteBegin, fieldLength,
          nullSequence.getBytes(), 0, nullSequence.getLength()) == 0)) {
      return null;
    }
    if (!fieldInited[fieldID]) {
      fieldInited[fieldID] = true;
      fields[fieldID].init(bytes, fieldByteBegin, fieldLength);
    }
    return fields[fieldID].getObject();
  }

  ArrayList<Object> cachedList;
  /**
   * Get the values of the fields as an ArrayList.
   * @param separator  The separator for delimiting the fields in the byte[]
   * @param nullSequence         The sequence for the NULL value
   * @param lastColumnTakesRest  Whether the additional fields should be all 
   *                             put into the last column in case the data 
   *                             contains more columns than the schema.  
   * @return The values of the fields as an ArrayList.
   */
  public ArrayList<Object> getFieldsAsList(byte separator, Text nullSequence, 
      boolean lastColumnTakesRest) {
    if (!parsed) {
      parse(separator, nullSequence, lastColumnTakesRest);
    }
    if (cachedList == null) {
      cachedList = new ArrayList<Object>();
    } else {
      cachedList.clear();
    }
    for (int i=0; i<fields.length; i++) {
      cachedList.add(uncheckedGetField(i, nullSequence));
    }
    return cachedList;
  }
  
  @Override
  public Object getObject() {
    return this;
  }

}
