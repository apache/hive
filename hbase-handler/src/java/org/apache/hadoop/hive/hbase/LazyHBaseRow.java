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

package org.apache.hadoop.hive.hbase;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

/**
 * LazyObject for storing an HBase row.  The field of an HBase row can be
 * primitive or non-primitive.
 */
public class LazyHBaseRow extends LazyStruct {

  /**
   * The HBase columns mapping of the row.
   */
  private Result result;
  private ColumnMapping[] columnsMapping;
  private ArrayList<Object> cachedList;

  private final int iKey;
  private final HBaseKeyFactory keyFactory;

  public LazyHBaseRow(LazySimpleStructObjectInspector oi) {
    this(oi, -1, null);
  }

  /**
   * Construct a LazyHBaseRow object with the ObjectInspector.
   */
  public LazyHBaseRow(LazySimpleStructObjectInspector oi, int iKey, HBaseKeyFactory keyFactory) {
    super(oi);
    this.iKey = iKey;
    this.keyFactory = keyFactory;
  }

  /**
   * Set the HBase row data(a Result writable) for this LazyStruct.
   * @see LazyHBaseRow#init(org.apache.hadoop.hbase.client.Result)
   */
  public void init(Result r, ColumnMappings columnsMappings) {
    this.result = r;
    this.columnsMapping = columnsMappings.getColumnsMapping();
    setParsed(false);
  }

  @Override
  protected LazyObjectBase createLazyField(int fieldID, StructField fieldRef) throws SerDeException {
    if (fieldID == iKey) {
      return keyFactory.createKey(fieldRef.getFieldObjectInspector());
    }
    ColumnMapping colMap = columnsMapping[fieldID];
    if (colMap.qualifierName == null && !colMap.hbaseRowKey) {
      // a column family
      return new LazyHBaseCellMap((LazyMapObjectInspector) fieldRef.getFieldObjectInspector());
    }
    return LazyFactory.createLazyObject(fieldRef.getFieldObjectInspector(),
        colMap.binaryStorage.get(0));
  }

  /**
   * Get one field out of the HBase row.
   *
   * If the field is a primitive field, return the actual object.
   * Otherwise return the LazyObject.  This is because PrimitiveObjectInspector
   * does not have control over the object used by the user - the user simply
   * directly uses the Object instead of going through
   * Object PrimitiveObjectInspector.get(Object).
   *
   * @param fieldID  The field ID
   * @return         The field as a LazyObject
   */
  @Override
  public Object getField(int fieldID) {
    initFields();
    return uncheckedGetField(fieldID);
  }

  private void initFields() {
    if (getFields() == null) {
      initLazyFields(oi.getAllStructFieldRefs());
    }
    if (!getParsed()) {
      Arrays.fill(getFieldInited(), false);
      setParsed(true);
    }
  }

  /**
   * Get the field out of the row without checking whether parsing is needed.
   * This is called by both getField and getFieldsAsList.
   * @param fieldID  The id of the field starting from 0.
   * @param nullSequence  The sequence representing NULL value.
   * @return  The value of the field
   */
  private Object uncheckedGetField(int fieldID) {

    LazyObjectBase[] fields = getFields();
    boolean [] fieldsInited = getFieldInited();

    if (!fieldsInited[fieldID]) {
      ByteArrayRef ref = null;
      ColumnMapping colMap = columnsMapping[fieldID];

      if (colMap.hbaseRowKey) {
        ref = new ByteArrayRef();
        ref.setData(result.getRow());
      } else {
        if (colMap.qualifierName == null) {
          // it is a column family
          // primitive type for Map<Key, Value> can be stored in binary format. Pass in the
          // qualifier prefix to cherry pick the qualifiers that match the prefix instead of picking
          // up everything
          ((LazyHBaseCellMap) fields[fieldID]).init(
              result, colMap.familyNameBytes, colMap.binaryStorage, colMap.qualifierPrefixBytes);
        } else {
          // it is a column i.e. a column-family with column-qualifier
          byte [] res = result.getValue(colMap.familyNameBytes, colMap.qualifierNameBytes);

          if (res == null) {
            return null;
          } else {
            ref = new ByteArrayRef();
            ref.setData(res);
          }
        }
      }

      if (ref != null) {
        fields[fieldID].init(ref, 0, ref.getData().length);
      }
    }

    // Has to be set last because of HIVE-3179: NULL fields would not work otherwise
    fieldsInited[fieldID] = true;

    return fields[fieldID].getObject();
  }

  /**
   * Get the values of the fields as an ArrayList.
   * @return The values of the fields as an ArrayList.
   */
  @Override
  public ArrayList<Object> getFieldsAsList() {
    initFields();
    if (cachedList == null) {
      cachedList = new ArrayList<Object>();
    } else {
      cachedList.clear();
    }
    for (int i = 0; i < getFields().length; i++) {
      cachedList.add(uncheckedGetField(i));
    }
    return cachedList;
  }

  @Override
  public Object getObject() {
    return this;
  }
}
