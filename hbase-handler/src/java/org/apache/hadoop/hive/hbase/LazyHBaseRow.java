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
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.hbase.struct.HBaseValueFactory;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.LazyTimestamp;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

import com.google.common.annotations.VisibleForTesting;

/**
 * LazyObject for storing an HBase row.  The field of an HBase row can be
 * primitive or non-primitive.
 */
public class LazyHBaseRow extends LazyStruct {

  /**
   * The HBase columns mapping of the row.
   */
  private Result result;
  private ArrayList<Object> cachedList;

  private final HBaseKeyFactory keyFactory;
  private final List<HBaseValueFactory> valueFactories;
  private final ColumnMapping[] columnsMapping;

  @VisibleForTesting
  LazyHBaseRow(LazySimpleStructObjectInspector oi, ColumnMappings columnMappings) {
    super(oi);
    this.keyFactory = DefaultHBaseKeyFactory.forTest(null, columnMappings);
    this.valueFactories = null;
    this.columnsMapping = columnMappings.getColumnsMapping();
  }

  /**
   * Construct a LazyHBaseRow object with the ObjectInspector.
   */
  public LazyHBaseRow(LazySimpleStructObjectInspector oi, HBaseSerDeParameters serdeParams) {
    super(oi);
    this.keyFactory = serdeParams.getKeyFactory();
    this.valueFactories = serdeParams.getValueFactories();
    this.columnsMapping = serdeParams.getColumnMappings().getColumnsMapping();
  }

  /**
   * Set the HBase row data(a Result writable) for this LazyStruct.
   * @see LazyHBaseRow#init(org.apache.hadoop.hbase.client.Result)
   */
  public void init(Result r) {
    this.result = r;
    setParsed(false);
  }

  @Override
  protected LazyObjectBase createLazyField(final int fieldID, final StructField fieldRef)
      throws SerDeException {
    if (columnsMapping[fieldID].hbaseRowKey) {
      return keyFactory.createKey(fieldRef.getFieldObjectInspector());
    }
    if (columnsMapping[fieldID].hbaseTimestamp) {
      return LazyFactory.createLazyObject(fieldRef.getFieldObjectInspector());
    }

    if (valueFactories != null) {
      return valueFactories.get(fieldID).createValueObject(fieldRef.getFieldObjectInspector());
    }

    // fallback to default
    return HBaseSerDeHelper.createLazyField(columnsMapping, fieldID,
        fieldRef.getFieldObjectInspector());
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
   * @return  The value of the field
   */
  private Object uncheckedGetField(int fieldID) {

    LazyObjectBase[] fields = getFields();
    boolean [] fieldsInited = getFieldInited();

    if (!fieldsInited[fieldID]) {
      fieldsInited[fieldID] = true;

      ColumnMapping colMap = columnsMapping[fieldID];

      if (!colMap.hbaseRowKey && !colMap.hbaseTimestamp && colMap.qualifierName == null) {
        // it is a column family
        // primitive type for Map<Key, Value> can be stored in binary format. Pass in the
        // qualifier prefix to cherry pick the qualifiers that match the prefix instead of picking
        // up everything
        ((LazyHBaseCellMap) fields[fieldID]).init(
            result, colMap.familyNameBytes, colMap.binaryStorage, colMap.qualifierPrefixBytes, colMap.isDoPrefixCut());
        return fields[fieldID].getObject();
      }

      if (colMap.hbaseTimestamp) {
        // Get the latest timestamp of all the cells as the row timestamp
        long timestamp = result.rawCells()[0].getTimestamp(); // from hbase-0.96.0
        for (int i = 1; i < result.rawCells().length; i++) {
          timestamp = Math.max(timestamp, result.rawCells()[i].getTimestamp());
        }
        LazyObjectBase lz = fields[fieldID];
        if (lz instanceof LazyTimestamp) {
          ((LazyTimestamp) lz).getWritableObject().setTime(timestamp);
        } else {
          ((LazyLong) lz).getWritableObject().set(timestamp);
        }
        return lz.getObject();
      }

      byte[] bytes;
      if (colMap.hbaseRowKey) {
        bytes = result.getRow();
      } else {
        // it is a column i.e. a column-family with column-qualifier
        bytes = result.getValue(colMap.familyNameBytes, colMap.qualifierNameBytes);
      }
      if (bytes == null || isNull(oi.getNullSequence(), bytes, 0, bytes.length)) {
        fields[fieldID].setNull();
      } else {
        ByteArrayRef ref = new ByteArrayRef();
        ref.setData(bytes);
        fields[fieldID].init(ref, 0, bytes.length);
      }
    }

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
