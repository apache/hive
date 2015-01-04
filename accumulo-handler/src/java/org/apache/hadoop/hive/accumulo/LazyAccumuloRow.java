/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloMapColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloRowIdColumnMapping;
import org.apache.hadoop.hive.accumulo.serde.AccumuloRowIdFactory;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 *
 * Parses column tuples in each AccumuloHiveRow and creates Lazy objects for each field.
 *
 */
public class LazyAccumuloRow extends LazyStruct {
  private static final Logger log = Logger.getLogger(LazyAccumuloRow.class);

  private AccumuloHiveRow row;
  private List<ColumnMapping> columnMappings;
  private ArrayList<Object> cachedList = new ArrayList<Object>();
  private AccumuloRowIdFactory rowIdFactory;

  public LazyAccumuloRow(LazySimpleStructObjectInspector inspector) {
    super(inspector);
  }

  public void init(AccumuloHiveRow hiveRow, List<ColumnMapping> columnMappings,
      AccumuloRowIdFactory rowIdFactory) {
    this.row = hiveRow;
    this.columnMappings = columnMappings;
    this.rowIdFactory = rowIdFactory;
    setParsed(false);
  }

  private void parse() {
    if (getFields() == null) {
      // Will properly set string or binary serialization via createLazyField(...)
      initLazyFields(oi.getAllStructFieldRefs());
    }
    if (!getParsed()) {
      Arrays.fill(getFieldInited(), false);
      setParsed(true);
    }
  }

  @Override
  public Object getField(int id) {
    if (!getParsed()) {
      parse();
    }
    return uncheckedGetField(id);
  }

  /*
   * split pairs by delimiter.
   */
  private Object uncheckedGetField(int id) {
    if (getFieldInited()[id]) {
      return getFields()[id].getObject();
    }
    getFieldInited()[id] = true;

    ColumnMapping columnMapping = columnMappings.get(id);

    LazyObjectBase field = getFields()[id];

    if (columnMapping instanceof HiveAccumuloMapColumnMapping) {
      HiveAccumuloMapColumnMapping mapColumnMapping = (HiveAccumuloMapColumnMapping) columnMapping;

      LazyAccumuloMap map = (LazyAccumuloMap) field;
      map.init(row, mapColumnMapping);
    } else {
      byte[] value;
      if (columnMapping instanceof HiveAccumuloRowIdColumnMapping) {
        // Use the rowID directly
        value = row.getRowId().getBytes();
      } else if (columnMapping instanceof HiveAccumuloColumnMapping) {
        HiveAccumuloColumnMapping accumuloColumnMapping = (HiveAccumuloColumnMapping) columnMapping;

        // Use the colfam and colqual to get the value
        value = row.getValue(
            new Text(accumuloColumnMapping.getColumnFamilyBytes()),
            new Text(accumuloColumnMapping.getColumnQualifierBytes()));
      } else {
        log.error("Could not process ColumnMapping of type " + columnMapping.getClass()
            + " at offset " + id + " in column mapping: " + columnMapping.getMappingSpec());
        throw new IllegalArgumentException("Cannot process ColumnMapping of type "
            + columnMapping.getClass());
      }
      if (value == null || isNull(oi.getNullSequence(), value, 0, value.length)) {
        field.setNull();
      } else {
        ByteArrayRef ref = new ByteArrayRef();
        ref.setData(value);
        field.init(ref, 0, value.length);
      }
    }

    return field.getObject();
  }

  @Override
  public ArrayList<Object> getFieldsAsList() {
    if (!getParsed()) {
      parse();
    }
    cachedList.clear();
    for (int i = 0; i < getFields().length; i++) {
      cachedList.add(uncheckedGetField(i));
    }
    return cachedList;
  }

  @Override
  protected LazyObjectBase createLazyField(int fieldID, StructField fieldRef) throws SerDeException {
    final ColumnMapping columnMapping = columnMappings.get(fieldID);

    if (columnMapping instanceof HiveAccumuloRowIdColumnMapping) {
      return rowIdFactory.createRowId(fieldRef.getFieldObjectInspector());
    } else if (columnMapping instanceof HiveAccumuloMapColumnMapping) {
      return new LazyAccumuloMap((LazyMapObjectInspector) fieldRef.getFieldObjectInspector());
    } else {
      return LazyFactory.createLazyObject(fieldRef.getFieldObjectInspector(),
          ColumnEncoding.BINARY == columnMapping.getEncoding());
    }
  }
}
