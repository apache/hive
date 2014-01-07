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
import org.apache.hadoop.hive.hbase.HBaseSerDe.ColumnMapping;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * LazyObject for storing an HBase row.  The field of an HBase row can be
 * primitive or non-primitive.
 */
public class LazyHBaseRow extends LazyStruct {

  /**
   * The HBase columns mapping of the row.
   */
  private Result result;
  private List<ColumnMapping> columnsMapping;
  private Object compositeKeyObj;
  private ArrayList<Object> cachedList;

  /**
   * Construct a LazyHBaseRow object with the ObjectInspector.
   */
  public LazyHBaseRow(LazySimpleStructObjectInspector oi) {
    super(oi);
  }

  /**
   * Set the HBase row data(a Result writable) for this LazyStruct.
   * @see LazyHBaseRow#init(Result)
   */
  public void init(Result r, List<ColumnMapping> columnsMapping) {
    init(r, columnsMapping, null);
  }

  /**
   * Set the HBase row data(a Result writable) for this LazyStruct.
   *
   * @see LazyHBaseRow#init(Result)
   *
   * @param compositeKeyClass
   *          custom implementation to interpret the composite key
   */
  public void init(Result r, List<ColumnMapping> columnsMapping, Object compositeKeyObj) {

    result = r;
    this.columnsMapping = columnsMapping;
    this.compositeKeyObj = compositeKeyObj;
    setParsed(false);
  }

  /**
   * Parse the Result and fill each field.
   * @see LazyStruct#parse()
   */
  private void parse() {

    if (getFields() == null) {
      List<? extends StructField> fieldRefs =
        ((StructObjectInspector)getInspector()).getAllStructFieldRefs();
      LazyObject<? extends ObjectInspector> [] fields = new LazyObject<?>[fieldRefs.size()];

      for (int i = 0; i < fields.length; i++) {
        ColumnMapping colMap = columnsMapping.get(i);

        if (colMap.qualifierName == null && !colMap.hbaseRowKey) {
          // a column family
          fields[i] = new LazyHBaseCellMap(
              (LazyMapObjectInspector) fieldRefs.get(i).getFieldObjectInspector());
          continue;
        }

        fields[i] = LazyFactory.createLazyObject(
            fieldRefs.get(i).getFieldObjectInspector(),
            colMap.binaryStorage.get(0));
      }

      setFields(fields);
      setFieldInited(new boolean[fields.length]);
    }

    Arrays.fill(getFieldInited(), false);
    setParsed(true);
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
    if (!getParsed()) {
      parse();
    }

    Object value = uncheckedGetField(fieldID);

    if (columnsMapping.get(fieldID).hbaseRowKey && compositeKeyObj != null) {
      return compositeKeyObj;
    } else {
      return value;
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

    LazyObject<?> [] fields = getFields();
    boolean [] fieldsInited = getFieldInited();

    if (!fieldsInited[fieldID]) {
      ByteArrayRef ref = null;
      ColumnMapping colMap = columnsMapping.get(fieldID);

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

        // if it was a row key and we have been provided a custom composite key class, initialize it
        // with the bytes for the row key
        if (colMap.hbaseRowKey && compositeKeyObj != null) {
          ((LazyStruct) compositeKeyObj).init(ref, 0, ref.getData().length);
        }
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
    if (!getParsed()) {
      parse();
    }
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
