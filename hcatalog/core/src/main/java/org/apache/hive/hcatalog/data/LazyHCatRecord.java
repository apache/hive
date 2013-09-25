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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of HCatRecord that wraps an Object returned by a SerDe
 * and an ObjectInspector.  This delays deserialization of unused columns.
 */
public class LazyHCatRecord extends HCatRecord {

  public static final Logger LOG = LoggerFactory.getLogger(LazyHCatRecord.class.getName());

  private Object wrappedObject;
  private StructObjectInspector soi;

  @Override
  public Object get(int fieldNum) {
    try {
      StructField fref = soi.getAllStructFieldRefs().get(fieldNum);
      return HCatRecordSerDe.serializeField(
        soi.getStructFieldData(wrappedObject, fref),
          fref.getFieldObjectInspector());
    } catch (SerDeException e) {
      throw new IllegalStateException("SerDe Exception deserializing",e);
    }
  }

  @Override
  public List<Object> getAll() {
    List<Object> r = new ArrayList<Object>(this.size());
    for (int i = 0; i < this.size(); i++){
      r.add(i, get(i));
    }
    return r;
  }

  @Override
  public void set(int fieldNum, Object value) {
    throw new UnsupportedOperationException("not allowed to run set() on LazyHCatRecord");
  }

  @Override
  public int size() {
    return soi.getAllStructFieldRefs().size();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException("LazyHCatRecord is intended to wrap"
      + " an object/object inspector as a HCatRecord "
      + "- it does not need to be read from DataInput.");
  }

  @Override
  public void write(DataOutput out) throws IOException {
    throw new UnsupportedOperationException("LazyHCatRecord is intended to wrap"
      + " an object/object inspector as a HCatRecord "
      + "- it does not need to be written to a DataOutput.");
  }

  @Override
  public Object get(String fieldName, HCatSchema recordSchema) throws HCatException {
    int idx = recordSchema.getPosition(fieldName);
    return get(idx);
  }

  @Override
  public void set(String fieldName, HCatSchema recordSchema, Object value) throws HCatException {
    throw new UnsupportedOperationException("not allowed to run set() on LazyHCatRecord");
  }

  @Override
  public void remove(int idx) throws HCatException {
    throw new UnsupportedOperationException("not allowed to run remove() on LazyHCatRecord");
  }

  @Override
  public void copy(HCatRecord r) throws HCatException {
    throw new UnsupportedOperationException("not allowed to run copy() on LazyHCatRecord");
  }

  public LazyHCatRecord(Object wrappedObject, ObjectInspector oi) throws Exception {
    if (oi.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString() +
        " can only make a lazy hcat record from " +
        "objects of struct types, but we got: " + oi.getTypeName());
    }

    this.soi = (StructObjectInspector)oi;
    this.wrappedObject = wrappedObject;
  }

  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    for(int i = 0; i< size() ; i++) {
      sb.append(get(i)+"\t");
    }
    return sb.toString();
  }

  /**
   * Convert this LazyHCatRecord to a DefaultHCatRecord.  This is required
   * before you can write out a record via write.
   * @return an HCatRecord that can be serialized
   * @throws HCatException
   */
  public HCatRecord getWritable() throws HCatException {
    DefaultHCatRecord d = new DefaultHCatRecord();
    d.copy(this);
    return d;
  }
}
