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

package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.vector.VectorHashKeyWrapper;
import org.apache.hadoop.hive.ql.exec.vector.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Writable;

@SuppressWarnings("deprecation")
public class MapJoinKeyObject extends MapJoinKey {
  private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

  private Object[] key;

  public MapJoinKeyObject(Object[] key) {
    this.key = key;
  }

  public MapJoinKeyObject() {
    this(EMPTY_OBJECT_ARRAY);
  }

  public Object[] getKeyObjects() {
    return key;
  }

  public void setKeyObjects(Object[] key) {
    this.key = key;
  }

  public int getKeyLength() {
    return key.length;
  }

  @Override
  public boolean hasAnyNulls(int fieldCount, boolean[] nullsafes) {
    assert fieldCount == key.length;
    if (key != null && key.length > 0) {
      for (int i = 0; i < key.length; i++) {
        if (key[i] == null && (nullsafes == null || !nullsafes[i])) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return ObjectInspectorUtils.writableArrayHashCode(key);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MapJoinKeyObject other = (MapJoinKeyObject) obj;
    if (!Arrays.equals(key, other.key))
      return false;
    return true;
  }

  public void read(MapJoinObjectSerDeContext context, ObjectInputStream in, Writable container)
  throws IOException, SerDeException {
    container.readFields(in);
    read(context, container);
  }

  public void read(MapJoinObjectSerDeContext context, Writable container) throws SerDeException {
    read(context.getSerDe().getObjectInspector(), context.getSerDe().deserialize(container));
  }

  protected void read(ObjectInspector oi, Object obj) throws SerDeException {
    @SuppressWarnings("unchecked")
    List<Object> value = (List<Object>)ObjectInspectorUtils.copyToStandardObject(
        obj, oi, ObjectInspectorCopyOption.WRITABLE);
    if (value == null) {
      key = EMPTY_OBJECT_ARRAY;
    } else {
      key = value.toArray();
    }
  }

  @Override
  public void write(MapJoinObjectSerDeContext context, ObjectOutputStream out)
      throws IOException, SerDeException {
    AbstractSerDe serde = context.getSerDe();
    ObjectInspector objectInspector = context.getStandardOI();
    Writable container = serde.serialize(key, objectInspector);
    container.write(out);
  }

  public void readFromRow(Object[] fieldObjs, List<ObjectInspector> keyFieldsOI)
      throws HiveException {
    if (key == null || key.length != fieldObjs.length) {
      key = new Object[fieldObjs.length];
    }
    for (int keyIndex = 0; keyIndex < fieldObjs.length; ++keyIndex) {
      key[keyIndex] = (ObjectInspectorUtils.copyToStandardObject(fieldObjs[keyIndex],
          keyFieldsOI.get(keyIndex), ObjectInspectorCopyOption.WRITABLE));
    }
  }

  protected boolean[] getNulls() {
    boolean[] nulls = null;
    for (int i = 0; i < key.length; ++i) {
      if (key[i] == null) {
        if (nulls == null) {
          nulls = new boolean[key.length];
        }
        nulls[i] = true;
      }
    }
    return nulls;
  }

  public void readFromVector(VectorHashKeyWrapper kw, VectorExpressionWriter[] keyOutputWriters,
      VectorHashKeyWrapperBatch keyWrapperBatch) throws HiveException {
    if (key == null || key.length != keyOutputWriters.length) {
      key = new Object[keyOutputWriters.length];
    }
    for (int keyIndex = 0; keyIndex < keyOutputWriters.length; ++keyIndex) {
      key[keyIndex] = keyWrapperBatch.getWritableKeyValue(
          kw, keyIndex, keyOutputWriters[keyIndex]);
    }
  }
}
