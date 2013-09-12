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

package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Writable;

@SuppressWarnings("deprecation")
public class MapJoinKey {
  private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

  private Object[] key;
  
  public MapJoinKey(Object[] key) {
    this.key = key;
  }
  public MapJoinKey() {
    this(EMPTY_OBJECT_ARRAY);
  }

  public Object[] getKey() {
    return key;
  }
  public boolean hasAnyNulls(boolean[] nullsafes){
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
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(key);
    return result;
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MapJoinKey other = (MapJoinKey) obj;
    if (!Arrays.equals(key, other.key))
      return false;
    return true;
  }
  @SuppressWarnings("unchecked")
  public void read(MapJoinObjectSerDeContext context, ObjectInputStream in, Writable container) 
  throws IOException, SerDeException {
    SerDe serde = context.getSerDe();
    container.readFields(in);
    List<Object> value = (List<Object>)ObjectInspectorUtils.copyToStandardObject(serde.deserialize(container),
        serde.getObjectInspector(), ObjectInspectorCopyOption.WRITABLE);
    if(value == null) {
      key = EMPTY_OBJECT_ARRAY;
    } else {
      key = value.toArray();
    }
  }
  
  public void write(MapJoinObjectSerDeContext context, ObjectOutputStream out) 
  throws IOException, SerDeException {
    SerDe serde = context.getSerDe();
    ObjectInspector objectInspector = context.getStandardOI();
    Writable container = serde.serialize(key, objectInspector);
    container.write(out);
  }
}
