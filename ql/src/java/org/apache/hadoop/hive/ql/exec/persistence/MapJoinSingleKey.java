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
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.MapJoinMetaData;
import org.apache.hadoop.hive.ql.exec.HashTableSinkOperator.HashTableSinkObjectCtx;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Writable;

public class MapJoinSingleKey extends AbstractMapJoinKey {


  protected transient Object obj;

  public MapJoinSingleKey() {
  }

  /**
   * @param obj
   */
  public MapJoinSingleKey(Object obj) {
    this.obj = obj;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof MapJoinSingleKey) {
      MapJoinSingleKey mObj = (MapJoinSingleKey) o;
      Object key = mObj.getObj();
      if ((obj == null) && (key == null)) {
        return true;
      }
      if ((obj != null) && (key != null)) {
        if (obj.equals(key)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode;
    if (obj == null) {
      hashCode = metadataTag;
    } else {
      hashCode = 31 + obj.hashCode();
    }
    return hashCode;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    try {
      // get the tableDesc from the map stored in the mapjoin operator
      HashTableSinkObjectCtx ctx = MapJoinMetaData.get(Integer.valueOf(metadataTag));

      Writable val = ctx.getSerDe().getSerializedClass().newInstance();
      val.readFields(in);



      ArrayList<Object> list = (ArrayList<Object>) ObjectInspectorUtils.copyToStandardObject(ctx
          .getSerDe().deserialize(val), ctx.getSerDe().getObjectInspector(),
          ObjectInspectorCopyOption.WRITABLE);

      if (list == null) {
        obj = null;
        System.out.println("read empty back");
      } else {
        obj = list.get(0);
      }

    } catch (Exception e) {
      throw new IOException(e);
    }

  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    try {
      // out.writeInt(metadataTag);
      // get the tableDesc from the map stored in the mapjoin operator
      HashTableSinkObjectCtx ctx = MapJoinMetaData.get(Integer.valueOf(metadataTag));

      ArrayList<Object> list = MapJoinMetaData.getList();
      list.add(obj);

      // Different processing for key and value
      Writable outVal = ctx.getSerDe().serialize(list, ctx.getStandardOI());
      outVal.write(out);

    } catch (SerDeException e) {
      throw new IOException(e);
    }
  }



  /**
   * @return the obj
   */
  public Object getObj() {
    return obj;
  }

  /**
   * @param obj
   *          the obj to set
   */
  public void setObj(Object obj) {
    this.obj = obj;
  }

  @Override
  public boolean hasAnyNulls(boolean[] nullsafes) {
    if (obj == null && (nullsafes == null || !nullsafes[0])) {
      return true;
    }
    return false;
  }



}
