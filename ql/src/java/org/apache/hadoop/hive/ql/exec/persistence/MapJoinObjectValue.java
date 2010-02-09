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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator.MapJoinObjectCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Writable;

/**
 * Map Join Object used for both key and value.
 */
public class MapJoinObjectValue implements Externalizable {

  protected transient int metadataTag;
  protected transient RowContainer obj;
  protected transient Configuration conf;

  public MapJoinObjectValue() {
  }

  /**
   * @param metadataTag
   * @param obj
   */
  public MapJoinObjectValue(int metadataTag, RowContainer obj) {
    this.metadataTag = metadataTag;
    this.obj = obj;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof MapJoinObjectValue) {
      MapJoinObjectValue mObj = (MapJoinObjectValue) o;
      if (mObj.getMetadataTag() == metadataTag) {
        if ((obj == null) && (mObj.getObj() == null)) {
          return true;
        }
        if ((obj != null) && (mObj.getObj() != null)
            && (mObj.getObj().equals(obj))) {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public int hashCode() {
    return (obj == null) ? 0 : obj.hashCode();
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
    try {
      metadataTag = in.readInt();

      // get the tableDesc from the map stored in the mapjoin operator
      MapJoinObjectCtx ctx = MapJoinOperator.getMapMetadata().get(
          Integer.valueOf(metadataTag));
      int sz = in.readInt();

      RowContainer res = new RowContainer(ctx.getConf());
      res.setSerDe(ctx.getSerDe(), ctx.getStandardOI());
      res.setTableDesc(ctx.getTblDesc());
      for (int pos = 0; pos < sz; pos++) {
        Writable val = ctx.getSerDe().getSerializedClass().newInstance();
        val.readFields(in);

        ArrayList<Object> memObj = (ArrayList<Object>) ObjectInspectorUtils
            .copyToStandardObject(ctx.getSerDe().deserialize(val), ctx
            .getSerDe().getObjectInspector(),
            ObjectInspectorCopyOption.WRITABLE);

        res.add(memObj);
      }
      obj = res;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    try {

      out.writeInt(metadataTag);

      // get the tableDesc from the map stored in the mapjoin operator
      MapJoinObjectCtx ctx = MapJoinOperator.getMapMetadata().get(
          Integer.valueOf(metadataTag));

      // Different processing for key and value
      RowContainer<ArrayList<Object>> v = obj;
      out.writeInt(v.size());

      for (ArrayList<Object> row = v.first(); row != null; row = v.next()) {
        Writable outVal = ctx.getSerDe().serialize(row, ctx.getStandardOI());
        outVal.write(out);
      }
    } catch (SerDeException e) {
      throw new IOException(e);
    } catch (HiveException e) {
      throw new IOException(e);
    }
  }

  /**
   * @return the metadataTag
   */
  public int getMetadataTag() {
    return metadataTag;
  }

  /**
   * @param metadataTag
   *          the metadataTag to set
   */
  public void setMetadataTag(int metadataTag) {
    this.metadataTag = metadataTag;
  }

  /**
   * @return the obj
   */
  public RowContainer getObj() {
    return obj;
  }

  /**
   * @param obj
   *          the obj to set
   */
  public void setObj(RowContainer obj) {
    this.obj = obj;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

}
