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

package org.apache.hadoop.hive.ql.exec;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.hive.ql.exec.MapJoinOperator.MapJoinObjectCtx;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Map Join Object used for both key and value
 */
public class MapJoinObject implements Externalizable {

  transient protected int     metadataTag;
  transient protected int     objectTypeTag;
  transient protected Object  obj;
  
  public MapJoinObject() {
  }

  /**
   * @param metadataTag
   * @param objectTypeTag
   * @param obj
   */
  public MapJoinObject(int metadataTag, int objectTypeTag, Object obj) {
    this.metadataTag = metadataTag;
    this.objectTypeTag = objectTypeTag;
    this.obj = obj;
  }
  
  public boolean equals(Object o) {
    if (o instanceof MapJoinObject) {
      MapJoinObject mObj = (MapJoinObject)o;
      if ((mObj.getMetadataTag() == metadataTag) && (mObj.getObjectTypeTag() == objectTypeTag)) {
        if ((obj == null) && (mObj.getObj() == null))
          return true;
        if ((obj != null) && (mObj.getObj() != null) && (mObj.getObj().equals(obj)))
          return true;
      }
    }

    return false;
  }
  
  public int hashCode() {
    return (obj == null) ? 0 : obj.hashCode();
  }
  
  @Override
  public void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
    try {
      metadataTag   = in.readInt();
      objectTypeTag = in.readInt();

      // get the tableDesc from the map stored in the mapjoin operator
      MapJoinObjectCtx ctx = MapJoinOperator.getMapMetadata().get(new Integer(metadataTag));
      Writable val = null;
    
      assert ((objectTypeTag == 1) || (objectTypeTag == 2));
      if (objectTypeTag == 1) {
        val = new BytesWritable();
        val.readFields(in);      
        obj = (ArrayList<Object>)ctx.getDeserializer().deserialize(val);
      }
      else if (objectTypeTag == 2) {
        int sz = in.readInt();

        Vector<ArrayList<Object>> res = new Vector<ArrayList<Object>>();
        for (int pos = 0; pos < sz; pos++) {
          ArrayList<Object> memObj = new ArrayList<Object>();
          val = new Text();
          val.readFields(in);
          StructObjectInspector objIns = (StructObjectInspector)ctx.getDeserObjInspector();
          LazyStruct lazyObj = (LazyStruct)(((LazyObject)ctx.getDeserializer().deserialize(val)).getObject());
          List<? extends StructField> listFields = objIns.getAllStructFieldRefs();
          int k = 0;
          for (StructField fld : listFields) {
            memObj.add(objIns.getStructFieldData(lazyObj, fld));
          }
          
          res.add(memObj);
        }
        obj = res;
      }
    } catch (Exception e) {
      throw new IOException(e.getMessage());
    }
  }
  
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    try {
      
      out.writeInt(metadataTag);
      out.writeInt(objectTypeTag);

      // get the tableDesc from the map stored in the mapjoin operator
      MapJoinObjectCtx ctx = MapJoinOperator.getMapMetadata().get(new Integer(metadataTag));

      // Different processing for key and value
      if (objectTypeTag == 1) {
        Writable val = ctx.getSerializer().serialize(obj, ctx.getSerObjInspector());
        val.write(out);
      }
      else if (objectTypeTag == 2) {
        Vector<Object> v = (Vector<Object>)obj;
        out.writeInt(v.size());

        for (int pos = 0; pos < v.size(); pos++) {
          Writable val = ctx.getSerializer().serialize(v.get(pos), ctx.getSerObjInspector());
          val.write(out);
        }
      }
    }
    catch (Exception e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * @return the metadataTag
   */
  public int getMetadataTag() {
    return metadataTag;
  }

  /**
   * @param metadataTag the metadataTag to set
   */
  public void setMetadataTag(int metadataTag) {
    this.metadataTag = metadataTag;
  }

  /**
   * @return the objectTypeTag
   */
  public int getObjectTypeTag() {
    return objectTypeTag;
  }

  /**
   * @param objectTypeTag the objectTypeTag to set
   */
  public void setObjectTypeTag(int objectTypeTag) {
    this.objectTypeTag = objectTypeTag;
  }

  /**
   * @return the obj
   */
  public Object getObj() {
    return obj;
  }

  /**
   * @param obj the obj to set
   */
  public void setObj(Object obj) {
    this.obj = obj;
  }

}
