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
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.util.ConcurrentModificationException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastTableContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Writable;

@SuppressWarnings("deprecation")
public class MapJoinTableContainerSerDe {

  private final MapJoinObjectSerDeContext keyContext;
  private final MapJoinObjectSerDeContext valueContext;
  public MapJoinTableContainerSerDe(MapJoinObjectSerDeContext keyContext,
      MapJoinObjectSerDeContext valueContext) {
    this.keyContext = keyContext;
    this.valueContext = valueContext;
  }

  public MapJoinObjectSerDeContext getKeyContext() {
    return keyContext;
  }
  public MapJoinObjectSerDeContext getValueContext() {
    return valueContext;
  }

  @SuppressWarnings({"unchecked"})
  /**
   * Loads the table container. Only used on MR path.
   * @param in Input stream.
   * @return Loaded table.
   */
  public MapJoinPersistableTableContainer load(ObjectInputStream in)
      throws HiveException {
    AbstractSerDe keySerDe = keyContext.getSerDe();
    AbstractSerDe valueSerDe = valueContext.getSerDe();
    MapJoinPersistableTableContainer tableContainer;
    try {
      String name = in.readUTF();
      Map<String, String> metaData = (Map<String, String>) in.readObject();
      tableContainer = create(name, metaData);
    } catch (IOException e) {
      throw new HiveException("IO error while trying to create table container", e);
    } catch (ClassNotFoundException e) {
      throw new HiveException("Class Initialization error while trying to create table container", e);
    }
    try {
      Writable keyContainer = keySerDe.getSerializedClass().newInstance();
      Writable valueContainer = valueSerDe.getSerializedClass().newInstance();
      int numKeys = in.readInt();
      for (int keyIndex = 0; keyIndex < numKeys; keyIndex++) {
        MapJoinKeyObject key = new MapJoinKeyObject();
        key.read(keyContext, in, keyContainer);
        MapJoinEagerRowContainer values = new MapJoinEagerRowContainer();
        values.read(valueContext, in, valueContainer);
        tableContainer.put(key, values);
      }
      return tableContainer;
    } catch (IOException e) {
      throw new HiveException("IO error while trying to create table container", e);
    } catch(Exception e) {
      throw new HiveException("Error while trying to create table container", e);
    }
  }

  private void loadNormal(MapJoinPersistableTableContainer container,
      ObjectInputStream in, Writable keyContainer, Writable valueContainer) throws Exception {
    int numKeys = in.readInt();
    for (int keyIndex = 0; keyIndex < numKeys; keyIndex++) {
      MapJoinKeyObject key = new MapJoinKeyObject();
      key.read(keyContext, in, keyContainer);
      if (container.get(key) == null) {
        container.put(key, new MapJoinEagerRowContainer());
      }
      MapJoinEagerRowContainer values = (MapJoinEagerRowContainer) container.get(key);
      values.read(valueContext, in, valueContainer);
      container.put(key, values);
    }
  }

  private void loadOptimized(MapJoinBytesTableContainer container, ObjectInputStream in,
      Writable key, Writable value) throws Exception {
    int numKeys = in.readInt();
    for (int keyIndex = 0; keyIndex < numKeys; keyIndex++) {
      key.readFields(in);
      long numRows = in.readLong();
      for (long rowIndex = 0L; rowIndex < numRows; rowIndex++) {
        value.readFields(in);
        container.putRow(key, value);
      }
    }
  }

  public void persist(ObjectOutputStream out, MapJoinPersistableTableContainer tableContainer)
      throws HiveException {
    int numKeys = tableContainer.size();
    try {
      out.writeUTF(tableContainer.getClass().getName());
      out.writeObject(tableContainer.getMetaData());
      out.writeInt(numKeys);
      for(Map.Entry<MapJoinKey, MapJoinRowContainer> entry : tableContainer.entrySet()) {
        entry.getKey().write(keyContext, out);
        entry.getValue().write(valueContext, out);
      }
    } catch (SerDeException e) {
      String msg = "SerDe error while attempting to persist table container";
      throw new HiveException(msg, e);
    } catch(IOException e) {
      String msg = "IO error while attempting to persist table container";
      throw new HiveException(msg, e);
    }
    if(numKeys != tableContainer.size()) {
      throw new ConcurrentModificationException("TableContainer was modified while persisting: " + tableContainer);
    }
  }

  public static void persistDummyTable(ObjectOutputStream out) throws IOException {
    MapJoinPersistableTableContainer tableContainer = new HashMapWrapper();
    out.writeUTF(tableContainer.getClass().getName());
    out.writeObject(tableContainer.getMetaData());
    out.writeInt(tableContainer.size());
  }

  private MapJoinPersistableTableContainer create(
      String name, Map<String, String> metaData) throws HiveException {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends MapJoinPersistableTableContainer> clazz =
          (Class<? extends MapJoinPersistableTableContainer>) JavaUtils.loadClass(name);
      Constructor<? extends MapJoinPersistableTableContainer> constructor =
          clazz.getDeclaredConstructor(Map.class);
      return constructor.newInstance(metaData);
    } catch (Exception e) {
      String msg = "Error while attempting to create table container" +
          " of type: " + name + ", with metaData: " + metaData;
      throw new HiveException(msg, e);
    }
  }
}
