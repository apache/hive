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
package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKeyObject;
import org.apache.hadoop.hive.ql.exec.persistence.LazyFlatRowContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;

/**
 * HashTableLoader for Tez constructs the hashtable from records read from
 * a broadcast edge.
 */
public class HashTableLoader implements org.apache.hadoop.hive.ql.exec.HashTableLoader {

  private static final Log LOG = LogFactory.getLog(HashTableLoader.class.getName());

  private ExecMapperContext context;
  private Configuration hconf;
  private MapJoinDesc desc;
  private MapJoinKey lastKey = null;

  @Override
  public void init(ExecMapperContext context, Configuration hconf, MapJoinOperator joinOp) {
    this.context = context;
    this.hconf = hconf;
    this.desc = joinOp.getConf();
  }

  @Override
  public void load(
      MapJoinTableContainer[] mapJoinTables,
      MapJoinTableContainerSerDe[] mapJoinTableSerdes) throws HiveException {

    TezContext tezContext = (TezContext) MapredContext.get();
    Map<Integer, String> parentToInput = desc.getParentToInput();
    int hashTableThreshold = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHASHTABLETHRESHOLD);
    float hashTableLoadFactor = HiveConf.getFloatVar(hconf,
        HiveConf.ConfVars.HIVEHASHTABLELOADFACTOR);
    boolean useLazyRows = HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVEMAPJOINLAZYHASHTABLE);

    TezCacheAccess tezCacheAccess = TezCacheAccess.createInstance(hconf);
    // We only check if we can use optimized keys here; that is ok because we don't
    // create optimized keys in MapJoin if hash map doesn't have optimized keys.
    if (!HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVEMAPJOINUSEOPTIMIZEDKEYS)) {
      lastKey = new MapJoinKeyObject();
    }
    Output output = new Output(); // Reusable output for serialization.
    for (int pos = 0; pos < mapJoinTables.length; pos++) {
      if (pos == desc.getPosBigTable()) {
        continue;
      }

      String inputName = parentToInput.get(pos);
      LogicalInput input = tezContext.getInput(inputName);

      try {
        KeyValueReader kvReader = (KeyValueReader) input.getReader();

        MapJoinTableContainer tableContainer = new HashMapWrapper(hashTableThreshold,
            hashTableLoadFactor);

        // simply read all the kv pairs into the hashtable.

        while (kvReader.next()) {
          // We pass key in as reference, to find out quickly if optimized keys can be used.
          // However, we do not reuse the object since we are putting them into the hashmap.
          lastKey = MapJoinKey.read(output, lastKey, mapJoinTableSerdes[pos].getKeyContext(),
              (Writable)kvReader.getCurrentKey(), false);

          LazyFlatRowContainer values = (LazyFlatRowContainer)tableContainer.get(lastKey);
          if (values == null) {
            values = new LazyFlatRowContainer();
            tableContainer.put(lastKey, values);
          }
          values.add(mapJoinTableSerdes[pos].getValueContext(),
              (BytesWritable)kvReader.getCurrentValue(), useLazyRows);
        }

        mapJoinTables[pos] = tableContainer;
      } catch (IOException e) {
        throw new HiveException(e);
      } catch (SerDeException e) {
        throw new HiveException(e);
      } catch (Exception e) {
        throw new HiveException(e);
      }
      // Register that the Input has been cached.
      LOG.info("Is this a bucket map join: " + desc.isBucketMapJoin());
      // cache is disabled for bucket map join because of the same reason
      // given in loadHashTable in MapJoinOperator.
      if (!desc.isBucketMapJoin()) {
        tezCacheAccess.registerCachedInput(inputName);
        LOG.info("Setting Input: " + inputName + " as cached");
      }
    }
    if (lastKey == null) {
      lastKey = new MapJoinKeyObject(); // No rows in tables, the key type doesn't matter.
    }
  }
}
