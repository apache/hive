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
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinRowContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Writable;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;

/**
 * HashTableLoader for Tez constructs the hashtable from records read from
 * a broadcast edge.
 */
public class HashTableLoader implements org.apache.hadoop.hive.ql.exec.HashTableLoader {

  private static final Log LOG = LogFactory.getLog(MapJoinOperator.class.getName());

  public HashTableLoader() {
  }

  @Override
  public void load(ExecMapperContext context,
      Configuration hconf,
      MapJoinDesc desc,
      byte posBigTable,
      MapJoinTableContainer[] mapJoinTables,
      MapJoinTableContainerSerDe[] mapJoinTableSerdes) throws HiveException {

    TezContext tezContext = (TezContext) MapredContext.get();
    Map<Integer, String> parentToInput = desc.getParentToInput();
    int hashTableThreshold = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHASHTABLETHRESHOLD);
    float hashTableLoadFactor = HiveConf.getFloatVar(hconf,
        HiveConf.ConfVars.HIVEHASHTABLELOADFACTOR);

    for (int pos = 0; pos < mapJoinTables.length; pos++) {
      if (pos == posBigTable) {
        continue;
      }

      LogicalInput input = tezContext.getInput(parentToInput.get(pos));

      try {
        KeyValueReader kvReader = (KeyValueReader) input.getReader();

        MapJoinTableContainer tableContainer = new HashMapWrapper(hashTableThreshold,
            hashTableLoadFactor);

        // simply read all the kv pairs into the hashtable.
        while (kvReader.next()) {
          MapJoinKey key = new MapJoinKey();
          key.read(mapJoinTableSerdes[pos].getKeyContext(), (Writable)kvReader.getCurrentKey());

          MapJoinRowContainer values = tableContainer.get(key);
          if(values == null){
        	  values = new MapJoinRowContainer();
        	  tableContainer.put(key, values);
          }
          values.read(mapJoinTableSerdes[pos].getValueContext(), (Writable)kvReader.getCurrentValue());
        }

        mapJoinTables[pos] = tableContainer;
      } catch (IOException e) {
        throw new HiveException(e);
      } catch (SerDeException e) {
        throw new HiveException(e);
      } catch (Exception e) {
        throw new HiveException(e);
      }
    }
  }
}
