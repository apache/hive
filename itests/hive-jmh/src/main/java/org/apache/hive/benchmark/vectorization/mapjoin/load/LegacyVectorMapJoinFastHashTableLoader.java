/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.benchmark.vectorization.mapjoin.load;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapDaemonInfo;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.MemoryMonitorInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mapjoin.MapJoinMemoryExhaustionError;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastTableContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class LegacyVectorMapJoinFastHashTableLoader implements org.apache.hadoop.hive.ql.exec.HashTableLoader {
  private static final Logger LOG = LoggerFactory.getLogger(LegacyVectorMapJoinFastHashTableLoader.class);
  private Configuration hconf;
  protected MapJoinDesc desc;
  private TezContext tezContext;
  private String cacheKey;
  private TezCounter htLoadCounter;

  public LegacyVectorMapJoinFastHashTableLoader(TezContext context, Configuration hconf, MapJoinOperator joinOp) {
    this.tezContext = context;
    this.hconf = hconf;
    this.desc = (MapJoinDesc)joinOp.getConf();
    this.cacheKey = joinOp.getCacheKey();
    this.htLoadCounter = this.tezContext.getTezProcessorContext().getCounters().findCounter(HiveConf.getVar(hconf, HiveConf.ConfVars.HIVE_COUNTER_GROUP), hconf.get("__hive.context.name", ""));
  }

  @Override
  public void init(ExecMapperContext context, MapredContext mrContext,
      Configuration hconf, MapJoinOperator joinOp) {
    this.tezContext = (TezContext) mrContext;
    this.hconf = hconf;
    this.desc = joinOp.getConf();
    this.cacheKey = joinOp.getCacheKey();
    String counterGroup = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVE_COUNTER_GROUP);
    String vertexName = hconf.get(Operator.CONTEXT_NAME_KEY, "");
    String counterName = Utilities.getVertexCounterName(HashTableLoaderCounters.HASHTABLE_LOAD_TIME_MS.name(), vertexName);
    this.htLoadCounter = tezContext.getTezProcessorContext().getCounters().findCounter(counterGroup, counterName);
  }

  @Override
  public void load(MapJoinTableContainer[] mapJoinTables,
      MapJoinTableContainerSerDe[] mapJoinTableSerdes)
      throws HiveException {

    Map<Integer, String> parentToInput = desc.getParentToInput();
    Map<Integer, Long> parentKeyCounts = desc.getParentKeyCounts();

    MemoryMonitorInfo memoryMonitorInfo = desc.getMemoryMonitorInfo();
    boolean doMemCheck = false;
    long effectiveThreshold = 0;
    if (memoryMonitorInfo != null) {
      effectiveThreshold = memoryMonitorInfo.getEffectiveThreshold(desc.getMaxMemoryAvailable());

      // hash table loading happens in server side, LlapDecider could kick out some fragments to run outside of LLAP.
      // Flip the flag at runtime in case if we are running outside of LLAP
      if (!LlapDaemonInfo.INSTANCE.isLlap()) {
        memoryMonitorInfo.setLlap(false);
      }
      if (memoryMonitorInfo.doMemoryMonitoring()) {
        doMemCheck = true;
        LOG.info("Memory monitoring for hash table loader enabled. {}", memoryMonitorInfo);
      }
    }

    if (!doMemCheck) {
      LOG.info("Not doing hash table memory monitoring. {}", memoryMonitorInfo);
    }
    for (int pos = 0; pos < mapJoinTables.length; pos++) {
      if (pos == desc.getPosBigTable()) {
        continue;
      }

      long numEntries = 0;
      String inputName = parentToInput.get(pos);
      LogicalInput input = tezContext.getInput(inputName);

      try {
        input.start();
        tezContext.getTezProcessorContext().waitForAnyInputReady(
            Collections.<Input> singletonList(input));
      } catch (Exception e) {
        throw new HiveException(e);
      }

      try {
        KeyValueReader kvReader = (KeyValueReader) input.getReader();

        Long keyCountObj = parentKeyCounts.get(pos);
        long estKeyCount = (keyCountObj == null) ? -1 : keyCountObj;

        long inputRecords = -1;
        try {
          //TODO : Need to use class instead of string.
          // https://issues.apache.org/jira/browse/HIVE-23981
          inputRecords = ((AbstractLogicalInput) input).getContext().getCounters().
              findCounter("org.apache.tez.common.counters.TaskCounter",
                  "APPROXIMATE_INPUT_RECORDS").getValue();
        } catch (Exception e) {
          LOG.debug("Failed to get value for counter APPROXIMATE_INPUT_RECORDS", e);
        }
        long keyCount = Math.max(estKeyCount, inputRecords);

        VectorMapJoinFastTableContainer vectorMapJoinFastTableContainer =
            new VectorMapJoinFastTableContainer(desc, hconf, keyCount, 1);

        LOG.info("Loading hash table for input: {} cacheKey: {} tableContainer: {} smallTablePos: {} " +
                "estKeyCount : {} keyCount : {}", inputName, cacheKey,
            vectorMapJoinFastTableContainer.getClass().getSimpleName(), pos, estKeyCount, keyCount);

        vectorMapJoinFastTableContainer.setSerde(null, null); // No SerDes here.
        long startTime = System.currentTimeMillis();
        while (kvReader.next()) {
          vectorMapJoinFastTableContainer.putRow((BytesWritable)kvReader.getCurrentKey(),
              (BytesWritable)kvReader.getCurrentValue());
          numEntries++;
          if (doMemCheck && (numEntries % memoryMonitorInfo.getMemoryCheckInterval() == 0)) {
            final long estMemUsage = vectorMapJoinFastTableContainer.getEstimatedMemorySize();
            if (estMemUsage > effectiveThreshold) {
              String msg = "Hash table loading exceeded memory limits for input: " + inputName +
                  " numEntries: " + numEntries + " estimatedMemoryUsage: " + estMemUsage +
                  " effectiveThreshold: " + effectiveThreshold + " memoryMonitorInfo: " + memoryMonitorInfo;
              LOG.error(msg);
              throw new MapJoinMemoryExhaustionError(msg);
            } else {
              LOG.info(
                  "Checking hash table loader memory usage for input: {} numEntries: {} "
                      + "estimatedMemoryUsage: {} effectiveThreshold: {}",
                  inputName, numEntries, estMemUsage, effectiveThreshold);
            }
          }
        }
        long delta = System.currentTimeMillis() - startTime;
        htLoadCounter.increment(delta);

        vectorMapJoinFastTableContainer.seal();
        mapJoinTables[pos] = vectorMapJoinFastTableContainer;
        if (doMemCheck) {
          LOG.info("Finished loading hash table for input: {} cacheKey: {} numEntries: {} " +
                  "estimatedMemoryUsage: {} Load Time : {} ", inputName, cacheKey, numEntries,
              vectorMapJoinFastTableContainer.getEstimatedMemorySize(), delta);
        } else {
          LOG.info("Finished loading hash table for input: {} cacheKey: {} numEntries: {} Load Time : {} ",
              inputName, cacheKey, numEntries, delta);
        }
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
