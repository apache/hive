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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.HashTableLoader;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig.CreateMapJoinResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig.MapJoinTestImplementation;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestData;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription.MapJoinPlanVariation;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription.SmallTableGenerationParameters;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastHashTableLoader;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.VectorMapJoinVariation;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public abstract class AbstractHTLoadBench {

  protected static final Logger LOG = LoggerFactory.getLogger(AbstractHTLoadBench.class.getName());

  protected VectorMapJoinVariation vectorMapJoinVariation;
  protected MapJoinTestImplementation mapJoinImplementation;
  protected CreateMapJoinResult createMapJoinResult;

  protected int rowCount;
  protected MapJoinTestData testData;
  protected TezContext mockTezContext;
  protected MapJoinTestDescription testDesc;
  protected CustomKeyValueReader customKeyValueReader;


  @Param({"0", "1", "2", "4", "8", "16", "32", "64"})
  static int LOAD_THREADS_NUM;

  @Param({"10000", "2000000"})
  static int ROWS_NUM;

  // MultiSet, HashSet, HashMap
  @Param({"INNER_BIG_ONLY", "LEFT_SEMI", "INNER"})
  static String JOIN_TYPE;

  @Benchmark
  public void hashTableLoadBench() throws Exception {
    /* Mocking TezContext */
    this.mockTezContext = mock(TezContext.class);
    ProcessorContext mockProcessorContest = mock(ProcessorContext.class);
    AbstractLogicalInput mockLogicalInput = mock(AbstractLogicalInput.class);
    TezCounters mockTezCounters = mock(TezCounters.class);
    TezCounter mockTezCounter = mock(TezCounter.class);
    InputContext mockInputContext = mock(InputContext.class);
    // Make sure the KEY estimation is correct to have properly sized HT
    when(mockTezCounter.getValue()).thenReturn((long)rowCount);
    when(mockTezContext.getInput(any())).thenReturn(mockLogicalInput);
    when(mockLogicalInput.getContext()).thenReturn(mockInputContext);
    when(mockInputContext.getCounters()).thenReturn(mockTezCounters);
    when(mockTezCounters.findCounter(anyString(), anyString())).thenReturn(mockTezCounter);
    when(mockTezContext.getTezProcessorContext()).thenReturn(mockProcessorContest);
    when(mockTezContext.getTezProcessorContext().getCounters()).thenReturn(mockTezCounters);
    when(mockTezContext.getTezProcessorContext().getCounters().findCounter(anyString(), anyString())).
        thenReturn(mockTezCounter);
    // Replace streaming Tez-Input with our custom Iterator
    when(mockTezContext.getInput(any())).thenReturn(mockLogicalInput);
    when(mockLogicalInput.getReader()).thenReturn(customKeyValueReader);
    /* Mocking Done*/
    HashTableLoader ht = LOAD_THREADS_NUM == 0 ?
        new LegacyVectorMapJoinFastHashTableLoader(mockTezContext, testDesc.hiveConf, createMapJoinResult.mapJoinOperator) :
        new VectorMapJoinFastHashTableLoader(mockTezContext, testDesc.hiveConf, createMapJoinResult.mapJoinOperator);
    ht.load(new MapJoinTableContainer[createMapJoinResult.mapJoinOperator.getConf().getTagLength()], null);
  }

  // Common Setup
  protected void setupMapJoinHT(HiveConf hiveConf, long seed, int rowCount,
      VectorMapJoinVariation vectorMapJoinVariation, MapJoinTestImplementation mapJoinImplementation,
      String[] bigTableColumnNames, TypeInfo[] bigTableTypeInfos, int[] bigTableKeyColumnNums,
      TypeInfo[] smallTableValueTypeInfos, int[] smallTableRetainKeyColumnNums,
      SmallTableGenerationParameters smallTableGenerationParameters) throws Exception {

    hiveConf.set(HiveConf.ConfVars.HIVE_MAPJOIN_PARALEL_HASHTABLE_THREADS.varname, LOAD_THREADS_NUM + "");
    LOG.info("Number of threads: " + hiveConf.get(HiveConf.ConfVars.HIVE_MAPJOIN_PARALEL_HASHTABLE_THREADS.varname));


    this.rowCount = rowCount;

    this.vectorMapJoinVariation = vectorMapJoinVariation;
    this.mapJoinImplementation = mapJoinImplementation;
    this.testDesc = new MapJoinTestDescription(hiveConf, vectorMapJoinVariation, bigTableColumnNames, bigTableTypeInfos,
        bigTableKeyColumnNums, smallTableValueTypeInfos, smallTableRetainKeyColumnNums, smallTableGenerationParameters,
        MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);
    this.testData = new MapJoinTestData(1, testDesc, seed);

    MapJoinDesc mapJoinDesc = MapJoinTestConfig.createMapJoinDesc(testDesc);
    this.createMapJoinResult = MapJoinTestConfig.createMapJoinImplementation(mapJoinImplementation, testDesc, testData,
        mapJoinDesc,/* shareMapJoinTableContainer */ null);
  }

  static class CustomKeyValueReader extends KeyValueReader {
    private BytesWritable[] keys;
    private BytesWritable[] values;
    private int idx = -1;

    public CustomKeyValueReader(BytesWritable[] k, BytesWritable[] v) {
      this.keys = k;
      this.values = v;
    }

    @Override
    public boolean next() throws IOException {
      if (++idx >= keys.length)
        return false;
      return true;
    }

    @Override
    public Object getCurrentKey() throws IOException {
      return keys[idx];
    }

    @Override
    public Object getCurrentValue() throws IOException {
      return values[idx];
    }

    void reset() {
      this.idx = -1;
    }
  }
}
