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
package org.apache.hive.benchmark.vectorization.mapjoin;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.tez.ObjectCache;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.CountCollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.CountVectorCollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig.MapJoinTestImplementation;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestData;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig.CreateMapJoinResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription.MapJoinPlanVariation;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription.SmallTableGenerationParameters;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.VectorMapJoinVariation;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public abstract class AbstractMapJoin {
  protected VectorMapJoinVariation vectorMapJoinVariation;
  protected MapJoinTestImplementation mapJoinImplementation;
  protected MapJoinTestDescription testDesc;
  protected MapJoinTestData testData;

  protected MapJoinOperator operator;

  protected boolean isVectorOutput;

  protected Object[][] bigTableRows;

  protected VectorizedRowBatch[] bigTableBatches;

  @Benchmark
  @Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.MILLISECONDS)
  public void bench() throws Exception {
    if (!isVectorOutput) {
      executeBenchmarkImplementationRow(mapJoinImplementation, testDesc, testData, operator, bigTableRows);
    } else {
      executeBenchmarkImplementationVector(mapJoinImplementation, testDesc, testData, operator, bigTableBatches);
    }
  }

  protected void setupMapJoin(HiveConf hiveConf, long seed, int rowCount,
      VectorMapJoinVariation vectorMapJoinVariation, MapJoinTestImplementation mapJoinImplementation,
      String[] bigTableColumnNames, TypeInfo[] bigTableTypeInfos,
      int[] bigTableKeyColumnNums,
      String[] smallTableValueColumnNames, TypeInfo[] smallTableValueTypeInfos,
      int[] bigTableRetainColumnNums,
      int[] smallTableRetainKeyColumnNums, int[] smallTableRetainValueColumnNums,
      SmallTableGenerationParameters smallTableGenerationParameters) throws Exception {

    this.vectorMapJoinVariation = vectorMapJoinVariation;
    this.mapJoinImplementation = mapJoinImplementation;
    testDesc = new MapJoinTestDescription(
        hiveConf, vectorMapJoinVariation,
        bigTableTypeInfos,
        bigTableKeyColumnNums,
        smallTableValueTypeInfos,
        smallTableRetainKeyColumnNums,
        smallTableGenerationParameters,
        MapJoinPlanVariation.DYNAMIC_PARTITION_HASH_JOIN);

    // Prepare data.  Good for ANY implementation variation.
    testData = new MapJoinTestData(rowCount, testDesc, seed);

    ObjectRegistryImpl objectRegistry = new ObjectRegistryImpl();
    ObjectCache.setupObjectRegistry(objectRegistry);

    operator = setupBenchmarkImplementation(
        mapJoinImplementation, testDesc, testData);

    isVectorOutput = isVectorOutput(mapJoinImplementation);

    /*
     * We don't measure data generation execution cost -- generate the big table into memory first.
     */
    if (!isVectorOutput) {

      bigTableRows = testData.getBigTableBatchSource().getRandomRows();

    } else {

      ArrayList<VectorizedRowBatch> bigTableBatchList = new ArrayList<VectorizedRowBatch>();
      VectorRandomBatchSource batchSource = testData.getBigTableBatchSource();
      batchSource.resetBatchIteration();
      while (true) {
        VectorizedRowBatch batch = testData.createBigTableBatch(testDesc);
        if (!batchSource.fillNextBatch(batch)) {
          break;
        }
        bigTableBatchList.add(batch);
      }
      bigTableBatches = bigTableBatchList.toArray(new VectorizedRowBatch[0]);
    }
  }

  private static boolean isVectorOutput(MapJoinTestImplementation mapJoinImplementation) {
    return
        (mapJoinImplementation != MapJoinTestImplementation.ROW_MODE_HASH_MAP &&
         mapJoinImplementation != MapJoinTestImplementation.ROW_MODE_OPTIMIZED);
  }

  protected static MapJoinOperator setupBenchmarkImplementation(
	      MapJoinTestImplementation mapJoinImplementation, MapJoinTestDescription testDesc,
	      MapJoinTestData testData)
	          throws Exception {

    MapJoinDesc mapJoinDesc = MapJoinTestConfig.createMapJoinDesc(testDesc);

    final boolean isVectorOutput = isVectorOutput(mapJoinImplementation);

    // This collector is just a row counter.
    Operator<? extends OperatorDesc> testCollectorOperator =
        (!isVectorOutput ? new CountCollectorTestOperator() :
            new CountVectorCollectorTestOperator());

    CreateMapJoinResult createMapJoinResult =
        MapJoinTestConfig.createMapJoinImplementation(
            mapJoinImplementation, testDesc, testData, mapJoinDesc,
            /* shareMapJoinTableContainer */ null);
    MapJoinOperator operator = createMapJoinResult.mapJoinOperator;
    MapJoinTableContainer mapJoinTableContainer = createMapJoinResult.mapJoinTableContainer;

    // Invoke initializeOp methods.
    operator.initialize(testDesc.hiveConf, testDesc.inputObjectInspectors);

    // Fixup the mapJoinTables.
    operator.setTestMapJoinTableContainer(1, mapJoinTableContainer, null);

    return operator;
  }

  private static void executeBenchmarkImplementationRow(
      MapJoinTestImplementation mapJoinImplementation, MapJoinTestDescription testDesc,
      MapJoinTestData testData, MapJoinOperator operator, Object[][] bigTableRows)
          throws Exception {

    final int size = bigTableRows.length;
    for (int i = 0; i < size; i++) {
      operator.process(bigTableRows[i], 0);
    }
    operator.closeOp(false);
  }

  private static void executeBenchmarkImplementationVector(
      MapJoinTestImplementation mapJoinImplementation, MapJoinTestDescription testDesc,
      MapJoinTestData testData, MapJoinOperator operator, VectorizedRowBatch[] bigTableBatches)
          throws Exception {

    final int size = bigTableBatches.length;
    for (int i = 0; i < size; i++) {
      operator.process(bigTableBatches[i], 0);
    }
    operator.closeOp(false);
  }
}
