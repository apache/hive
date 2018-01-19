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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.CountCollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.CountVectorCollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerateStream;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerateUtil;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestData;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig.MapJoinTestImplementation;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription.SmallTableGenerationParameters;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.VectorMapJoinVariation;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;
import java.util.concurrent.TimeUnit;

// UNDONE: For now, just run once cold.
@BenchmarkMode(Mode.SingleShotTime)
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
  // @Warmup(iterations = 0, time = 1, timeUnit = TimeUnit.MILLISECONDS)
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
    String[] bigTableColumnNames, TypeInfo[] bigTableTypeInfos, int[] bigTableKeyColumnNums,
    String[] smallTableValueColumnNames, TypeInfo[] smallTableValueTypeInfos,
    int[] bigTableRetainColumnNums,
    int[] smallTableRetainKeyColumnNums, int[] smallTableRetainValueColumnNums,
    SmallTableGenerationParameters smallTableGenerationParameters) throws Exception {

    this.vectorMapJoinVariation = vectorMapJoinVariation;
    this.mapJoinImplementation = mapJoinImplementation;
    testDesc = new MapJoinTestDescription(
        hiveConf, vectorMapJoinVariation,
        bigTableColumnNames, bigTableTypeInfos,
        bigTableKeyColumnNums,
        smallTableValueColumnNames, smallTableValueTypeInfos,
        bigTableRetainColumnNums,
        smallTableRetainKeyColumnNums, smallTableRetainValueColumnNums,
        smallTableGenerationParameters);

    // Prepare data.  Good for ANY implementation variation.
    testData = new MapJoinTestData(rowCount, testDesc, seed, seed * 10);
  
    operator = setupBenchmarkImplementation(
        mapJoinImplementation, testDesc, testData);

    isVectorOutput = isVectorOutput(mapJoinImplementation);

    /*
     * We don't measure data generation execution cost -- generate the big table into memory first.
     */
    if (!isVectorOutput) {

      bigTableRows = VectorBatchGenerateUtil.generateRowObjectArray(
          testDesc.bigTableKeyTypeInfos, testData.getBigTableBatchStream(),
          testData.getBigTableBatch(), testDesc.outputObjectInspectors);

    } else {

      bigTableBatches = VectorBatchGenerateUtil.generateBatchArray(
          testData.getBigTableBatchStream(), testData.getBigTableBatch());

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

    // UNDONE: Parameterize for implementation variation?
    MapJoinDesc mapJoinDesc = MapJoinTestConfig.createMapJoinDesc(testDesc);

    final boolean isVectorOutput = isVectorOutput(mapJoinImplementation);

    // This collector is just a row counter.
    Operator<? extends OperatorDesc> testCollectorOperator =
        (!isVectorOutput ? new CountCollectorTestOperator() :
            new CountVectorCollectorTestOperator());

    MapJoinOperator operator =
        MapJoinTestConfig.createMapJoinImplementation(
            mapJoinImplementation, testDesc, testCollectorOperator, testData, mapJoinDesc);
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
