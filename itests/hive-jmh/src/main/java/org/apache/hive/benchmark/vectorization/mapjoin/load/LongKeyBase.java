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
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class LongKeyBase extends AbstractHTLoadBench {

  public void doSetup(VectorMapJoinDesc.VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinTestConfig.MapJoinTestImplementation mapJoinImplementation, int rows) throws Exception {
    long seed = 2543;
    int rowCount = rows;
    HiveConf hiveConf = new HiveConf();
    int[] bigTableKeyColumnNums = new int[] { 0 };
    String[] bigTableColumnNames = new String[] { "number1" };
    TypeInfo[] bigTableTypeInfos = new TypeInfo[] { TypeInfoFactory.longTypeInfo };
    int[] smallTableRetainKeyColumnNums = new int[] {};
    TypeInfo[] smallTableValueTypeInfos =
        new TypeInfo[] { TypeInfoFactory.dateTypeInfo, TypeInfoFactory.stringTypeInfo };
    MapJoinTestDescription.SmallTableGenerationParameters smallTableGenerationParameters =
        new MapJoinTestDescription.SmallTableGenerationParameters();
    smallTableGenerationParameters
        .setValueOption(MapJoinTestDescription.SmallTableGenerationParameters.ValueOption.ONLY_ONE);
    setupMapJoinHT(hiveConf, seed, rowCount, vectorMapJoinVariation, mapJoinImplementation, bigTableColumnNames,
        bigTableTypeInfos, bigTableKeyColumnNums, smallTableValueTypeInfos, smallTableRetainKeyColumnNums,
        smallTableGenerationParameters);
    this.customKeyValueReader = generateLongKVPairs(rowCount, seed);
  }

  private static CustomKeyValueReader generateLongKVPairs(int rows, long seed) throws IOException {
    LOG.info("Data GEN for: " + rows);
    Random random = new Random(seed);
    BytesWritable[] keys = new BytesWritable[rows];
    BytesWritable[] values = new BytesWritable[rows];
    BinarySortableSerializeWrite bsw = new BinarySortableSerializeWrite(1);
    long startTime = System.currentTimeMillis();
    ByteStream.Output outp;
    BytesWritable key;
    BytesWritable value;
    for (int i = 0; i < rows; i++) {
      outp = new ByteStream.Output();
      bsw.set(outp);
      long k = random.nextInt(rows);
      bsw.writeLong(k);
      key = new BytesWritable(outp.getData(), outp.getLength());
      outp = new ByteStream.Output();
      bsw.reset();
      bsw.writeLong(random.nextInt(rows * 2));
      value = new BytesWritable(outp.getData(), outp.getLength());
      keys[i] = key;
      values[i] = value;
    }
    LOG.info("Data GEN done after {} sec",
        TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime));
    return new CustomKeyValueReader(keys, values);
  }
}
