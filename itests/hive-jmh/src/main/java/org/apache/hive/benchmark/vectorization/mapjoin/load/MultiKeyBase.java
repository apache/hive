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
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VerifyFastRow;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.RandomTypeUtil;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MultiKeyBase extends AbstractHTLoadBench {

  public void doSetup(VectorMapJoinDesc.VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinTestConfig.MapJoinTestImplementation mapJoinImplementation, int rows) throws Exception {
    long seed = 2543;
    int rowCount = rows;
    HiveConf hiveConf = new HiveConf();
    int[] bigTableKeyColumnNums = new int[] { 0, 1, 2};
    String[] bigTableColumnNames = new String[] { "b1", "b2", "b3" };
    TypeInfo[] bigTableTypeInfos = new TypeInfo[] {
        TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.stringTypeInfo
    };
    int[] smallTableRetainKeyColumnNums = new int[] {};
    TypeInfo[] smallTableValueTypeInfos = new TypeInfo[] { TypeInfoFactory.stringTypeInfo };
    MapJoinTestDescription.SmallTableGenerationParameters smallTableGenerationParameters =
        new MapJoinTestDescription.SmallTableGenerationParameters();
    smallTableGenerationParameters
        .setValueOption(MapJoinTestDescription.SmallTableGenerationParameters.ValueOption.ONLY_ONE);
    setupMapJoinHT(hiveConf, seed, rowCount, vectorMapJoinVariation, mapJoinImplementation, bigTableColumnNames,
        bigTableTypeInfos, bigTableKeyColumnNums, smallTableValueTypeInfos, smallTableRetainKeyColumnNums,
        smallTableGenerationParameters);
    this.customKeyValueReader = generateByteKVPairs(rowCount, seed);
  }

  private static CustomKeyValueReader generateByteKVPairs(int rows, long seed) throws IOException {
    LOG.info("Data GEN for: " + rows);
    Random random = new Random(seed);
    BytesWritable[] keys = new BytesWritable[rows];
    BytesWritable[] values = new BytesWritable[rows];
    for (int i = 0; i < rows; i++) {
      keys[i] = new BytesWritable();
      values[i] = new BytesWritable();
    }
    BinarySortableSerializeWrite serializeWrite = new BinarySortableSerializeWrite(1);
    ByteStream.Output output = new ByteStream.Output();
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < rows; i++) {
      output.reset();
      serializeWrite.set(output);
      VerifyFastRow.serializeWrite(serializeWrite, TypeInfoFactory.timestampTypeInfo, new TimestampWritableV2(RandomTypeUtil.getRandTimestamp(random)));
      keys[i].set(output.getData(), 0, output.getLength());

      output.reset();
      Text v = new Text(RandomTypeUtil.getRandString(random, null, 20*2));
      serializeWrite.set(output);
      VerifyFastRow.serializeWrite(serializeWrite, TypeInfoFactory.stringTypeInfo, v);
      values[i].set(output.getData(), 0, output.getLength());
    }
    LOG.info("Data GEN done after {} sec",
        TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime));
    return new CustomKeyValueReader(keys, values);
  }
}
