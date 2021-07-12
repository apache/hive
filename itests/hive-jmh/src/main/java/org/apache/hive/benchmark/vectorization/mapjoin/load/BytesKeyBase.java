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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.RandomTypeUtil;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class BytesKeyBase extends AbstractHTLoadBench {

  public void doSetup(VectorMapJoinDesc.VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinTestConfig.MapJoinTestImplementation mapJoinImplementation, int rows) throws Exception {
    long seed = 2543;
    int rowCount = rows;
    HiveConf hiveConf = new HiveConf();
    int[] bigTableKeyColumnNums = new int[] { 0 };
    String[] bigTableColumnNames = new String[] { "b1" };
    TypeInfo[] bigTableTypeInfos = new TypeInfo[] { TypeInfoFactory.stringTypeInfo };
    int[] smallTableRetainKeyColumnNums = new int[] {};
    TypeInfo[] smallTableValueTypeInfos =
        new TypeInfo[] { TypeInfoFactory.dateTypeInfo, TypeInfoFactory.timestampTypeInfo };
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
    long startTime = System.currentTimeMillis();

    BinarySortableSerializeWrite serializeWrite = new BinarySortableSerializeWrite(1);
    ByteStream.Output output = new ByteStream.Output();
    int str_len = 20;

    for (int i = 0; i < rows; i++) {
      output.reset();
      Text k = new Text(RandomTypeUtil.getRandString(random, null, str_len));
      serializeWrite.set(output);
      VerifyFastRow.serializeWrite(serializeWrite, TypeInfoFactory.stringTypeInfo, k);
      keys[i].set(output.getData(), 0, output.getLength());

      output.reset();
      Text v = new Text(RandomTypeUtil.getRandString(random, null, str_len*2));
      serializeWrite.set(output);
      VerifyFastRow.serializeWrite(serializeWrite, TypeInfoFactory.stringTypeInfo, v);
      values[i].set(output.getData(), 0, output.getLength());
    }
    LOG.info("Data GEN done after {} sec",
        TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime));
    return new CustomKeyValueReader(keys, values);
  }

  public static String deserializeBinary(BytesWritable currentKey) throws HiveException {
    PrimitiveTypeInfo[] primitiveTypeInfos = { TypeInfoFactory.stringTypeInfo };
    boolean[] columnSortOrderIsDesc = new boolean[1];
    Arrays.fill(columnSortOrderIsDesc, false);

    byte[] columnNullMarker = new byte[1];
    Arrays.fill(columnNullMarker, BinarySortableSerDe.ZERO);
    byte[] columnNotNullMarker = new byte[1];
    Arrays.fill(columnNotNullMarker, BinarySortableSerDe.ONE);

    BinarySortableDeserializeRead keyBinarySortableDeserializeRead =
        new BinarySortableDeserializeRead(
            primitiveTypeInfos,
            /* useExternalBuffer */ false,
            columnSortOrderIsDesc,
            columnNullMarker,
            columnNotNullMarker);
    byte[] keyBytes = currentKey.getBytes();
    int keyLength = currentKey.getLength();
    keyBinarySortableDeserializeRead.set(keyBytes, 0, keyLength);
    try {
      if (!keyBinarySortableDeserializeRead.readNextField()) {
        return null;
      }
    } catch (Exception e) {
      throw new HiveException("DeserializeRead details: " +
          keyBinarySortableDeserializeRead.getDetailedReadPositionString(), e);
    }


    byte[] stringBytes = Arrays.copyOfRange(
        keyBinarySortableDeserializeRead.currentBytes,
        keyBinarySortableDeserializeRead.currentBytesStart,
        keyBinarySortableDeserializeRead.currentBytesStart + keyBinarySortableDeserializeRead.currentBytesLength);
    Text text = new Text(stringBytes);
    String string = text.toString();
    return string;
  }
}
