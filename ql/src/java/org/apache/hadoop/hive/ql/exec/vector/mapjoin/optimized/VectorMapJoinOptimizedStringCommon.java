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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.optimized;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.vector.mapjoin.optimized.VectorMapJoinOptimizedHashTable.SerializedBytes;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;

/*
 * An single byte array value hash map based on the BytesBytesMultiHashMap.
 *
 * Since BytesBytesMultiHashMap does not interpret the key as BinarySortable we optimize
 * this case and just reference the byte array key directly for the lookup instead of serializing
 * the byte array into BinarySortable. We rely on it just doing byte array equality comparisons.
 */
public class VectorMapJoinOptimizedStringCommon {

  // private boolean isOuterJoin;

  // private BinarySortableDeserializeRead keyBinarySortableDeserializeRead;

  // private ReadStringResults readStringResults;

  private BinarySortableSerializeWrite keyBinarySortableSerializeWrite;

  private transient Output output;

  private transient SerializedBytes serializedBytes;

  /*
  private BytesWritable bytesWritable;

  public void adaptPutRow(VectorMapJoinOptimizedHashTable hashTable,
      BytesWritable currentKey, BytesWritable currentValue)
      throws SerDeException, HiveException, IOException {

    byte[] keyBytes = currentKey.getBytes();
    int keyLength = currentKey.getLength();
    keyBinarySortableDeserializeRead.set(keyBytes, 0, keyLength);
    if (keyBinarySortableDeserializeRead.readCheckNull()) {
      if (isOuterJoin) {
        return;
      } else {
        // For inner join, we expect all NULL values to have been filtered out before now.
        throw new HiveException("Unexpected NULL");
      }
    }
    keyBinarySortableDeserializeRead.readString(readStringResults);

    bytesWritable.set(readStringResults.bytes, readStringResults.start, readStringResults.length);

    hashTable.putRowInternal(bytesWritable, currentValue);
  }
  */

  public SerializedBytes serialize(byte[] keyBytes, int keyStart, int keyLength) throws IOException {

    keyBinarySortableSerializeWrite.reset();
    keyBinarySortableSerializeWrite.writeString(keyBytes, keyStart, keyLength);

    serializedBytes.bytes = output.getData();
    serializedBytes.offset = 0;
    serializedBytes.length = output.getLength();

    return serializedBytes;

  }

  public VectorMapJoinOptimizedStringCommon(boolean isOuterJoin) {
    // this.isOuterJoin = isOuterJoin;
    // PrimitiveTypeInfo[] primitiveTypeInfos = { TypeInfoFactory.stringTypeInfo };
    // keyBinarySortableDeserializeRead = new BinarySortableDeserializeRead(primitiveTypeInfos);
    // readStringResults = keyBinarySortableDeserializeRead.createReadStringResults();
    // bytesWritable = new BytesWritable();
    keyBinarySortableSerializeWrite = new BinarySortableSerializeWrite(1);
    output = new Output();
    keyBinarySortableSerializeWrite.set(output);
    serializedBytes = new SerializedBytes();
  }
}