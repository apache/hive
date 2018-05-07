/*
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.optimized.VectorMapJoinOptimizedHashTable.SerializedBytes;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;

/*
 * An single long value hash map based on the BytesBytesMultiHashMap.
 *
 * We serialize the long key into BinarySortable format into an output buffer accepted by
 * BytesBytesMultiHashMap.
 */
public class VectorMapJoinOptimizedLongCommon {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinOptimizedLongCommon.class.getName());

  private boolean isOuterJoin;

  private HashTableKeyType hashTableKeyType;

  // private BinarySortableDeserializeRead keyBinarySortableDeserializeRead;

  private BinarySortableSerializeWrite keyBinarySortableSerializeWrite;

  private transient Output output;

  private transient SerializedBytes serializedBytes;

  // protected boolean useMinMax;
  protected long min;
  protected long max;

  public boolean useMinMax() {
    return false;
  }

  public long min() {
    return min;
  }

  public long max() {
    return max;
  }

  public SerializedBytes serialize(long key) throws IOException {
    keyBinarySortableSerializeWrite.reset();

    switch (hashTableKeyType) {
    case BOOLEAN:
      keyBinarySortableSerializeWrite.writeBoolean(key == 1);
      break;
    case BYTE:
      keyBinarySortableSerializeWrite.writeByte((byte) key);
      break;
    case SHORT:
      keyBinarySortableSerializeWrite.writeShort((short) key);
      break;
    case INT:
      keyBinarySortableSerializeWrite.writeInt((int) key);
      break;
    case LONG:
      keyBinarySortableSerializeWrite.writeLong(key);
      break;
    default:
      throw new RuntimeException("Unexpected hash table key type " + hashTableKeyType.name());
    }

    // byte[] bytes = Arrays.copyOf(output.getData(), output.getLength());
    // LOG.debug("VectorMapJoinOptimizedLongCommon serialize key " + key + " hashTableKeyType " + hashTableKeyType.name() + " hex " + Hex.encodeHexString(bytes));

    serializedBytes.bytes = output.getData();
    serializedBytes.offset = 0;
    serializedBytes.length = output.getLength();

    return serializedBytes;
  }

  public VectorMapJoinOptimizedLongCommon(
        boolean minMaxEnabled, boolean isOuterJoin, HashTableKeyType hashTableKeyType) {
    this.isOuterJoin = isOuterJoin;
    // useMinMax = minMaxEnabled;
    min = Long.MAX_VALUE;
    max = Long.MIN_VALUE;
    this.hashTableKeyType = hashTableKeyType;
    keyBinarySortableSerializeWrite = new BinarySortableSerializeWrite(1);
    output = new Output();
    keyBinarySortableSerializeWrite.set(output);
    serializedBytes = new SerializedBytes();
  }
}
