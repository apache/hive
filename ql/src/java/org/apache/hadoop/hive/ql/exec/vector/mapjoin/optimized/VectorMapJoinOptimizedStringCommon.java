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

import org.apache.hadoop.hive.ql.exec.vector.mapjoin.optimized.VectorMapJoinOptimizedHashTable.SerializedBytes;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;

/*
 * An single byte array value hash map based on the BytesBytesMultiHashMap.
 */
public class VectorMapJoinOptimizedStringCommon {

  private BinarySortableSerializeWrite keyBinarySortableSerializeWrite;

  private transient Output output;

  private transient SerializedBytes serializedBytes;
  private transient TableDesc tableDesc;

  public SerializedBytes serialize(byte[] keyBytes, int keyStart, int keyLength) throws IOException {

    keyBinarySortableSerializeWrite.reset();
    keyBinarySortableSerializeWrite.writeString(keyBytes, keyStart, keyLength);

    serializedBytes.bytes = output.getData();
    serializedBytes.offset = 0;
    serializedBytes.length = output.getLength();

    return serializedBytes;
  }

  public VectorMapJoinOptimizedStringCommon(boolean isOuterJoin, TableDesc tableDesc) {
    this.tableDesc = tableDesc;
    keyBinarySortableSerializeWrite = BinarySortableSerializeWrite.with(tableDesc.getProperties(), 1);
    output = new Output();
    keyBinarySortableSerializeWrite.set(output);
    serializedBytes = new SerializedBytes();
  }

  public TableDesc getTableDesc() {
    return tableDesc;
  }
}
