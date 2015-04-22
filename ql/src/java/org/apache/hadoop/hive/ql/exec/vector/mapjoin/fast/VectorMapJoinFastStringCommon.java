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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead.ReadStringResults;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;

/*
 * An single byte array value hash map optimized for vector map join.
 */
public class VectorMapJoinFastStringCommon {

  private boolean isOuterJoin;

  private BinarySortableDeserializeRead keyBinarySortableDeserializeRead;

  private ReadStringResults readStringResults;

  public void adaptPutRow(VectorMapJoinFastBytesHashTable hashTable,
          BytesWritable currentKey, BytesWritable currentValue) throws HiveException, IOException {

    byte[] keyBytes = currentKey.getBytes();
    int keyLength = currentKey.getLength();
    keyBinarySortableDeserializeRead.set(keyBytes, 0, keyLength);
    if (keyBinarySortableDeserializeRead.readCheckNull()) {
      if (isOuterJoin) {
        return;
      } else {
        // For inner join, we expect all NULL values to have been filtered out before now.
        throw new HiveException("Unexpected NULL in map join small table");
      }
    }
    keyBinarySortableDeserializeRead.readString(readStringResults);

    hashTable.add(readStringResults.bytes, readStringResults.start, readStringResults.length,
        currentValue);
  }

  public VectorMapJoinFastStringCommon(boolean isOuterJoin) {
    this.isOuterJoin = isOuterJoin;
    PrimitiveTypeInfo[] primitiveTypeInfos = { TypeInfoFactory.stringTypeInfo };
    keyBinarySortableDeserializeRead = new BinarySortableDeserializeRead(primitiveTypeInfos);
    readStringResults = keyBinarySortableDeserializeRead.createReadStringResults();
  }
}