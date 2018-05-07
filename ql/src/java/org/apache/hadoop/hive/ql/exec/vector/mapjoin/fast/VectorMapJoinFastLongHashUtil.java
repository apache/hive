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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import java.io.IOException;

import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;

public class VectorMapJoinFastLongHashUtil {

  public static long deserializeLongKey(BinarySortableDeserializeRead keyBinarySortableDeserializeRead,
      HashTableKeyType hashTableKeyType) throws IOException {
    long key = 0;
    switch (hashTableKeyType) {
    case BOOLEAN:
      key = (keyBinarySortableDeserializeRead.currentBoolean ? 1 : 0);
      break;
    case BYTE:
      key = (long) keyBinarySortableDeserializeRead.currentByte;
      break;
    case SHORT:
      key = (long) keyBinarySortableDeserializeRead.currentShort;
      break;
    case INT:
      key = (long) keyBinarySortableDeserializeRead.currentInt;
      break;
    case LONG:
      key = keyBinarySortableDeserializeRead.currentLong;
      break;
    default:
      throw new RuntimeException("Unexpected hash table key type " + hashTableKeyType.name());
    }
    return key;
  }
}