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

package org.apache.hadoop.hive.ql.exec.vector.keyseries;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hive.common.util.HashCodeUtil;

import com.google.common.base.Preconditions;

/**
 * Implementation of base serialization interface.
 *
 */
public abstract class VectorKeySeriesSerializedImpl<T extends SerializeWrite>
    extends VectorKeySeriesSingleImpl implements VectorKeySeriesSerialized {

  protected T serializeWrite;

  protected int bufferOffset;

  // The serialized (non-NULL) series keys.  These 3 members represent the value.
  public int serializedStart;
  public int serializedLength;
  public byte[] serializedBytes;

  protected final Output output;

  protected final int[] serializedKeyLengths;

  public VectorKeySeriesSerializedImpl(T serializeWrite) {
    super();
    this.serializeWrite = serializeWrite;
    output = new Output();
    serializedKeyLengths = new int[VectorizedRowBatch.DEFAULT_SIZE];
  }

  public boolean validate() {
    super.validate();

    int nullCount = 0;
    for (int i = 0; i < seriesCount; i++) {
      if (seriesIsAllNull[i]) {
        nullCount++;
      }
    }
    Preconditions.checkState(nullCount + nonNullKeyCount == seriesCount);

    int lengthSum = 0;
    int keyLength;
    for (int i = 0; i < nonNullKeyCount; i++) {
      keyLength = serializedKeyLengths[i];
      Preconditions.checkState(keyLength > 0);
      lengthSum += keyLength;
      Preconditions.checkState(lengthSum <= output.getLength());
    }
    return true;
  }

  @Override
  public byte[] getSerializedBytes() {
    return serializedBytes;
  }

  @Override
  public int getSerializedStart() {
    return serializedStart;
  }

  @Override
  public int getSerializedLength() {
    return serializedLength;
  }

  /**
   * Batch compute the hash codes for all the serialized keys.
   *
   * NOTE: MAJOR MAJOR ASSUMPTION:
   *     We assume that HashCodeUtil.murmurHash produces the same result
   *     as MurmurHash.hash with seed = 0 (the method used by ReduceSinkOperator for
   *     UNIFORM distribution).
   */
  protected void computeSerializedHashCodes() {
    int offset = 0;
    int keyLength;
    byte[] bytes = output.getData();
    for (int i = 0; i < nonNullKeyCount; i++) {
      keyLength = serializedKeyLengths[i];
      hashCodes[i] = HashCodeUtil.murmurHash(bytes, offset, keyLength);
      offset += keyLength;
    }
  }

  @Override
  public void positionToFirst() {

    // Reset this before calling positionToFirst.
    bufferOffset = 0;

    super.positionToFirst();

    // This is constant for whole series.
    serializedBytes = output.getData();
  }

  @Override
  public void setNextNonNullKey(int nonNullKeyPosition) {
    serializedStart = bufferOffset;
    serializedLength = serializedKeyLengths[nonNullKeyPosition];
    Preconditions.checkState(serializedStart + serializedLength <= output.getData().length);
    bufferOffset += serializedLength;
  }
}