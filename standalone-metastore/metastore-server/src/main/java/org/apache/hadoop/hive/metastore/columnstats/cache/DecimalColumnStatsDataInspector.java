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
package org.apache.hadoop.hive.metastore.columnstats.cache;

import java.nio.ByteBuffer;

import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimatorFactory;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;

@SuppressWarnings("serial")
public class DecimalColumnStatsDataInspector extends DecimalColumnStatsData {

  private NumDistinctValueEstimator ndvEstimator;

  public DecimalColumnStatsDataInspector() {
    super();
  }

  public DecimalColumnStatsDataInspector(long numNulls, long numDVs) {
    super(numNulls, numDVs);
  }

  public DecimalColumnStatsDataInspector(DecimalColumnStatsDataInspector other) {
    super(other);
    if (other.ndvEstimator != null) {
      super.setBitVectors(ndvEstimator.serialize());
    }
  }

  public DecimalColumnStatsDataInspector(DecimalColumnStatsData other) {
    super(other);
  }

  @Override
  public DecimalColumnStatsDataInspector deepCopy() {
    return new DecimalColumnStatsDataInspector(this);
  }

  @Override
  public byte[] getBitVectors() {
    if (ndvEstimator != null) {
      updateBitVectors();
    }
    return super.getBitVectors();
  }

  @Override
  public ByteBuffer bufferForBitVectors() {
    if (ndvEstimator != null) {
      updateBitVectors();
    }
    return super.bufferForBitVectors();
  }

  @Override
  public void setBitVectors(byte[] bitVectors) {
    super.setBitVectors(bitVectors);
    this.ndvEstimator = null;
  }

  @Override
  public void setBitVectors(ByteBuffer bitVectors) {
    super.setBitVectors(bitVectors);
    this.ndvEstimator = null;
  }

  @Override
  public void unsetBitVectors() {
    super.unsetBitVectors();
    this.ndvEstimator = null;
  }

  @Override
  public boolean isSetBitVectors() {
    if (ndvEstimator != null) {
      updateBitVectors();
    }
    return super.isSetBitVectors();
  }

  @Override
  public void setBitVectorsIsSet(boolean value) {
    if (ndvEstimator != null) {
      updateBitVectors();
    }
    super.setBitVectorsIsSet(value);
  }

  public NumDistinctValueEstimator getNdvEstimator() {
    if (isSetBitVectors() && getBitVectors().length != 0) {
      updateNdvEstimator();
    }
    return ndvEstimator;
  }

  public void setNdvEstimator(NumDistinctValueEstimator ndvEstimator) {
    super.unsetBitVectors();
    this.ndvEstimator = ndvEstimator;
  }

  private void updateBitVectors() {
    super.setBitVectors(ndvEstimator.serialize());
    this.ndvEstimator = null;
  }

  private void updateNdvEstimator() {
    this.ndvEstimator = NumDistinctValueEstimatorFactory
        .getNumDistinctValueEstimator(super.getBitVectors());
    super.unsetBitVectors();
  }

}
