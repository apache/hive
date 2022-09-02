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

import org.apache.hadoop.hive.common.frequencies.FreqItemsEstimator;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimatorFactory;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;

@SuppressWarnings("serial")
public class StringColumnStatsDataInspector extends StringColumnStatsData {

  private NumDistinctValueEstimator ndvEstimator;
  private FreqItemsEstimator freqItemsEstimator;

  public StringColumnStatsDataInspector() {
    super();
  }

  public StringColumnStatsDataInspector(long maxColLen, double avgColLen,
      long numNulls, long numDVs) {
    super(maxColLen, avgColLen, numNulls, numDVs);
  }

  public StringColumnStatsDataInspector(StringColumnStatsDataInspector other) {
    super(other);
    if (other.ndvEstimator != null) {
      super.setBitVectors(ndvEstimator.serialize());
    }
    if (other.freqItemsEstimator != null) {
      super.setFreqItems(freqItemsEstimator.serialize());
    }
  }

  public StringColumnStatsDataInspector(StringColumnStatsData other) {
    super(other);
  }

  @Override
  public StringColumnStatsDataInspector deepCopy() {
    return new StringColumnStatsDataInspector(this);
  }

  @Override
  public byte[] getBitVectors() {
    if (ndvEstimator != null) {
      updateBitVectors();
    }
    return super.getBitVectors();
  }

  @Override
  public byte[] getFreqItems() {
    if (freqItemsEstimator != null) {
      updateFreqItems();
    }
    return super.getFreqItems();
  }

  @Override
  public ByteBuffer bufferForBitVectors() {
    if (ndvEstimator != null) {
      updateBitVectors();
    }
    return super.bufferForBitVectors();
  }

  @Override
  public ByteBuffer bufferForFreqItems() {
    if (freqItemsEstimator != null) {
      updateFreqItems();
    }
    return super.bufferForFreqItems();
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
  public void setFreqItems(byte[] stats) {
    super.setFreqItems(stats);
    this.freqItemsEstimator = null;
  }

  @Override
  public void setFreqItems(ByteBuffer stats) {
    super.setFreqItems(stats);
    this.freqItemsEstimator = null;
  }


  @Override
  public void unsetBitVectors() {
    super.unsetBitVectors();
    this.ndvEstimator = null;
  }

  @Override
  public void unsetFreqItems() {
    super.unsetFreqItems();
    this.freqItemsEstimator = null;
  }

  @Override
  public boolean isSetBitVectors() {
    if (ndvEstimator != null) {
      updateBitVectors();
    }
    return super.isSetBitVectors();
  }

  @Override
  public boolean isSetFreqItems() {
    if (freqItemsEstimator != null) {
      updateFreqItems();
    }
    return super.isSetFreqItems();
  }

  @Override
  public void setBitVectorsIsSet(boolean value) {
    if (ndvEstimator != null) {
      updateBitVectors();
    }
    super.setBitVectorsIsSet(value);
  }

  @Override
  public void setFreqItemsIsSet(boolean value) {
    if (freqItemsEstimator != null) {
      updateFreqItems();
    }
    super.setFreqItemsIsSet(value);
  }

  public NumDistinctValueEstimator getNdvEstimator() {
    if (ndvEstimator == null && isSetBitVectors() && getBitVectors().length != 0) {
      updateNdvEstimator();
    }
    return ndvEstimator;
  }

  public FreqItemsEstimator getFreqItemsEstimator() {
    if (freqItemsEstimator == null && isSetFreqItems() && getFreqItems().length != 0) {
      updateFreqItemsEstimator();
    }
    return freqItemsEstimator;
  }

  public void setNdvEstimator(NumDistinctValueEstimator ndvEstimator) {
    super.unsetBitVectors();
    this.ndvEstimator = ndvEstimator;
  }

  public void setFreqItemsEstimator(FreqItemsEstimator freqItemsEstimator) {
    super.unsetFreqItems();
    this.freqItemsEstimator = freqItemsEstimator;
  }

  private void updateBitVectors() {
    super.setBitVectors(ndvEstimator.serialize());
    this.ndvEstimator = null;
  }

  private void updateFreqItems() {
    super.setFreqItems(freqItemsEstimator.serialize());
    this.freqItemsEstimator = null;
  }

  private void updateNdvEstimator() {
    this.ndvEstimator = NumDistinctValueEstimatorFactory
        .getNumDistinctValueEstimator(super.getBitVectors());
    super.unsetBitVectors();
  }

  private void updateFreqItemsEstimator() {
    this.freqItemsEstimator = KllHistogramEstimatorFactory
        .getKllHistogramEstimator(super.getFreqItems());
    super.unsetFreqItems();
  }

}
