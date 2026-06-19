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

import org.apache.hadoop.hive.common.histogram.KllHistogramEstimator;
import org.apache.hadoop.hive.common.histogram.KllHistogramEstimatorFactory;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimatorFactory;
import org.apache.hadoop.hive.metastore.api.TimestampColumnStatsData;

@SuppressWarnings("serial")
public class TimestampColumnStatsDataInspector extends TimestampColumnStatsData {

  private NumDistinctValueEstimator ndvEstimator;
  private KllHistogramEstimator histogramEstimator;

  public TimestampColumnStatsDataInspector() {
    super();
  }

  public TimestampColumnStatsDataInspector(long numNulls, long numDVs) {
    super(numNulls, numDVs);
  }

  public TimestampColumnStatsDataInspector(TimestampColumnStatsDataInspector other) {
    super(other);
    if (other.ndvEstimator != null) {
      super.setBitVectors(ndvEstimator.serialize());
    }
    if (other.histogramEstimator != null) {
      super.setHistogram(histogramEstimator.serialize());
    }
  }

  public TimestampColumnStatsDataInspector(TimestampColumnStatsData other) {
    super(other);
  }

  @Override
  public TimestampColumnStatsDataInspector deepCopy() {
    return new TimestampColumnStatsDataInspector(this);
  }

  @Override
  public byte[] getBitVectors() {
    if (ndvEstimator != null) {
      updateBitVectors();
    }
    return super.getBitVectors();
  }

  @Override
  public byte[] getHistogram() {
    if (histogramEstimator != null) {
      updateHistogram();
    }
    return super.getHistogram();
  }

  @Override
  public ByteBuffer bufferForBitVectors() {
    if (ndvEstimator != null) {
      updateBitVectors();
    }
    return super.bufferForBitVectors();
  }

  @Override
  public ByteBuffer bufferForHistogram() {
    if (histogramEstimator != null) {
      updateHistogram();
    }
    return super.bufferForHistogram();
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
  public void setHistogram(byte[] stats) {
    super.setHistogram(stats);
    this.histogramEstimator = null;
  }

  @Override
  public void setHistogram(ByteBuffer stats) {
    super.setHistogram(stats);
    this.histogramEstimator = null;
  }

  @Override
  public void unsetBitVectors() {
    super.unsetBitVectors();
    this.ndvEstimator = null;
  }

  @Override
  public void unsetHistogram() {
    super.unsetHistogram();
    this.histogramEstimator = null;
  }

  @Override
  public boolean isSetBitVectors() {
    if (ndvEstimator != null) {
      updateBitVectors();
    }
    return super.isSetBitVectors();
  }

  @Override
  public boolean isSetHistogram() {
    if (histogramEstimator != null) {
      updateHistogram();
    }
    return super.isSetHistogram();
  }

  @Override
  public void setBitVectorsIsSet(boolean value) {
    if (ndvEstimator != null) {
      updateBitVectors();
    }
    super.setBitVectorsIsSet(value);
  }

  @Override
  public void setHistogramIsSet(boolean value) {
    if (histogramEstimator != null) {
      updateHistogram();
    }
    super.setHistogramIsSet(value);
  }

  public NumDistinctValueEstimator getNdvEstimator() {
    if (ndvEstimator == null && isSetBitVectors() && getBitVectors().length != 0) {
      updateNdvEstimator();
    }
    return ndvEstimator;
  }

  public KllHistogramEstimator getHistogramEstimator() {
    if (histogramEstimator == null && isSetHistogram() && getHistogram().length != 0) {
      updateHistogramEstimator();
    }
    return histogramEstimator;
  }

  public void setNdvEstimator(NumDistinctValueEstimator ndvEstimator) {
    super.unsetBitVectors();
    this.ndvEstimator = ndvEstimator;
  }

  public void setHistogramEstimator(KllHistogramEstimator histogramEstimator) {
    super.unsetHistogram();
    this.histogramEstimator = histogramEstimator;
  }

  private void updateBitVectors() {
    super.setBitVectors(ndvEstimator.serialize());
    this.ndvEstimator = null;
  }

  private void updateHistogram() {
    super.setHistogram(histogramEstimator.serialize());
    this.histogramEstimator = null;
  }

  private void updateNdvEstimator() {
    this.ndvEstimator = NumDistinctValueEstimatorFactory
        .getNumDistinctValueEstimator(super.getBitVectors());
    super.unsetBitVectors();
  }

  private void updateHistogramEstimator() {
    this.histogramEstimator = KllHistogramEstimatorFactory
        .getKllHistogramEstimator(super.getHistogram());
    super.unsetHistogram();
  }
}
