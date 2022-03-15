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
package org.apache.hadoop.hive.common.histogram;

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.hadoop.hive.common.histogram.kll.KllUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.util.JavaDataModel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class KllHistogramEstimator implements HistogramEstimator {

  private final KllFloatsSketch kll;

  public KllHistogramEstimator(int k) {
    this.kll = new KllFloatsSketch(k);
  }

  public KllHistogramEstimator(KllFloatsSketch kll) {
    this.kll = kll;
  }

  @Override public void reset() {
    // TODO: AS - do we need reset?
  }

  @Override public byte[] serialize() {
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      KllUtils.serializeKLL(bos, kll);
      final byte[] result = bos.toByteArray();
      bos.close();
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  @Override public HistogramEstimator deserialize(byte[] buf) {
    return null;
  }

  @Override public void addToEstimator(long v) {
    kll.update(v);
  }

  @Override public void addToEstimator(double d) {
    kll.update((float) d);
  }

  @Override public void addToEstimator(String s) {
    throw new UnsupportedOperationException("String is not supported for KLL yet");
  }

  @Override public void addToEstimator(byte[] value, int offset, int length) {
    throw new UnsupportedOperationException("Binary is not supported for KLL yet");
  }

  @Override public void addToEstimator(HiveDecimal decimal) {
    kll.update(decimal.floatValue());
  }

  @Override public void mergeEstimators(HistogramEstimator o) {
    kll.merge(((KllHistogramEstimator) o).kll);
  }

  @Override public long computeHistogram() {
    // TODO: AS - plug binned histogram class here, and change return type too
    return 0;
  }

  @Override public int lengthFor(JavaDataModel model) {
    return KllUtils.lengthFor(model, kll);
  }

  @Override public boolean canMerge(HistogramEstimator o) {
    return o instanceof KllHistogramEstimator && this.kll.getK() == ((KllHistogramEstimator) o).kll.getK();
  }

  @Override public KllFloatsSketch getSketch() {
    return kll;
  }
}
