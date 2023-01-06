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

public class KllHistogramEstimator {

  private final KllFloatsSketch kll;

  KllHistogramEstimator() {
    this.kll = new KllFloatsSketch();
  }

  KllHistogramEstimator(int k) {
    this.kll = new KllFloatsSketch(k);
  }

  KllHistogramEstimator(KllFloatsSketch kll) {
    this.kll = kll;
  }

  public byte[] serialize() {
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      KllUtils.serializeKll(bos, kll);
      final byte[] result = bos.toByteArray();
      bos.close();
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void addToEstimator(long v) {
    kll.update(v);
  }

  public void addToEstimator(double d) {
    kll.update((float) d);
  }

  public void addToEstimator(HiveDecimal decimal) {
    kll.update(decimal.floatValue());
  }

  public void mergeEstimators(KllHistogramEstimator o) {
    kll.merge(o.kll);
  }

  public int lengthFor(JavaDataModel model) {
    return KllUtils.lengthFor(model, kll);
  }

  public boolean canMerge(KllHistogramEstimator o) {
    return o != null && this.kll.getK() == o.kll.getK();
  }

  public int getK() {
    return kll.getK();
  }

  public KllFloatsSketch getSketch() {
    return kll;
  }
}
