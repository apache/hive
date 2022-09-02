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
package org.apache.hadoop.hive.common.frequencies;

import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.hadoop.hive.common.frequencies.freqitems.FIUtils;
import org.apache.hadoop.hive.ql.util.JavaDataModel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class FreqItemsEstimator {

  private final ItemsSketch<String> freqSketch;

  public FreqItemsEstimator() {
    this.freqSketch = new ItemsSketch<String>(200);
  }

  public FreqItemsEstimator(int maxSize) {
    this.freqSketch = new ItemsSketch<String>(maxSize);
  }

  public FreqItemsEstimator(ItemsSketch<String> freqSketch) {
    this.freqSketch = freqSketch;
  }

  public byte[] serialize() {
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
        FIUtils.serializeFI(bos, freqSketch);

      final byte[] result = bos.toByteArray();
      bos.close();
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void mergeEstimators(FreqItemsEstimator o) {
    freqSketch.merge(o.freqSketch);
  }

  public int lengthFor(JavaDataModel model) {
    return FIUtils.lengthFor(model, freqSketch);
  }

  public boolean canMerge(FreqItemsEstimator o) {
    return o != null && this.freqSketch.getMaximumMapCapacity() == o.freqSketch.getMaximumMapCapacity();
  }

  public ItemsSketch<String> getSketch() {
    return freqSketch;
  }

}
