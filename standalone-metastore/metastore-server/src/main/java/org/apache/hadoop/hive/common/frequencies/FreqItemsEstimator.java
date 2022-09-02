package org.apache.hadoop.hive.common.frequencies;

import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.hadoop.hive.common.frequencies.freqitems.FIUtils;

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

  public boolean canMerge(FreqItemsEstimator o) {
    return o != null && this.freqSketch.getMaximumMapCapacity() == o.freqSketch.getMaximumMapCapacity();
  }

  public ItemsSketch<String> getSketch() {
    return freqSketch;
  }

}
