package org.apache.hadoop.hive.common.frequencies;

import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.hadoop.hive.common.frequencies.freqitems.FIUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class FreqItemsEstimator {

  private final ItemsSketch<String> freqSketch;

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

}
