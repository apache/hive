package org.apache.hadoop.hive.common.frequencies.freqitems;

import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.ArrayOfUtf16StringsSerDe;
import org.apache.datasketches.frequencies.ItemsSketch;

import java.io.IOException;
import java.io.OutputStream;

public class FIUtils {

  private FIUtils() {
    throw new AssertionError("Suppress default constructor for non instantiation");
  }

  /**
   * KLL is serialized according to what provided by data-sketches library
   * @param out output stream to write to
   * @param itemsSketch Frequent Items sketch that needs to be serialized
   * @throws IOException if an error occurs during serialization
   */
  public static void serializeFI(OutputStream out, ItemsSketch<String> itemsSketch) throws IOException {
    byte[] b = itemsSketch.toByteArray(new ArrayOfUtf16StringsSerDe());
    out.write(b);
  }

}
