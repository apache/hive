package org.apache.hadoop.hive.common.frequencies.freqitems;

import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.ArrayOfUtf16StringsSerDe;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;

import java.io.*;

public class FIUtils {

  private FIUtils() {
    throw new AssertionError("Suppress default constructor for non instantiation");
  }

  /**
   * Freq Items is serialized according to what provided by data-sketches library
   * @param out output stream to write to
   * @param itemsSketch Frequent Items sketch that needs to be serialized
   * @throws IOException if an error occurs during serialization
   */
  public static void serializeFI(OutputStream out, ItemsSketch<String> itemsSketch) throws IOException {
    byte[] b = itemsSketch.toByteArray(new ArrayOfUtf16StringsSerDe());
    out.write(b);
  }

  /**
   * This function deserializes the serialized Freq Items sketch from a stream.
   * @param in input stream to be deserialized
   * @return Freq Items sketch
   * @throws IOException if errors occur while reading the stream
   */
  public static ItemsSketch<String> deserializeFI(InputStream in) throws IOException {
    final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    final byte[] data = new byte[4];
    int nRead;

    while ((nRead = in.read(data, 0, data.length)) != -1) {
      buffer.write(data, 0, nRead);
    }

    buffer.flush();
    return ItemsSketch.getInstance(Memory.wrap(buffer.toByteArray()), new ArrayOfUtf16StringsSerDe());
  }


  /**
   * This function deserializes the serialized Freq Items sketch from a byte array.
   * @param buf to deserialize
   * @param start start index for deserialization
   * @param len start+len is deserialized
   * @return KLL sketch
   */
  public static ItemsSketch<String> deserializeFI(byte[] buf, int start, int len) {
    InputStream is = new ByteArrayInputStream(buf, start, len);
    try {
      ItemsSketch<String> result = deserializeFI(is);
      is.close();
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This function deserializes the serialized Freq Items sketch from a byte array.
   * @param buf to deserialize
   * @return KLL sketch
   */
  public static ItemsSketch deserializeFI(final byte[] buf) {
    return deserializeFI(buf, 0, buf.length);
  }

}
