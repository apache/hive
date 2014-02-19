/**
 * 
 */
package org.apache.hive.common.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A helper class for fast serialization of decimal128 in the BigDecimal byte[] representation
 *
 */
public class Decimal128FastBuffer {

  /**
   * Preallocated byte[] for each Decimal128 size (0-4)
   */
  private final byte[][] sumBytes;

  /**
   * Preallocated ByteBuffer wrappers around each sumBytes
   */
  private final ByteBuffer[] sumBuffer;

  public Decimal128FastBuffer() {
      sumBytes = new byte[5][];
      sumBuffer = new ByteBuffer[5];
      sumBytes[0] = new byte[1];
      sumBuffer[0] = ByteBuffer.wrap(sumBytes[0]);
      sumBytes[1] = new byte[5];
      sumBuffer[1] = ByteBuffer.wrap(sumBytes[1]);
      sumBuffer[1].order(ByteOrder.BIG_ENDIAN);
      sumBytes[2] = new byte[9];
      sumBuffer[2] = ByteBuffer.wrap(sumBytes[2]);
      sumBuffer[2].order(ByteOrder.BIG_ENDIAN);
      sumBytes[3] = new byte[13];
      sumBuffer[3] = ByteBuffer.wrap(sumBytes[3]);
      sumBuffer[3].order(ByteOrder.BIG_ENDIAN);
      sumBytes[4] = new byte[17];
      sumBuffer[4] = ByteBuffer.wrap(sumBytes[4]);
      sumBuffer[4].order(ByteOrder.BIG_ENDIAN);
  }

  public ByteBuffer getByteBuffer(int index) {
    return sumBuffer[index];
  }

  public byte[] getBytes(int index) {
    return sumBytes[index];
  }
}
