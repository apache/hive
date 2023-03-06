package org.apache.hadoop.hive.common.type;

import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;

class TestFastHiveDecimalImpl {
  @Test
  void testRead() throws IOException {
    BigDecimal bigDecimal = new BigDecimal(new BigInteger("7000000000000000000"), 9);
    HiveDecimal decimal = HiveDecimal.create("7000000000.000000000", false);
    byte[] bytes;
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      long[] scratchLongs = new long[6];
      FastHiveDecimalImpl.fastSerializationUtilsWrite(
              outputStream, decimal.fastSignum(), decimal.fast0, decimal.fast1, decimal.fast2,
              decimal.fastIntegerDigitCount, decimal.fastScale, scratchLongs);
      bytes = outputStream.toByteArray();
    }

    HiveDecimalWritable decimalWritable = new HiveDecimalWritable();
    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes)) {
      byte[] scratchBytes = new byte[20];
      decimalWritable.serializationUtilsRead(inputStream, 9, scratchBytes);
    }

    assertEquals("7000000000.000000000", decimalWritable.toFormatString(9));
  }
}