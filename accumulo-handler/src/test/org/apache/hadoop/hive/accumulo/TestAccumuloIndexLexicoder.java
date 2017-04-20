package org.apache.hadoop.hive.accumulo;

import org.apache.accumulo.core.client.lexicoder.BigIntegerLexicoder;
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestAccumuloIndexLexicoder {

  @Test
  public void testBooleanString() {
    byte[] value = Boolean.TRUE.toString().getBytes(UTF_8);
    assertArrayEquals(AccumuloIndexLexicoder.encodeValue(value, serdeConstants.BOOLEAN_TYPE_NAME,
        true), value);
  }

  @Test
  public void testBooleanBinary() {
    byte[] value = new byte[] { 1 };
    assertArrayEquals(AccumuloIndexLexicoder.encodeValue(value, serdeConstants.BOOLEAN_TYPE_NAME,
        false), Boolean.TRUE.toString().getBytes(UTF_8));
  }

  @Test
  public void testIntString() {
    byte[] value = "10".getBytes(UTF_8);
    byte[] encoded = new IntegerLexicoder().encode(10);

    byte[] lex = AccumuloIndexLexicoder.encodeValue(value, serdeConstants.INT_TYPE_NAME, true);
    assertArrayEquals(lex, encoded);

    lex = AccumuloIndexLexicoder.encodeValue(value, serdeConstants.SMALLINT_TYPE_NAME, true);
    assertArrayEquals(lex, encoded);

    lex = AccumuloIndexLexicoder.encodeValue(value, serdeConstants.TINYINT_TYPE_NAME, true);
    assertArrayEquals(lex, encoded);
  }

  @Test
  public void testIntBinary() {
    byte[] value = ByteBuffer.allocate(4).putInt(10).array();
    byte[] encoded = new IntegerLexicoder().encode(10);

    byte[] lex = AccumuloIndexLexicoder.encodeValue(value, serdeConstants.INT_TYPE_NAME, false);
    assertArrayEquals(lex, encoded);

    value = ByteBuffer.allocate(2).putShort((short) 10).array();
    lex = AccumuloIndexLexicoder.encodeValue(value, serdeConstants.SMALLINT_TYPE_NAME, false);
    assertArrayEquals(lex, encoded);

    value = ByteBuffer.allocate(1).put((byte)10).array();
    lex = AccumuloIndexLexicoder.encodeValue(value, serdeConstants.TINYINT_TYPE_NAME, false);
    assertArrayEquals(lex, encoded);
  }

  @Test
  public void testFloatBinary() {
    byte[] value = ByteBuffer.allocate(4).putFloat(10.55f).array();
    byte[] encoded = new DoubleLexicoder().encode((double)10.55f);
    String val = new String(encoded);

    byte[] lex = AccumuloIndexLexicoder.encodeValue(value, serdeConstants.FLOAT_TYPE_NAME, false);
    assertArrayEquals(lex, encoded);

    value = ByteBuffer.allocate(8).putDouble(10.55).array();
    encoded = new DoubleLexicoder().encode(10.55);
    lex = AccumuloIndexLexicoder.encodeValue(value, serdeConstants.DOUBLE_TYPE_NAME, false);
    assertArrayEquals(lex, encoded);
  }

  @Test
  public void testFloatString() {
    byte[] value = "10.55".getBytes(UTF_8);
    byte[] encoded = new DoubleLexicoder().encode(10.55);

    byte[] lex = AccumuloIndexLexicoder.encodeValue(value, serdeConstants.FLOAT_TYPE_NAME, true);
    assertArrayEquals(lex, encoded);

    lex = AccumuloIndexLexicoder.encodeValue(value, serdeConstants.DOUBLE_TYPE_NAME, true);
    assertArrayEquals(lex, encoded);
  }

  @Test
  public void testBigIntBinary() {
    byte[] value = ByteBuffer.allocate(8).putLong(1232322323).array();
    byte[] encoded = new LongLexicoder().encode(1232322323L);

    byte[] lex = AccumuloIndexLexicoder.encodeValue(value, serdeConstants.BIGINT_TYPE_NAME, false);
    assertArrayEquals(lex, encoded);

    value = new BigInteger( "1232322323", 10 ).toByteArray();
    encoded = new BigIntegerLexicoder().encode(new BigInteger("1232322323", 10 ));
    lex = AccumuloIndexLexicoder.encodeValue(value, serdeConstants.DECIMAL_TYPE_NAME, false);
    assertArrayEquals(lex, encoded);
  }

  @Test
  public void testDecimalString() {
    String strVal = "12323232233434";
    byte[] value = strVal.getBytes(UTF_8);
    byte[] encoded = new BigIntegerLexicoder().encode(new BigInteger(strVal, 10));

    byte[] lex = AccumuloIndexLexicoder.encodeValue(value, serdeConstants.DECIMAL_TYPE_NAME, true);
    assertArrayEquals(lex, encoded);


    lex = AccumuloIndexLexicoder.encodeValue(value, "DECIMAL (10,3)", true);
    assertArrayEquals(lex, encoded);
  }

  @Test
  public void testDecimalBinary() {
    BigInteger value = new BigInteger("12323232233434", 10);
    byte[] encoded = new BigIntegerLexicoder().encode(value);

    byte[] lex = AccumuloIndexLexicoder.encodeValue(value.toByteArray(), serdeConstants.DECIMAL_TYPE_NAME, false);
    assertArrayEquals(lex, encoded);
  }

  @Test
  public void testDateString() {
    String date = "2016-02-22";
    byte[] value = date.getBytes(UTF_8);
    assertArrayEquals(AccumuloIndexLexicoder.encodeValue(value, serdeConstants.DATE_TYPE_NAME,
                                                        true), value);
  }

  @Test
  public void testDateTimeString() {
    String timestamp = "2016-02-22 12:12:06.000000005";
    byte[] value = timestamp.getBytes(UTF_8);
    assertArrayEquals(AccumuloIndexLexicoder.encodeValue(value, serdeConstants.TIMESTAMP_TYPE_NAME,
                                                        true), value);
  }

  @Test
  public void testString() {
    String strVal = "The quick brown fox";
    byte[] value = strVal.getBytes(UTF_8);
    assertArrayEquals(AccumuloIndexLexicoder.encodeValue(value, serdeConstants.STRING_TYPE_NAME,
                                                        true), value);
    assertArrayEquals(AccumuloIndexLexicoder.encodeValue(value, "varChar(20)",
                                                        true), value);
    assertArrayEquals(AccumuloIndexLexicoder.encodeValue(value, "CHAR (20)",
                                                        true), value);
  }
}
