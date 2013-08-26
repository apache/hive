package org.apache.hadoop.hive.ql.udf;

import junit.framework.TestCase;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class TestUDFHex extends TestCase {
  public void testHexConversion(){
    byte[] bytes = "string".getBytes();
    // Let's make sure we only read the relevant part of the writable in case of reuse
    byte[] longBytes = "longer string".getBytes();
    BytesWritable writable = new BytesWritable(longBytes);
    writable.set(bytes, 0, bytes.length);
    UDFHex udf = new UDFHex();
    Text text = udf.evaluate(writable);
    String hexString = text.toString();
    assertEquals("737472696E67", hexString);
  }
}
