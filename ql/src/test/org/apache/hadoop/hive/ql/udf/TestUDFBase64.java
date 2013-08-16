package org.apache.hadoop.hive.ql.udf;

import junit.framework.TestCase;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class TestUDFBase64 extends TestCase {
  public void testBase64Conversion(){
    byte[] bytes = "string".getBytes();
    // Let's make sure we only read the relevant part of the writable in case of reuse
    byte[] longBytes = "longer string".getBytes();
    BytesWritable writable = new BytesWritable(longBytes);
    writable.set(bytes, 0, bytes.length);
    UDFBase64 udf = new UDFBase64();
    Text text = udf.evaluate(writable);
    String base64String = text.toString();
    assertEquals("c3RyaW5n", base64String);
  }
}
