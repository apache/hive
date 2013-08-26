package org.apache.hadoop.hive.ql.udf;

import junit.framework.TestCase;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class TestUDFUnbase64 extends TestCase {
  public void testUnbase64Conversion(){
    Text base64 = new Text();
    // Let's make sure we only read the relevant part of the writable in case of reuse
    base64.set("Garbage 64. Should be ignored.");
    base64.set("c3RyaW5n");

    BytesWritable expected = new BytesWritable("string".getBytes());

    UDFUnbase64 udf = new UDFUnbase64();
    BytesWritable output = udf.evaluate(base64);
    assertEquals(expected, output);
  }
}
