package org.apache.hadoop.hive.ql.udf;

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;

public class TestUDFUnhex extends TestCase {
  public void testUnhexConversion(){
    Text hex = new Text();
    // Let's make sure we only read the relevant part of the writable in case of reuse
    hex.set("57686174207765726520796F7520686F70696E6720666F723F");
    hex.set("737472696E67");

    byte[] expected = "string".getBytes();

    UDFUnhex udf = new UDFUnhex();
    byte[] output = udf.evaluate(hex);
    assertEquals(expected.length,output.length);
    for (int i = 0; i < expected.length; i++){
      assertEquals(expected[i], output[i]);
    }
  }
}
