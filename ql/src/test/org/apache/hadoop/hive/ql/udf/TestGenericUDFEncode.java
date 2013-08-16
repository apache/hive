package org.apache.hadoop.hive.ql.udf;

import java.io.UnsupportedEncodingException;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;

public class TestGenericUDFEncode extends TestCase {
  public void testEncode() throws UnsupportedEncodingException, HiveException{
    String[] charsetNames = {"US-ASCII", "ISO-8859-1", "UTF-8", "UTF-16BE", "UTF-16LE", "UTF-16"};
    for (String charsetName : charsetNames){
      verifyEncode("A sample string", charsetName);
    }
  }

  public void verifyEncode(String string, String charsetName) throws UnsupportedEncodingException, HiveException{
    GenericUDFEncode udf = new GenericUDFEncode();
    byte[] expected = string.getBytes(charsetName);

    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector charsetOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] initArguments = {valueOI, charsetOI};
    udf.initialize(initArguments);

    DeferredObject valueObj = new DeferredJavaObject(string);
    DeferredObject charsetObj = new DeferredJavaObject(charsetName);
    DeferredObject[] arguments = {valueObj, charsetObj};
    BytesWritable outputWritable = (BytesWritable) udf.evaluate(arguments);

    byte[] output = outputWritable.getBytes();
    assertTrue("Encoding failed for CharSet: " + charsetName, expected.length == outputWritable.getLength());
    for (int i = 0; i < expected.length; i++){
      assertEquals("Encoding failed for CharSet: " + charsetName, expected[i], output[i]);
    }
  }
}
