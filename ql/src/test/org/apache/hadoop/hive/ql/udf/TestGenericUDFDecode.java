package org.apache.hadoop.hive.ql.udf;

import java.io.UnsupportedEncodingException;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class TestGenericUDFDecode extends TestCase {
  public void testDecode() throws UnsupportedEncodingException, HiveException {
    String[] charsetNames = {"US-ASCII", "ISO-8859-1", "UTF-8", "UTF-16BE", "UTF-16LE", "UTF-16"};
    for (String charsetName : charsetNames){
      verifyDecode("A sample string", charsetName);
    }
  }

  public void verifyDecode(String string, String charsetName) throws UnsupportedEncodingException, HiveException{
    GenericUDFDecode udf = new GenericUDFDecode();
    byte[] bytes = string.getBytes(charsetName);

    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    ObjectInspector charsetOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] initArguments = {valueOI, charsetOI};
    udf.initialize(initArguments);

    DeferredObject valueObj = new DeferredJavaObject(bytes);
    DeferredObject charsetObj = new DeferredJavaObject(charsetName);
    DeferredObject[] arguments = {valueObj, charsetObj};
    String output = (String) udf.evaluate(arguments);

    assertEquals("Decoding failed for CharSet: " + charsetName, string, output);
  }
}

