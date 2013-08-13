package org.apache.hadoop.hive.ql.udf;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

@Description(name = "unbase64",
    value = "_FUNC_(str) - Convert the argument from a base 64 string to binary")
public class UDFUnbase64 extends UDF {
  private final transient BytesWritable result = new BytesWritable();

  public BytesWritable evaluate(Text value){
    if (value == null) {
      return null;
    }
    byte[] bytes = new byte[value.getLength()];
    System.arraycopy(value.getBytes(), 0, bytes, 0, value.getLength());
    byte[] decoded = Base64.decodeBase64(bytes);
    result.set(decoded, 0, decoded.length);
    return result;
  }
}