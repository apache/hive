package org.apache.hadoop.hive.ql.udf;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

@Description(name = "base64",
    value = "_FUNC_(bin) - Convert the argument from binary to a base 64 string")
public class UDFBase64 extends UDF {
  private final transient Text result = new Text();

  public Text evaluate(BytesWritable b){
    if (b == null) {
      return null;
    }
    byte[] bytes = new byte[b.getLength()];
    System.arraycopy(b.getBytes(), 0, bytes, 0, b.getLength());
    result.set(Base64.encodeBase64(bytes));
    return result;
  }
}
