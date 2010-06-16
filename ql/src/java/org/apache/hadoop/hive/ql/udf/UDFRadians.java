package org.apache.hadoop.hive.ql.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;


@Description(
      name = "radians",
      value = "_FUNC_(x) - Converts degrees to radians",
      extended = "Example:\n" +
          "  > SELECT _FUNC_(90) FROM src LIMIT 1;\n" +
          "  1.5707963267949mo\n"
      )
public class UDFRadians extends UDF {

  @SuppressWarnings("unused")
  private static Log LOG = LogFactory.getLog(UDFRadians.class.getName());
  DoubleWritable result = new DoubleWritable();
  
  public UDFRadians() {
  }

  public DoubleWritable evaluate(DoubleWritable i)  {
    if (i == null) {
      return null;
    } else {
      result.set(Math.toRadians(i.get()));
      return result;
    }
  }

}
