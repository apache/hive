package org.apache.hadoop.hive.ql.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

@Description(
    name = "UDFDegrees",
    value = "_FUNC_(x) - Converts radians to degrees",
    extended = "Example:\n" +
        "  > SELECT _FUNC_(30) FROM src LIMIT 1;\n" +
        "  -1\n"
    )
public class UDFDegrees extends UDF{

  @SuppressWarnings("unused")
  private static Log LOG = LogFactory.getLog(UDFDegrees.class.getName());
  DoubleWritable result = new DoubleWritable();

  public UDFDegrees() {
  }

  public DoubleWritable evaluate(DoubleWritable i)  {
    if (i == null) {
      return null;
    } else {
      result.set(Math.toDegrees(i.get()));
      return result;
    }
  }

}
