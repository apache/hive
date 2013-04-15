package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

@UDFType(deterministic = false)
@Description(name = "unix_timestamp",
    value = "_FUNC_([date[, pattern]]) - Returns the UNIX timestamp",
    extended = "Converts the current or specified time to number of seconds "
        + "since 1970-01-01.")
public class GenericUDFUnixTimeStamp extends GenericUDFToUnixTimeStamp {

  @Override
  protected void initializeInput(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length > 0) {
      super.initializeInput(arguments);
    }
  }

  @Override
  protected String getName() {
    return "unix_timestamp";
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments.length == 0) {
      retValue.set(System.currentTimeMillis() / 1000);
      return retValue;
    }
    return super.evaluate(arguments);
  }
}
