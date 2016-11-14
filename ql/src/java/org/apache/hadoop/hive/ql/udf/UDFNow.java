package org.apache.hadoop.hive.ql.udf;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 *   Returns the current date and time as a value in 'YYYY-MM-DD HH:MM:SS' format.
 * It mimics now() in mysql.
 */
@Description(name = "now",
value = "_FUNC_() - Returns the current date",
extended = "date is a string in the format of 'yyyy-MM-dd HH:mm:ss' or "
+ "'yyyy-MM-dd'.\n"
+ "Example:\n "
+ "  > SELECT * FROM src WHERE time_field =  _FUNC_() LIMIT 1;\n" + "  30")
public class UDFNow extends UDF {
  private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  Text result = new Text();

  public Text evaluate()  {
    Date date = new Date();
    result.set(formatter.format(date));
    return result;
  }
}