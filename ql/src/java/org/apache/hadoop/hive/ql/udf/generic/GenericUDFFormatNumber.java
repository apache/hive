/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.io.Text;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.lang.Number;
import java.lang.NumberFormatException;

/**
 * Generic UDF for format_number function
 * <code>FORMAT_NUMBER(X, D)</code>.
 * This is supposed to function like MySQL's FORMAT,
 * http://dev.mysql.com/doc/refman/5.1/en/string-functions.html#
 * function_format
 *
 * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF
 */
@Description(name = "format_number",
    value = "_FUNC_(X, D) - Formats the number X to "
    + "a format like '#,###,###.##', rounded to D decimal places,"
    + " and returns the result as a string. If D is 0, the result"
    + " has no decimal point or fractional part."
    + " This is supposed to function like MySQL's FORMAT",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(12332.123456, 4) FROM src LIMIT 1;\n"
    + "  '12,332.1235'")
public class GenericUDFFormatNumber extends GenericUDF {
  private ObjectInspector[] argumentOIs;
  private final Text resultText = new Text();
  private final StringBuilder pattern = new StringBuilder("");
  private final DecimalFormat numberFormat = new DecimalFormat("");
  private int lastDValue = -1;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
          "The function FORMAT_NUMBER(X, D) needs two arguments.");
    }

    switch (arguments[0].getCategory()) {
      case PRIMITIVE:
        break;
      default:
        throw new UDFArgumentTypeException(0, "Argument 1"
          + " of function FORMAT_NUMBER must be \""
          + serdeConstants.TINYINT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.SMALLINT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.INT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.BIGINT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.DOUBLE_TYPE_NAME + "\""
          + " or \"" + serdeConstants.FLOAT_TYPE_NAME + "\", but \""
          + arguments[0].getTypeName() + "\" was found.");
    }

    switch (arguments[1].getCategory()) {
      case PRIMITIVE:
        break;
      default:
        throw new UDFArgumentTypeException(1, "Argument 2"
          + " of function FORMAT_NUMBER must be \""
          + serdeConstants.TINYINT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.SMALLINT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.INT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.BIGINT_TYPE_NAME + "\", but \""
          + arguments[1].getTypeName() + "\" was found.");
    }

    PrimitiveObjectInspector xObjectInspector = (PrimitiveObjectInspector)arguments[0];
    PrimitiveObjectInspector dObjectInspector = (PrimitiveObjectInspector)arguments[1];

    switch (xObjectInspector.getPrimitiveCategory()) {
      case VOID:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        break;
      default:
        throw new UDFArgumentTypeException(0, "Argument 1"
          + " of function FORMAT_NUMBER must be \""
          + serdeConstants.TINYINT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.SMALLINT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.INT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.BIGINT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.DOUBLE_TYPE_NAME + "\""
          + " or \"" + serdeConstants.FLOAT_TYPE_NAME + "\", but \""
          + arguments[0].getTypeName() + "\" was found.");
    }

    switch (dObjectInspector.getPrimitiveCategory()) {
      case VOID:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        break;
      default:
        throw new UDFArgumentTypeException(1, "Argument 2"
          + " of function FORMAT_NUMBER must be \""
          + serdeConstants.TINYINT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.SMALLINT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.INT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.BIGINT_TYPE_NAME + "\", but \""
          + arguments[1].getTypeName() + "\" was found.");
    }

    argumentOIs = arguments;
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    int dValue = ((IntObjectInspector) argumentOIs[1]).get(arguments[1].get());

    if (dValue < 0) {
      throw new HiveException("Argument 2 of function FORMAT_NUMBER must be >= 0, but \""
      + dValue + "\" was found");
    }

    if (dValue != lastDValue) {
      // construct a new DecimalFormat only if a new dValue
      pattern.delete(0, pattern.length());
      pattern.append("#,###,###,###,###,###,##0");

      //decimal place
      if (dValue > 0) {
        pattern.append(".");
        for (int i = 0; i < dValue; i++) {
          pattern.append("0");
        }
      }
      DecimalFormat dFormat = new DecimalFormat(pattern.toString());
      lastDValue = dValue;
      numberFormat.applyPattern(dFormat.toPattern());
    }

    double xDoubleValue = 0.0;
    int xIntValue = 0;
    long xLongValue = 0L;

    PrimitiveObjectInspector xObjectInspector = (PrimitiveObjectInspector)argumentOIs[0];
    switch (xObjectInspector.getPrimitiveCategory()) {
      case VOID:
      case FLOAT:
      case DOUBLE:
        xDoubleValue = ((DoubleObjectInspector) argumentOIs[0]).get(arguments[0].get());
        resultText.set(numberFormat.format(xDoubleValue));
        break;
      case BYTE:
      case SHORT:
      case INT:
        xIntValue = ((IntObjectInspector) argumentOIs[0]).get(arguments[0].get());
        resultText.set(numberFormat.format(xIntValue));
        break;
      case LONG:
        xLongValue = ((LongObjectInspector) argumentOIs[0]).get(arguments[0].get());
        resultText.set(numberFormat.format(xLongValue));
        break;
      default:
        throw new HiveException("Argument 1 of function FORMAT_NUMBER must be "
          + serdeConstants.TINYINT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.SMALLINT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.INT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.BIGINT_TYPE_NAME + "\""
          + " or \"" + serdeConstants.DOUBLE_TYPE_NAME + "\""
          + " or \"" + serdeConstants.FLOAT_TYPE_NAME + "\", but \""
          + argumentOIs[0].getTypeName() + "\" was found.");
    }
    return resultText;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    StringBuilder sb = new StringBuilder();
    sb.append("format_number(");
    for (int i = 0; i < children.length - 1; i++) {
      sb.append(children[i]).append(", ");
    }
    sb.append(children[children.length - 1]).append(")");
    return sb.toString();
  }
}
