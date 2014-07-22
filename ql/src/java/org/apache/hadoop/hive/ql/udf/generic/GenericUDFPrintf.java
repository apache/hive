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

import java.util.ArrayList;
import java.util.Formatter;
import java.util.Locale;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * Generic UDF for printf function
 * <code>printf(String format, Obj... args)</code>.
 *
 * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF
 */
@Description(name = "printf",
    value = "_FUNC_(String format, Obj... args) - "
    + "function that can format strings according to printf-style format strings",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(\"Hello World %d %s\", 100, \"days\")"
    + "FROM src LIMIT 1;\n"
    + "  \"Hello World 100 days\"")
public class GenericUDFPrintf extends GenericUDF {
  private transient ObjectInspector[] argumentOIs;
  protected transient Converter converterFormat;

  private final Text resultText = new Text();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1) {
      throw new UDFArgumentLengthException(
          "The function PRINTF(String format, Obj... args) needs at least one arguments.");
    }

    WritableStringObjectInspector resultOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;

    if (arguments[0].getCategory() == ObjectInspector.Category.PRIMITIVE) {
      PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) arguments[0]);
      if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING ||
          poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.CHAR ||
          poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.VARCHAR ||
          poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.VOID) {
        converterFormat = ObjectInspectorConverters.getConverter(arguments[0], resultOI);
      } else {
        throw new UDFArgumentTypeException(0, "Argument 1"
            + " of function PRINTF must be \"" + serdeConstants.STRING_TYPE_NAME
            + "\", but \"" + arguments[0].getTypeName() + "\" was found.");
      }
    } else {
      throw new UDFArgumentTypeException(0, "Argument 1"
          + " of function PRINTF must be \"" + serdeConstants.STRING_TYPE_NAME
          + "\", but \"" + arguments[0].getTypeName() + "\" was found.");
    }

    for (int i = 1; i < arguments.length; i++) {
      if (!arguments[i].getCategory().equals(Category.PRIMITIVE)){
        throw new UDFArgumentTypeException(i, "Argument " + (i + 1)
        + " of function PRINTF must be \"" + Category.PRIMITIVE
        + "\", but \"" + arguments[i].getTypeName() + "\" was found.");
      }
    }

    argumentOIs = arguments;
    return resultOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    // If the first argument is null, return null. (It's okay for other arguments to be null, in
    // which case, "null" will be printed.)
    if (arguments[0].get() == null) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    Formatter formatter = new Formatter(sb, Locale.US);

    Text pattern = (Text)converterFormat.convert(arguments[0].get());

    ArrayList<Object> argumentList = new ArrayList<Object>();
    for (int i = 1; i < arguments.length; i++) {
      switch (((PrimitiveObjectInspector)argumentOIs[i]).getPrimitiveCategory()) {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case CHAR:
        case VARCHAR:
        case STRING:
        case TIMESTAMP:
        case DECIMAL:
          argumentList.add(((PrimitiveObjectInspector)argumentOIs[i])
            .getPrimitiveJavaObject(arguments[i].get()));
          break;
        default:
          argumentList.add(arguments[i].get());
          break;
      }
    }
    formatter.format(pattern.toString(), argumentList.toArray());

    resultText.set(sb.toString());
    return resultText;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length >= 2);
    StringBuilder sb = new StringBuilder();
    sb.append("printf(");
    for (int i = 0; i < children.length - 1; i++) {
      sb.append(children[i]).append(", ");
    }
    sb.append(children[children.length - 1]).append(")");
    return sb.toString();
  }
}
