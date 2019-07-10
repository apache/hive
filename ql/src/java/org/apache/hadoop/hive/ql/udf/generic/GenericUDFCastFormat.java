/*
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.common.format.datetime.HiveSqlDateTimeFormatter;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * CAST(<value> AS <type> FORMAT <STRING>).
 *
 * Vector expressions: CastDateToCharWithFormat, CastDateToStringWithFormat,
 *     CastDateToVarCharWithFormat, CastTimestampToCharWithFormat,
 *     CastTimestampToStringWithFormat, CastTimestampToVarCharWithFormat.
 * Could not use @VectorizedExpressions annotation because e.g. CastXToCharWithFormat,
 * CastXToStringWithFormat, CastXToVarCharWithFormat would have same description.
 */
@Description(name = "cast_format",
    value = "CAST(<value> AS <type> FORMAT <STRING>) - Converts a datetime value to string or"
        + " string-type value to datetime based on the format pattern specified.",
    extended =  "If format is specified with FORMAT argument then SQL:2016 datetime formats will "
        + "be used.\n"
        + "Example:\n "
        + "  > SELECT CAST(\"2018-01-01 4 PM\" AS timestamp FORMAT \"yyyy-mm-dd hh12 AM\");\n"
        + "  2018-01-01 16:00:00")
public class GenericUDFCastFormat extends GenericUDF implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(GenericUDFCastFormat.class.getName());

  @VisibleForTesting
  static final Map<Integer, String> OUTPUT_TYPES = ImmutableMap.<Integer, String>builder()
      .put(HiveParser_IdentifiersParser.TOK_STRING, serdeConstants.STRING_TYPE_NAME)
      .put(HiveParser_IdentifiersParser.TOK_VARCHAR, serdeConstants.VARCHAR_TYPE_NAME)
      .put(HiveParser_IdentifiersParser.TOK_CHAR, serdeConstants.CHAR_TYPE_NAME)
      .put(HiveParser_IdentifiersParser.TOK_TIMESTAMP, serdeConstants.TIMESTAMP_TYPE_NAME)
      .put(HiveParser_IdentifiersParser.TOK_DATE, serdeConstants.DATE_TYPE_NAME).build();

  private transient HiveSqlDateTimeFormatter formatter;
  private transient PrimitiveObjectInspector outputOI;
  private transient PrimitiveObjectInspector inputOI;

  public GenericUDFCastFormat() {
  }

  /**
   * @param arguments
   *  0. const int, value of a HiveParser_IdentifiersParser constant which represents a TOK_[TYPE]
   *  1. expression to convert
   *  2. constant string, format pattern
   *  3. (optional) constant int, output char/varchar length
   */
  @Override public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length != 3 && arguments.length != 4) {
      throw new UDFArgumentException(
          "Function cast_format requires 3 or 4 arguments (int, expression, StringLiteral"
              + "[, var/char length]), got " + arguments.length);
    }

    outputOI = getOutputOI(arguments);
    try {
      inputOI = (PrimitiveObjectInspector) arguments[1];
    } catch (ClassCastException e) {
      throw new UDFArgumentException(
          "Function CAST...as ... FORMAT ...takes only primitive types");
    }
    PrimitiveObjectInspectorUtils.PrimitiveGrouping inputPG =
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(inputOI.getPrimitiveCategory());
    PrimitiveObjectInspectorUtils.PrimitiveGrouping outputPG =
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(outputOI.getPrimitiveCategory());

    if (inputOI.getPrimitiveCategory()
        == PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMPLOCALTZ) {
      throw new UDFArgumentException(
          "Timestamp with local time zone not yet supported for cast ... format function");
    }
    if (!(inputPG == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP
        && outputPG == PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP
        || inputPG == PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP
        && outputPG == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP
        || inputPG == PrimitiveObjectInspectorUtils.PrimitiveGrouping.VOID_GROUP)) {
      throw new UDFArgumentException(
          "Function CAST...as ... FORMAT ... only converts datetime objects to string types"
              + " and string or void objects to datetime types. Type of object provided: "
              + outputOI.getPrimitiveCategory() + " in primitive grouping " + inputPG
              + ", type provided: " + inputOI.getPrimitiveCategory() + " in primitive grouping "
              + outputPG);
    }

    boolean forParsing = (outputPG == PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP);
    formatter =  new HiveSqlDateTimeFormatter(getConstantStringValue(arguments, 2), forParsing);
    return outputOI;
  }

  private PrimitiveObjectInspector getOutputOI(ObjectInspector[] arguments)
      throws UDFArgumentException {
    int key = getConstantIntValue(arguments, 0);
    if (!OUTPUT_TYPES.keySet().contains(key)) {
      throw new UDFArgumentException("Cast...format can only convert to DATE, TIMESTAMP, STRING,"
          + "VARCHAR, CHAR. Can't convert to HiveParser_IdentifiersParser constant with value "
          + key);
    }
    String typeString = OUTPUT_TYPES.get(key);
    if (serdeConstants.VARCHAR_TYPE_NAME.equals(typeString)
        || serdeConstants.CHAR_TYPE_NAME.equals(typeString)) {
      if (arguments.length < 4 || arguments[3] == null) {
        throw new UDFArgumentException(typeString + " missing length argument");
      }
      typeString += "(" + getConstantIntValue(arguments, 3) + ")";
    }
    PrimitiveTypeInfo typeInfo = TypeInfoFactory.getPrimitiveTypeInfo(typeString);
    return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(typeInfo);
  }

  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object o0 = arguments[1].get();
    if (o0 == null) {
      return null;
    }
    return convert(o0);
  }

  private Object convert(Object o) throws HiveException {
    Object input;
    switch (inputOI.getPrimitiveCategory()) {
    case STRING:
      input = ((StringObjectInspector) inputOI).getPrimitiveJavaObject(o);
      break;
    case CHAR:
      input = ((HiveCharObjectInspector) inputOI).getPrimitiveJavaObject(o).getStrippedValue();
      break;
    case VARCHAR:
      input = ((HiveVarcharObjectInspector) inputOI).getPrimitiveJavaObject(o).toString();
      break;
    case TIMESTAMP:
      input = ((TimestampObjectInspector) inputOI).getPrimitiveWritableObject(o).getTimestamp();
      break;
    case DATE:
      input = ((DateObjectInspector) inputOI).getPrimitiveWritableObject(o).get();
      break;
    default:
      throw new HiveException("Input type " + inputOI.getPrimitiveCategory() + " not valid");
    }

    // format here
    Object formattedOutput = null;
    if (inputOI.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DATE
        || inputOI.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP) {
      if (inputOI.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DATE) {
        try {
          formattedOutput = formatter.format((Date) input);
        } catch (IllegalArgumentException e) {
          return null;
        }
      } else {
        try {
          formattedOutput = formatter.format((Timestamp) input);
        } catch (IllegalArgumentException e) {
          return null;
        }
      }
      if (formattedOutput == null) {
        return null;
      }
    }

    // parse and create Writables
    switch (outputOI.getPrimitiveCategory()) {
    case STRING:
      return new Text((String) formattedOutput);
    case CHAR:
      return ((SettableHiveCharObjectInspector) outputOI)
          .create(new HiveChar((String) formattedOutput, -1));
    case VARCHAR:
      return ((SettableHiveVarcharObjectInspector) outputOI)
          .create(new HiveVarchar((String) formattedOutput, -1));
    case TIMESTAMP:
      try {
        Timestamp t = formatter.parseTimestamp((String) input);
        if (t == null) {
          return null;
        }
        return ((SettableTimestampObjectInspector) outputOI).create(t);
      } catch (IllegalArgumentException e) {
        return null;
      }

    case DATE:
      try {
        Date d = formatter.parseDate((String) input);
        if (d == null) {
          return null;
        }
        return ((SettableDateObjectInspector) outputOI).create(d);
      } catch (IllegalArgumentException e) {
        return null;
      }
    default:
      throw new HiveException("Output type " + outputOI.getPrimitiveCategory() + " not valid");
    }
  }

  @Override public String getDisplayString(String[] children) {
    assert children.length == 3 || children.length == 4;
    StringBuilder sb = new StringBuilder();
    sb.append("CAST( ");
    sb.append(children[1]);
    sb.append(" AS ");
    int typeKey = Integer.parseInt(children[0]);
    if (!OUTPUT_TYPES.keySet().contains(typeKey)) {
      sb.append("HiveParsers_IdentifiersParser index ").append(typeKey);
    } else {
      sb.append(OUTPUT_TYPES.get(typeKey));
      if (children.length == 4) {
        sb.append("(").append(children[3]).append(")");
      }
    }
    sb.append(" FORMAT ");
    sb.append(children[2]);
    sb.append(" )");
    return sb.toString();
  }
}
