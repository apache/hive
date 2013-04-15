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
package org.apache.hadoop.hive.serde2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * RegexSerDe uses regular expression (regex) to deserialize data. It doesn't
 * support data serialization.
 *
 * It can deserialize the data using regex and extracts groups as columns.
 *
 * In deserialization stage, if a row does not match the regex, then all columns
 * in the row will be NULL. If a row matches the regex but has less than
 * expected groups, the missing groups will be NULL. If a row matches the regex
 * but has more than expected groups, the additional groups are just ignored.
 *
 * NOTE: Regex SerDe supports primitive column types such as TINYINT, SMALLINT,
 * INT, BIGINT, FLOAT, DOUBLE, STRING, BOOLEAN and DECIMAL
 *
 *
 * NOTE: This implementation uses javaStringObjectInspector for STRING. A
 * more efficient implementation should use UTF-8 encoded Text and
 * writableStringObjectInspector. We should switch to that when we have a UTF-8
 * based Regex library.
 */
public class RegexSerDe extends AbstractSerDe {

  public static final Log LOG = LogFactory.getLog(RegexSerDe.class.getName());

  int numColumns;
  String inputRegex;

  Pattern inputPattern;

  StructObjectInspector rowOI;
  List<Object> row;
  List<TypeInfo> columnTypes;
  Object[] outputFields;
  Text outputRowText;

  boolean alreadyLoggedNoMatch = false;
  boolean alreadyLoggedPartialMatch = false;

  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {

    // We can get the table definition from tbl.

    // Read the configuration parameters
    inputRegex = tbl.getProperty("input.regex");
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    boolean inputRegexIgnoreCase = "true".equalsIgnoreCase(tbl
        .getProperty("input.regex.case.insensitive"));

    // output format string is not supported anymore, warn user of deprecation
    if (null != tbl.getProperty("output.format.string")) {
      LOG.warn("output.format.string has been deprecated");
    }

    // Parse the configuration parameters
    if (inputRegex != null) {
      inputPattern = Pattern.compile(inputRegex, Pattern.DOTALL
          + (inputRegexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));
    } else {
      inputPattern = null;
      throw new SerDeException(
          "This table does not have serde property \"input.regex\"!");
    }


    List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
    columnTypes = TypeInfoUtils
        .getTypeInfosFromTypeString(columnTypeProperty);
    assert columnNames.size() == columnTypes.size();
    numColumns = columnNames.size();

    /* Constructing the row ObjectInspector:
     * The row consists of some set of primitive columns, each column will
     * be a java object of primitive type.
     */
    List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());
    for (int c = 0; c < numColumns; c++) {
      String typeName = columnTypes.get(c).getTypeName();
      if (typeName.equals(serdeConstants.STRING_TYPE_NAME)) {
        columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      } else if (typeName.equals(serdeConstants.TINYINT_TYPE_NAME)) {
        columnOIs.add(PrimitiveObjectInspectorFactory.javaByteObjectInspector);
      } else if (typeName.equals(serdeConstants.SMALLINT_TYPE_NAME)) {
       columnOIs.add(PrimitiveObjectInspectorFactory.javaShortObjectInspector);
      } else if (typeName.equals(serdeConstants.INT_TYPE_NAME)) {
        columnOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
      } else if (typeName.equals(serdeConstants.BIGINT_TYPE_NAME)) {
       columnOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
      } else if (typeName.equals(serdeConstants.FLOAT_TYPE_NAME)) {
        columnOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
      } else if (typeName.equals(serdeConstants.DOUBLE_TYPE_NAME)) {
       columnOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
      } else if (typeName.equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
        columnOIs.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
      } else if (typeName.equals(serdeConstants.DECIMAL_TYPE_NAME)) {
        columnOIs.add(PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector);
      } else {
         throw new SerDeException(getClass().getName()
         + " doesn't allow column [" + c + "] named "
         + columnNames.get(c) + " with type " + columnTypes.get(c));
       }
     }

    // StandardStruct uses ArrayList to store the row.
    rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
        columnNames, columnOIs);

    row = new ArrayList<Object>(numColumns);
    // Constructing the row object, etc, which will be reused for all rows.
    for (int c = 0; c < numColumns; c++) {
      row.add(null);
    }
    outputFields = new Object[numColumns];
    outputRowText = new Text();
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowOI;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  // Number of rows not matching the regex
  long unmatchedRowsCount = 0;
  // Number of rows that match the regex but have missing groups.
  long partialMatchedRowsCount = 0;

  @Override
  public Object deserialize(Writable blob) throws SerDeException {

    Text rowText = (Text) blob;
    Matcher m = inputPattern.matcher(rowText.toString());

    if (m.groupCount() != numColumns) {
      throw new SerDeException("Number of matching groups doesn't match the number of columns");
    }

    // If do not match, ignore the line, return a row with all nulls.
    if (!m.matches()) {
      unmatchedRowsCount++;
        if (!alreadyLoggedNoMatch) {
         // Report the row if its the first time
         LOG.warn("" + unmatchedRowsCount + " unmatched rows are found: " + rowText);
         alreadyLoggedNoMatch = true;
      }
      return null;
    }

    // Otherwise, return the row.
    for (int c = 0; c < numColumns; c++) {
      try {
        String t = m.group(c+1);
        String typeName = columnTypes.get(c).getTypeName();

        // Convert the column to the correct type when needed and set in row obj
        if (typeName.equals(serdeConstants.STRING_TYPE_NAME)) {
          row.set(c, t);
        } else if (typeName.equals(serdeConstants.TINYINT_TYPE_NAME)) {
          Byte b;
          b = Byte.valueOf(t);
          row.set(c,b);
        } else if (typeName.equals(serdeConstants.SMALLINT_TYPE_NAME)) {
          Short s;
          s = Short.valueOf(t);
          row.set(c,s);
        } else if (typeName.equals(serdeConstants.INT_TYPE_NAME)) {
          Integer i;
          i = Integer.valueOf(t);
          row.set(c, i);
        } else if (typeName.equals(serdeConstants.BIGINT_TYPE_NAME)) {
          Long l;
          l = Long.valueOf(t);
          row.set(c, l);
        } else if (typeName.equals(serdeConstants.FLOAT_TYPE_NAME)) {
          Float f;
          f = Float.valueOf(t);
          row.set(c,f);
        } else if (typeName.equals(serdeConstants.DOUBLE_TYPE_NAME)) {
          Double d;
          d = Double.valueOf(t);
          row.set(c,d);
        } else if (typeName.equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
          Boolean b;
          b = Boolean.valueOf(t);
          row.set(c, b);
        } else if (typeName.equals(serdeConstants.DECIMAL_TYPE_NAME)) {
          HiveDecimal bd;
          bd = new HiveDecimal(t);
          row.set(c, bd);
        }
      } catch (RuntimeException e) {
         partialMatchedRowsCount++;
         if (!alreadyLoggedPartialMatch) {
         // Report the row if its the first row
         LOG.warn("" + partialMatchedRowsCount
            + " partially unmatched rows are found, " + " cannot find group "
            + c + ": " + rowText);
           alreadyLoggedPartialMatch = true;
        }
        row.set(c, null);
       }
     }
    return row;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
        throw new UnsupportedOperationException(
          "Regex SerDe doesn't support the serialize() method");
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }
}
