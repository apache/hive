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
package org.apache.hadoop.hive.contrib.serde2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.MissingFormatArgumentException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * RegexSerDe uses regular expression (regex) to serialize/deserialize.
 *
 * It can deserialize the data using regex and extracts groups as columns. It
 * can also serialize the row object using a format string.
 *
 * In deserialization stage, if a row does not match the regex, then all columns
 * in the row will be NULL. If a row matches the regex but has less than
 * expected groups, the missing groups will be NULL. If a row matches the regex
 * but has more than expected groups, the additional groups are just ignored.
 *
 * In serialization stage, it uses java string formatter to format the columns
 * into a row. If the output type of the column in a query is not a string, it
 * will be automatically converted to String by Hive.
 *
 * For the format of the format String, please refer to {@link http
 * ://java.sun.com/j2se/1.5.0/docs/api/java/util/Formatter.html#syntax}
 *
 * NOTE: Obviously, all columns have to be strings. Users can use
 * "CAST(a AS INT)" to convert columns to other types.
 *
 * NOTE: This implementation is using String, and javaStringObjectInspector. A
 * more efficient implementation should use UTF-8 encoded Text and
 * writableStringObjectInspector. We should switch to that when we have a UTF-8
 * based Regex library.
 */
public class RegexSerDe extends AbstractSerDe {

  public static final Log LOG = LogFactory.getLog(RegexSerDe.class.getName());

  int numColumns;
  String inputRegex;
  String outputFormatString;

  Pattern inputPattern;

  StructObjectInspector rowOI;
  ArrayList<String> row;

  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {

    // We can get the table definition from tbl.

    // Read the configuration parameters
    inputRegex = tbl.getProperty("input.regex");
    outputFormatString = tbl.getProperty("output.format.string");
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    boolean inputRegexIgnoreCase = "true".equalsIgnoreCase(tbl
        .getProperty("input.regex.case.insensitive"));

    // Parse the configuration parameters
    if (inputRegex != null) {
      inputPattern = Pattern.compile(inputRegex, Pattern.DOTALL
          + (inputRegexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));
    } else {
      inputPattern = null;
    }
    List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
    List<TypeInfo> columnTypes = TypeInfoUtils
        .getTypeInfosFromTypeString(columnTypeProperty);
    assert columnNames.size() == columnTypes.size();
    numColumns = columnNames.size();

    // All columns have to be of type STRING.
    for (int c = 0; c < numColumns; c++) {
      if (!columnTypes.get(c).equals(TypeInfoFactory.stringTypeInfo)) {
        throw new SerDeException(getClass().getName()
            + " only accepts string columns, but column[" + c + "] named "
            + columnNames.get(c) + " has type " + columnTypes.get(c));
      }
    }

    // Constructing the row ObjectInspector:
    // The row consists of some string columns, each column will be a java
    // String object.
    List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(
        columnNames.size());
    for (int c = 0; c < numColumns; c++) {
      columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }
    // StandardStruct uses ArrayList to store the row.
    rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
        columnNames, columnOIs);

    // Constructing the row object, etc, which will be reused for all rows.
    row = new ArrayList<String>(numColumns);
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
  long unmatchedRows = 0;
  long nextUnmatchedRows = 1;
  // Number of rows that match the regex but have missing groups.
  long partialMatchedRows = 0;
  long nextPartialMatchedRows = 1;

  long getNextNumberToDisplay(long now) {
    return now * 10;
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {

    if (inputPattern == null) {
      throw new SerDeException(
          "This table does not have serde property \"input.regex\"!");
    }
    Text rowText = (Text) blob;

    Matcher m = inputPattern.matcher(rowText.toString());

    // If do not match, ignore the line, return a row with all nulls.
    if (!m.matches()) {
      unmatchedRows++;
      if (unmatchedRows >= nextUnmatchedRows) {
        nextUnmatchedRows = getNextNumberToDisplay(nextUnmatchedRows);
        // Report the row
        LOG.warn("" + unmatchedRows + " unmatched rows are found: " + rowText);
      }
      return null;
    }

    // Otherwise, return the row.
    for (int c = 0; c < numColumns; c++) {
      try {
        row.set(c, m.group(c + 1));
      } catch (RuntimeException e) {
        partialMatchedRows++;
        if (partialMatchedRows >= nextPartialMatchedRows) {
          nextPartialMatchedRows = getNextNumberToDisplay(nextPartialMatchedRows);
          // Report the row
          LOG.warn("" + partialMatchedRows
              + " partially unmatched rows are found, " + " cannot find group "
              + c + ": " + rowText);
        }
        row.set(c, null);
      }
    }
    return row;
  }

  Object[] outputFields;
  Text outputRowText;

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {

    if (outputFormatString == null) {
      throw new SerDeException(
          "Cannot write data into table because \"output.format.string\""
          + " is not specified in serde properties of the table.");
    }

    // Get all the fields out.
    // NOTE: The correct way to get fields out of the row is to use
    // objInspector.
    // The obj can be a Java ArrayList, or a Java class, or a byte[] or
    // whatever.
    // The only way to access the data inside the obj is through
    // ObjectInspector.

    StructObjectInspector outputRowOI = (StructObjectInspector) objInspector;
    List<? extends StructField> outputFieldRefs = outputRowOI
        .getAllStructFieldRefs();
    if (outputFieldRefs.size() != numColumns) {
      throw new SerDeException("Cannot serialize the object because there are "
          + outputFieldRefs.size() + " fields but the table has " + numColumns
          + " columns.");
    }

    // Get all data out.
    for (int c = 0; c < numColumns; c++) {
      Object field = outputRowOI
          .getStructFieldData(obj, outputFieldRefs.get(c));
      ObjectInspector fieldOI = outputFieldRefs.get(c)
          .getFieldObjectInspector();
      // The data must be of type String
      StringObjectInspector fieldStringOI = (StringObjectInspector) fieldOI;
      // Convert the field to Java class String, because objects of String type
      // can be
      // stored in String, Text, or some other classes.
      outputFields[c] = fieldStringOI.getPrimitiveJavaObject(field);
    }

    // Format the String
    String outputRowString = null;
    try {
      outputRowString = String.format(outputFormatString, outputFields);
    } catch (MissingFormatArgumentException e) {
      throw new SerDeException("The table contains " + numColumns
          + " columns, but the outputFormatString is asking for more.", e);
    }
    outputRowText.set(outputRowString);
    return outputRowText;
  }

  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

}
