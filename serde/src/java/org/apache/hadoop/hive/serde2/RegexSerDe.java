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
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
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
 * NOTE: Obviously, all columns have to be strings. Users can use
 * "CAST(a AS INT)" to convert columns to other types.
 *
 * NOTE: This implementation is using String, and javaStringObjectInspector. A
 * more efficient implementation should use UTF-8 encoded Text and
 * writableStringObjectInspector. We should switch to that when we have a UTF-8
 * based Regex library.
 */
public class RegexSerDe implements SerDe {

  public static final Log LOG = LogFactory.getLog(RegexSerDe.class.getName());

  int numColumns;
  String inputRegex;

  Pattern inputPattern;

  StructObjectInspector rowOI;
  ArrayList<String> row;
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
    String columnNameProperty = tbl.getProperty(Constants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
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
        row.set(c, m.group(c + 1));
      } catch (RuntimeException e) {
        partialMatchedRowsCount++;
          if (!alreadyLoggedPartialMatch) {
          // Report the row if its the first time
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

  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

}
