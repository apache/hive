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
package org.apache.hadoop.hive.contrib.serde2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.MissingFormatArgumentException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractEncodingAwareSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
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
 * For the format of the format String, please refer to link: http
 * ://java.sun.com/j2se/1.5.0/docs/api/java/util/Formatter.html#syntax
 *
 * NOTE: Obviously, all columns have to be strings. Users can use
 * "CAST(a AS INT)" to convert columns to other types.
 *
 * NOTE: This implementation is using String, and javaStringObjectInspector. A
 * more efficient implementation should use UTF-8 encoded Text and
 * writableStringObjectInspector. We should switch to that when we have a UTF-8
 * based Regex library.
 */
@SerDeSpec(schemaProps = {
    serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES, serdeConstants.SERIALIZATION_ENCODING,
    RegexSerDe.INPUT_REGEX, RegexSerDe.OUTPUT_FORMAT_STRING,
    RegexSerDe.INPUT_REGEX_CASE_SENSITIVE })
public class RegexSerDe extends AbstractEncodingAwareSerDe {

  private static final Logger LOG = LoggerFactory.getLogger(RegexSerDe.class);

  public static final String INPUT_REGEX = "input.regex";
  public static final String OUTPUT_FORMAT_STRING = "output.format.string";
  public static final String INPUT_REGEX_CASE_SENSITIVE = "input.regex.case.insensitive";

  int numColumns;
  String inputRegex;
  String outputFormatString;

  Pattern inputPattern;

  StructObjectInspector rowOI;
  ArrayList<String> row;

  @Override
  public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties)
      throws SerDeException {
   super.initialize(configuration, tableProperties, partitionProperties);

   numColumns = this.getColumnNames().size();

   // Read the configuration parameters
   inputRegex = properties.getProperty(INPUT_REGEX);
   outputFormatString = properties.getProperty(OUTPUT_FORMAT_STRING);
   boolean inputRegexIgnoreCase = "true".equalsIgnoreCase(properties
       .getProperty(INPUT_REGEX_CASE_SENSITIVE));

   // Parse the configuration parameters
   if (inputRegex != null) {
     inputPattern = Pattern.compile(inputRegex, Pattern.DOTALL
         + (inputRegexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));
   } else {
     inputPattern = null;
   }

    // All columns have to be of type STRING
   int i = 0;
   for (TypeInfo type : getColumnTypes()) {
     if (!type.equals(TypeInfoFactory.stringTypeInfo)) {
       throw new SerDeException(getClass().getName() + " only accepts string columns, but column[" + i + "] named "
           + getColumnNames().get(i) + " has type " + type);
     }
     i++;
   }

   // Constructing the row ObjectInspector:
   // The row consists of some string columns, each column will be a java
   // String object.
    List<ObjectInspector> columnOIs =
        Collections.nCopies(numColumns, PrimitiveObjectInspectorFactory.javaStringObjectInspector);

   // StandardStruct uses ArrayList to store the row.
    rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(getColumnNames(), columnOIs);

   // Constructing the row object, etc, which will be reused for all rows.
   row = new ArrayList<>(Collections.nCopies(numColumns, null));
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
  public Object doDeserialize(Writable blob) throws SerDeException {

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
        LOG.warn("{} unmatched rows are found: {}", unmatchedRows, rowText);
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
  public Writable doSerialize(Object obj, ObjectInspector objInspector)
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

  @Override
  protected Writable transformFromUTF8(Writable blob) {
    Text text = (Text)blob;
    return SerDeUtils.transformTextFromUTF8(text, this.charset);
  }

  @Override
  protected Writable transformToUTF8(Writable blob) {
    Text text = (Text)blob;
    return SerDeUtils.transformTextToUTF8(text, this.charset);
  }

}
