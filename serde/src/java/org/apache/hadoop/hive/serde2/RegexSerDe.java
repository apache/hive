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
package org.apache.hadoop.hive.serde2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

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
@SerDeSpec(schemaProps = {
    serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
    RegexSerDe.INPUT_REGEX, RegexSerDe.INPUT_REGEX_CASE_SENSITIVE })
public class RegexSerDe extends AbstractSerDe {

  public static final Logger LOG = LoggerFactory.getLogger(RegexSerDe.class.getName());

  public static final String INPUT_REGEX = "input.regex";
  public static final String INPUT_REGEX_CASE_SENSITIVE = "input.regex.case.insensitive";

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
    inputRegex = tbl.getProperty(INPUT_REGEX);
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    boolean inputRegexIgnoreCase = "true".equalsIgnoreCase(tbl
        .getProperty(INPUT_REGEX_CASE_SENSITIVE));

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

    final String columnNameDelimiter = tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? tbl
        .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
    List<String> columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
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
      TypeInfo typeInfo = columnTypes.get(c);
      if (typeInfo instanceof PrimitiveTypeInfo) {
        PrimitiveTypeInfo pti = (PrimitiveTypeInfo) columnTypes.get(c);
        AbstractPrimitiveJavaObjectInspector oi =
            PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(pti);
        columnOIs.add(oi);
      } else {
        throw new SerDeException(getClass().getName()
            + " doesn't allow column [" + c + "] named "
            + columnNames.get(c) + " with type " + columnTypes.get(c));
      }
     }

    // StandardStruct uses ArrayList to store the row.
    rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
        columnNames,columnOIs,Lists.newArrayList(Splitter.on('\0').split(tbl.getProperty("columns.comments"))));

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
        TypeInfo typeInfo = columnTypes.get(c);

        // Convert the column to the correct type when needed and set in row obj
        PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
        switch (pti.getPrimitiveCategory()) {
        case STRING:
          row.set(c, t);
          break;
        case BYTE:
          Byte b;
          b = Byte.valueOf(t);
          row.set(c,b);
          break;
        case SHORT:
          Short s;
          s = Short.valueOf(t);
          row.set(c,s);
          break;
        case INT:
          Integer i;
          i = Integer.valueOf(t);
          row.set(c, i);
          break;
        case LONG:
          Long l;
          l = Long.valueOf(t);
          row.set(c, l);
          break;
        case FLOAT:
          Float f;
          f = Float.valueOf(t);
          row.set(c,f);
          break;
        case DOUBLE:
          Double d;
          d = Double.valueOf(t);
          row.set(c,d);
          break;
        case BOOLEAN:
          Boolean bool;
          bool = Boolean.valueOf(t);
          row.set(c, bool);
          break;
        case TIMESTAMP:
          Timestamp ts;
          ts = Timestamp.valueOf(t);
          row.set(c, ts);
          break;
        case DATE:
          Date date;
          date = Date.valueOf(t);
          row.set(c, date);
          break;
        case DECIMAL:
          HiveDecimal bd = HiveDecimal.create(t);
          row.set(c, bd);
          break;
        case CHAR:
          HiveChar hc = new HiveChar(t, ((CharTypeInfo) typeInfo).getLength());
          row.set(c, hc);
          break;
        case VARCHAR:
          HiveVarchar hv = new HiveVarchar(t, ((VarcharTypeInfo)typeInfo).getLength());
          row.set(c, hv);
          break;
        default:
          throw new SerDeException("Unsupported type " + typeInfo);
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
