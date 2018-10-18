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

package org.apache.hadoop.hive.serde2.lazy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.Public;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Stable;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParameters;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SerDeParameters.
 *
 */
@Public
@Stable
public class LazySerDeParameters implements LazyObjectInspectorParameters {
  public static final Logger LOG = LoggerFactory.getLogger(LazySerDeParameters.class.getName());
  public static final byte[] DefaultSeparators = {(byte) 1, (byte) 2, (byte) 3};
  public static final String SERIALIZATION_EXTEND_NESTING_LEVELS
  	= "hive.serialization.extend.nesting.levels";
  public static final String SERIALIZATION_EXTEND_ADDITIONAL_NESTING_LEVELS
	= "hive.serialization.extend.additional.nesting.levels";

  private Properties tableProperties;
  private String serdeName;

  // The list of bytes used for the separators in the column (a nested struct 
  // such as Array<Array<int>> will use multiple separators).
  // The list of separators + escapeChar are the bytes required to be escaped.
  private byte[] separators;

  private Text nullSequence;

  private TypeInfo rowTypeInfo;
  private boolean lastColumnTakesRest;
  private List<String> columnNames;
  private List<TypeInfo> columnTypes;

  private boolean escaped;
  private byte escapeChar;
  private boolean[] needsEscape = new boolean[256];  // A flag for each byte to indicate if escape is needed.

  private boolean extendedBooleanLiteral;
  List<String> timestampFormats;
  
  public LazySerDeParameters(Configuration job, Properties tbl, String serdeName) throws SerDeException {
   this.tableProperties = tbl;
   this.serdeName = serdeName;

    String nullString = tbl.getProperty(
        serdeConstants.SERIALIZATION_NULL_FORMAT, "\\N");
    nullSequence = new Text(nullString);
    
    String lastColumnTakesRestString = tbl
        .getProperty(serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST);
    lastColumnTakesRest = (lastColumnTakesRestString != null && lastColumnTakesRestString
        .equalsIgnoreCase("true"));

    extractColumnInfo(job);

    // Create the LazyObject for storing the rows
    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);

    collectSeparators(tbl);

    // Get the escape information
    String escapeProperty = tbl.getProperty(serdeConstants.ESCAPE_CHAR);
    escaped = (escapeProperty != null);
    if (escaped) {
      escapeChar = LazyUtils.getByte(escapeProperty, (byte) '\\');
      needsEscape[escapeChar & 0xFF] = true;  // Converts the negative byte into positive index
      for (byte b : separators) {
        needsEscape[b & 0xFF] = true;         // Converts the negative byte into positive index
      }

      boolean isEscapeCRLF =
          Boolean.parseBoolean(tbl.getProperty(serdeConstants.SERIALIZATION_ESCAPE_CRLF));
      if (isEscapeCRLF) {
        needsEscape['\r'] = true;
        needsEscape['\n'] = true;
      }
    }

    extendedBooleanLiteral = (job == null ? false :
        job.getBoolean(ConfVars.HIVE_LAZYSIMPLE_EXTENDED_BOOLEAN_LITERAL.varname, false));
    
    String[] timestampFormatsArray =
        HiveStringUtils.splitAndUnEscape(tbl.getProperty(serdeConstants.TIMESTAMP_FORMATS));
    if (timestampFormatsArray != null) {
      timestampFormats = Arrays.asList(timestampFormatsArray);
    }

    LOG.debug(serdeName + " initialized with: columnNames="
        + columnNames + " columnTypes=" + columnTypes
        + " separator=" + Arrays.asList(separators)
        + " nullstring=" + nullString + " lastColumnTakesRest="
        + lastColumnTakesRest + " timestampFormats=" + timestampFormats);
  }

  /**
   * Extracts and set column names and column types from the table properties
   * @throws SerDeException
   */
  public void extractColumnInfo(Configuration conf) throws SerDeException {
    // Read the configuration parameters
    String columnNameProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMNS);
    // NOTE: if "columns.types" is missing, all columns will be of String type
    String columnTypeProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);

    // Parse the configuration parameters
    String columnNameDelimiter = tableProperties.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? tableProperties
        .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
    if (columnNameProperty != null && columnNameProperty.length() > 0) {
      columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
    } else {
      columnNames = new ArrayList<String>();
    }
    if (columnTypeProperty == null) {
      // Default type: all string
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < columnNames.size(); i++) {
        if (i > 0) {
          sb.append(":");
        }
        sb.append(serdeConstants.STRING_TYPE_NAME);
      }
      columnTypeProperty = sb.toString();
    }

    columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    // Insert time-zone for timestamp type
    if (conf != null) {
      final TimestampLocalTZTypeInfo tsTZTypeInfo = new TimestampLocalTZTypeInfo(
          conf.get(ConfVars.HIVE_LOCAL_TIME_ZONE.varname));
      for (int i = 0; i < columnTypes.size(); i++) {
        if (columnTypes.get(i) instanceof TimestampLocalTZTypeInfo) {
          columnTypes.set(i, tsTZTypeInfo);
        }
      }
    }

    if (columnNames.size() != columnTypes.size()) {
      throw new SerDeException(serdeName + ": columns has " + columnNames.size()
          + " elements while columns.types has " + columnTypes.size() + " elements!");
    }
  }

  public List<TypeInfo> getColumnTypes() {
    return columnTypes;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public byte[] getSeparators() {
    return separators;
  }

  public Text getNullSequence() {
    return nullSequence;
  }

  public TypeInfo getRowTypeInfo() {
    return rowTypeInfo;
  }

  public boolean isLastColumnTakesRest() {
    return lastColumnTakesRest;
  }

  public boolean isEscaped() {
    return escaped;
  }

  public byte getEscapeChar() {
    return escapeChar;
  }

  public boolean[] getNeedsEscape() {
    return needsEscape;
  }

  public boolean isExtendedBooleanLiteral() {
    return extendedBooleanLiteral;
  }

  public List<String> getTimestampFormats() {
    return timestampFormats;
  }
  
  public void setSeparator(int index, byte separator) throws SerDeException {
    if (index < 0 || index >= separators.length) {
      throw new SerDeException("Invalid separator array index value: " + index);
    }

    separators[index] = separator;
  }

  /**
   * To be backward-compatible, initialize the first 3 separators to 
   * be the given values from the table properties. The default number of separators is 8; if only
   * hive.serialization.extend.nesting.levels is set, the number of
   * separators is extended to 24; if hive.serialization.extend.additional.nesting.levels
   * is set, the number of separators is extended to 154.
   * @param tableProperties table properties to extract the user provided separators
   */
  private void collectSeparators(Properties tableProperties) {	
    List<Byte> separatorCandidates = new ArrayList<Byte>();

    String extendNestingValue = tableProperties.getProperty(SERIALIZATION_EXTEND_NESTING_LEVELS);
    String extendAdditionalNestingValue = tableProperties.getProperty(SERIALIZATION_EXTEND_ADDITIONAL_NESTING_LEVELS);
    boolean extendedNesting = extendNestingValue != null && extendNestingValue.equalsIgnoreCase("true");
    boolean extendedAdditionalNesting = extendAdditionalNestingValue != null 
    		&& extendAdditionalNestingValue.equalsIgnoreCase("true");

    separatorCandidates.add(LazyUtils.getByte(tableProperties.getProperty(serdeConstants.FIELD_DELIM,
        tableProperties.getProperty(serdeConstants.SERIALIZATION_FORMAT)), DefaultSeparators[0]));
    separatorCandidates.add(LazyUtils.getByte(tableProperties
        .getProperty(serdeConstants.COLLECTION_DELIM), DefaultSeparators[1]));
    separatorCandidates.add(LazyUtils.getByte(
        tableProperties.getProperty(serdeConstants.MAPKEY_DELIM), DefaultSeparators[2]));
    
    //use only control chars that are very unlikely to be part of the string
    // the following might/likely to be used in text files for strings
    // 9 (horizontal tab, HT, \t, ^I)
    // 10 (line feed, LF, \n, ^J),
    // 12 (form feed, FF, \f, ^L),
    // 13 (carriage return, CR, \r, ^M),
    // 27 (escape, ESC, \e [GCC only], ^[).
    for (byte b = 4; b <= 8; b++ ) {
    	separatorCandidates.add(b);
    }
    separatorCandidates.add((byte)11);
    for (byte b = 14; b <= 26; b++ ) {
      separatorCandidates.add(b);
    }
    for (byte b = 28; b <= 31; b++ ) {
      separatorCandidates.add(b);
    }

    for (byte b = -128; b <= -1; b++ ) {
      separatorCandidates.add(b);
    }

    int numSeparators = 8;
    if(extendedAdditionalNesting) {
      numSeparators = separatorCandidates.size();
    } else if (extendedNesting) {
      numSeparators = 24;
    }

    separators = new byte[numSeparators];
    for (int i = 0; i < numSeparators; i++) {
      separators[i] = separatorCandidates.get(i);
    }
  }
}
