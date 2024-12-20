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

package org.apache.hadoop.hive.ql.ddl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.TimestampColumnStatsData;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hive.common.util.HiveStringUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Utilities for SHOW ... commands.
 */
public final class ShowUtils {
  private ShowUtils() {
    throw new UnsupportedOperationException("ShowUtils should not be instantiated");
  }

  public static DataOutputStream getOutputStream(Path outputFile, DDLOperationContext context) throws HiveException {
    try {
      FileSystem fs = outputFile.getFileSystem(context.getConf());
      return fs.create(outputFile);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Creates a String from the properties in this format:
   *    'property_name1'='property_value1',
   *    'property_name2'='property_value2',
   *    ...
   *
   * Properties are listed in alphabetical order.
   *
   * @param properties The properties to list.
   * @param exclude Property names to exclude.
   */
  public static String propertiesToString(Map<String, String> properties, Set<String> exclude) {
    if (properties.isEmpty()) {
      return "";
    }

    SortedMap<String, String> sortedProperties = new TreeMap<>(properties);
    List<String> realProps = new ArrayList<>();
    for (Map.Entry<String, String> e : sortedProperties.entrySet()) {
      if (e.getValue() != null && !exclude.contains(e.getKey())) {
        realProps.add("  '" + e.getKey() + "'='" + HiveStringUtils.escapeHiveCommand(e.getValue()) + "'");
      }
    }
    return String.join(", \n", realProps);
  }

  public static void writeToFile(String data, String file, DDLOperationContext context) throws IOException {
    if (StringUtils.isEmpty(data)) {
      return;
    }

    Path resFile = new Path(file);
    FileSystem fs = resFile.getFileSystem(context.getConf());
    try (FSDataOutputStream out = fs.create(resFile);
         OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
      writer.write(data);
      writer.write((char) Utilities.newLineCode);
      writer.flush();
    }
  }

  public static void appendNonNull(StringBuilder builder, Object value) {
    appendNonNull(builder, value, false);
  }

  public static void appendNonNull(StringBuilder builder, Object value, boolean firstColumn) {
    if (!firstColumn) {
      builder.append((char)Utilities.tabCode);
    } else if (builder.length() > 0) {
      builder.append((char)Utilities.newLineCode);
    }
    if (value != null) {
      builder.append(value);
    }
  }

  // kept for backward compatibility since it's a public static method
  @SuppressWarnings("unused")
  public static String[] extractColumnValues(FieldSchema column, boolean isColumnStatsAvailable,
      ColumnStatisticsObj columnStatisticsObj) {
    return extractColumnValues(column, isColumnStatsAvailable, columnStatisticsObj, false);
  }

  public static String[] extractColumnValues(FieldSchema column, boolean isColumnStatsAvailable,
      ColumnStatisticsObj columnStatisticsObj, boolean histogramEnabled) {
    List<String> values = new ArrayList<>();
    values.add(column.getName());
    values.add(column.getType());

    if (isColumnStatsAvailable) {
      if (columnStatisticsObj != null) {
        ColumnStatisticsData statsData = columnStatisticsObj.getStatsData();
        if (statsData.isSetBinaryStats()) {
          BinaryColumnStatsData binaryStats = statsData.getBinaryStats();
          values.addAll(Lists.newArrayList("", "", "" + binaryStats.getNumNulls(), "",
              "" + binaryStats.getAvgColLen(), "" + binaryStats.getMaxColLen(), "", "",
              convertToString(binaryStats.getBitVectors())));
          if (histogramEnabled) {
            values.add("");
          }
        } else if (statsData.isSetStringStats()) {
          StringColumnStatsData stringStats = statsData.getStringStats();
          values.addAll(Lists.newArrayList("", "", "" + stringStats.getNumNulls(), "" + stringStats.getNumDVs(),
              "" + stringStats.getAvgColLen(), "" + stringStats.getMaxColLen(), "", "",
              convertToString(stringStats.getBitVectors())));
          if (histogramEnabled) {
            values.add("");
          }
        } else if (statsData.isSetBooleanStats()) {
          BooleanColumnStatsData booleanStats = statsData.getBooleanStats();
          values.addAll(Lists.newArrayList("", "", "" + booleanStats.getNumNulls(), "", "", "",
              "" + booleanStats.getNumTrues(), "" + booleanStats.getNumFalses(),
              convertToString(booleanStats.getBitVectors())));
          if (histogramEnabled) {
            values.add("");
          }
        } else if (statsData.isSetDecimalStats()) {
          DecimalColumnStatsData decimalStats = statsData.getDecimalStats();
          values.addAll(Lists.newArrayList(convertToString(decimalStats.getLowValue()),
              convertToString(decimalStats.getHighValue()), "" + decimalStats.getNumNulls(),
              "" + decimalStats.getNumDVs(), "", "", "", "", convertToString(decimalStats.getBitVectors())));
          if (histogramEnabled) {
            values.add(convertHistogram(statsData.getDecimalStats().getHistogram(), statsData.getSetField()));
          }
        } else if (statsData.isSetDoubleStats()) {
          DoubleColumnStatsData doubleStats = statsData.getDoubleStats();
          values.addAll(Lists.newArrayList("" + doubleStats.getLowValue(), "" + doubleStats.getHighValue(),
              "" + doubleStats.getNumNulls(), "" + doubleStats.getNumDVs(), "", "", "", "",
              convertToString(doubleStats.getBitVectors())));
          if (histogramEnabled) {
            values.add(convertHistogram(statsData.getDoubleStats().getHistogram(), statsData.getSetField()));
          }
        } else if (statsData.isSetLongStats()) {
          LongColumnStatsData longStats = statsData.getLongStats();
          values.addAll(Lists.newArrayList("" + longStats.getLowValue(), "" + longStats.getHighValue(),
              "" + longStats.getNumNulls(), "" + longStats.getNumDVs(), "", "", "", "",
              convertToString(longStats.getBitVectors())));
          if (histogramEnabled) {
            values.add(convertHistogram(statsData.getLongStats().getHistogram(), statsData.getSetField()));
          }
        } else if (statsData.isSetDateStats()) {
          DateColumnStatsData dateStats = statsData.getDateStats();
          values.addAll(Lists.newArrayList(convertToString(dateStats.getLowValue()),
              convertToString(dateStats.getHighValue()), "" + dateStats.getNumNulls(), "" + dateStats.getNumDVs(),
              "", "", "", "", convertToString(dateStats.getBitVectors())));
          if (histogramEnabled) {
            values.add(convertHistogram(statsData.getDateStats().getHistogram(), statsData.getSetField()));
          }
        } else if (statsData.isSetTimestampStats()) {
          TimestampColumnStatsData timestampStats = statsData.getTimestampStats();
          values.addAll(Lists.newArrayList(convertToString(timestampStats.getLowValue()),
              convertToString(timestampStats.getHighValue()), "" + timestampStats.getNumNulls(),
              "" + timestampStats.getNumDVs(), "", "", "", "", convertToString(timestampStats.getBitVectors())));
          if (histogramEnabled) {
            values.add(convertHistogram(statsData.getTimestampStats().getHistogram(), statsData.getSetField()));
          }
        }
      } else {
        values.addAll(Lists.newArrayList("", "", "", "", "", "", "", "", ""));
        if (histogramEnabled) {
          values.add("");
        }
      }
    }

    values.add(column.getComment() == null ? "" : column.getComment());
    return values.toArray(new String[0]);
  }

  public static String convertToString(Decimal val) {
    if (val == null) {
      return "";
    }

    HiveDecimal result = HiveDecimal.create(new BigInteger(val.getUnscaled()), val.getScale());
    return (result != null) ? result.toString() : "";
  }

  public static String convertToString(org.apache.hadoop.hive.metastore.api.Date val) {
    if (val == null) {
      return "";
    }

    DateWritableV2 writableValue = new DateWritableV2((int) val.getDaysSinceEpoch());
    return writableValue.toString();
  }

  // converts the histogram from its serialization to a string representing its quantiles
  private static String convertHistogram(byte[] buffer, ColumnStatisticsData._Fields field) {
    if (buffer == null || buffer.length == 0) {
      return "";
    }
    final KllFloatsSketch kll = KllFloatsSketch.heapify(Memory.wrap(buffer));
    // to keep the visualization compact, we print only the quartiles (Q1, Q2 and Q3),
    // as min and max are displayed as separate statistics already
    final float[] quantiles = kll.getQuantiles(new double[]{ 0.25, 0.5, 0.75 });

    Function<Float, Object> converter;

    switch(field) {
      case DATE_STATS:
        converter = f -> Date.valueOf(Timestamp.ofEpochSecond(f.longValue(), 0, getZoneIdFromConf()).toString());
        break;
      case DECIMAL_STATS:
        converter = HiveDecimal::create;
        break;
      case DOUBLE_STATS:
        converter = f -> f;
        break;
      case TIMESTAMP_STATS:
        converter = f -> Timestamp.ofEpochSecond(f.longValue(), 0, getZoneIdFromConf());
        break;
      case LONG_STATS:
        converter = Float::longValue;
        break;
      default:
        return "";
    }

    return kll.isEmpty() ? "" : "Q1: " + converter.apply(quantiles[0]) + ", Q2: "
        + converter.apply(quantiles[1]) + ", Q3: " + converter.apply(quantiles[2]);
  }

  private static ZoneId getZoneIdFromConf() {
    return SessionState.get() == null ? new HiveConf().getLocalTimeZone()
        : SessionState.get().getConf().getLocalTimeZone();
  }

  private static String convertToString(byte[] buffer) {
    if (buffer == null || buffer.length == 0) {
      return "";
    }
    return new String(Arrays.copyOfRange(buffer, 0, 2));
  }

  public static String convertToString(org.apache.hadoop.hive.metastore.api.Timestamp val) {
    if (val == null) {
      return "";
    }

    TimestampWritableV2 writableValue = new TimestampWritableV2(Timestamp.ofEpochSecond(val.getSecondsSinceEpoch()));
    return writableValue.toString();
  }

  /**
   * Convert the map to a JSON string.
   */
  public static void asJson(OutputStream out, Map<String, Object> data) throws HiveException {
    try {
      new ObjectMapper().writeValue(out, data);
    } catch (IOException e) {
      throw new HiveException("Unable to convert to json", e);
    }
  }

  public static final String FIELD_DELIM = "\t";
  public static final String LINE_DELIM = "\n";

  public static final int DEFAULT_STRINGBUILDER_SIZE = 2048;
  public static final int ALIGNMENT = 20;

  /**
   * Prints a row with the given fields into the builder.
   * The last field could be a multiline field, and the extra lines should be padded.
   *
   * @param fields The fields to print
   * @param tableInfo The target builder
   * @param isLastLinePadded Is the last field could be printed in multiple lines, if contains newlines?
   */
  public static void formatOutput(String[] fields, StringBuilder tableInfo, boolean isLastLinePadded,
      boolean isFormatted) {
    if (!isFormatted) {
      for (int i = 0; i < fields.length; i++) {
        Object value = HiveStringUtils.escapeJava(fields[i]);
        if (value != null) {
          tableInfo.append(value);
        }
        tableInfo.append((i == fields.length - 1) ? LINE_DELIM : FIELD_DELIM);
      }
    } else {
      int[] paddings = new int[fields.length - 1];
      if (fields.length > 1) {
        for (int i = 0; i < fields.length - 1; i++) {
          if (fields[i] == null) {
            tableInfo.append(FIELD_DELIM);
            continue;
          }
          tableInfo.append(String.format("%-" + ALIGNMENT + "s", fields[i])).append(FIELD_DELIM);
          paddings[i] = ALIGNMENT > fields[i].length() ? ALIGNMENT : fields[i].length();
        }
      }
      if (fields.length > 0) {
        String value = fields[fields.length - 1];
        String unescapedValue = (isLastLinePadded && value != null) ?
            value.replaceAll("\\\\n|\\\\r|\\\\r\\\\n", "\n") : value;
        indentMultilineValue(unescapedValue, tableInfo, paddings, false);
      } else {
        tableInfo.append(LINE_DELIM);
      }
    }
  }

  /**
   * Prints a row the given fields to a formatted line.
   *
   * @param fields The fields to print
   * @param tableInfo The target builder
   */
  public static void formatOutput(String[] fields, StringBuilder tableInfo) {
    formatOutput(fields, tableInfo, false, true);
  }

  /**
   * Prints the name value pair, and if the value contains newlines, it adds one more empty field
   * before the two values (Assumes, the name value pair is already indented with it).
   *
   * @param name The field name to print
   * @param value The value to print - might contain newlines
   * @param tableInfo The target builder
   */
  public static void formatOutput(String name, String value, StringBuilder tableInfo) {
    tableInfo.append(String.format("%-" + ALIGNMENT + "s", name)).append(FIELD_DELIM);
    int colNameLength = Math.max(ALIGNMENT, name.length());
    indentMultilineValue(value, tableInfo, new int[] {0, colNameLength}, true);
  }

  /**
   * Prints the name value pair
   * If the output is padded then unescape the value, so it could be printed in multiple lines.
   * In this case it assumes the pair is already indented with a field delimiter
   *
   * @param name The field name to print
   * @param value The value t print
   * @param tableInfo The target builder
   * @param isOutputPadded Should the value printed as a padded string?
   */
  public static void formatOutput(String name, String value, StringBuilder tableInfo, boolean isOutputPadded) {
    String unescapedValue = (isOutputPadded && value != null) ?
        value.replaceAll("\\\\n|\\\\r|\\\\r\\\\n", "\n") : value;
    formatOutput(name, unescapedValue, tableInfo);
  }

  /**
   * Indent processing for multi-line values.
   * Values should be indented the same amount on each line.
   * If the first line comment starts indented by k, the following line comments should also be indented by k.
   *
   * @param value the value to write
   * @param tableInfo the buffer to write to
   * @param columnWidths the widths of the previous columns
   * @param printNull print null as a string, or do not print anything
   */
  private static void indentMultilineValue(String value, StringBuilder tableInfo, int[] columnWidths,
      boolean printNull) {
    if (value == null) {
      if (printNull) {
        tableInfo.append(String.format("%-" + ALIGNMENT + "s", value));
      }
      tableInfo.append(LINE_DELIM);
    } else {
      String[] valueSegments = value.split("\n|\r|\r\n");
      tableInfo.append(String.format("%-" + ALIGNMENT + "s", valueSegments[0])).append(LINE_DELIM);
      for (int i = 1; i < valueSegments.length; i++) {
        printPadding(tableInfo, columnWidths);
        tableInfo.append(String.format("%-" + ALIGNMENT + "s", valueSegments[i])).append(LINE_DELIM);
      }
    }
  }

  /**
   * Print the right padding, with the given column widths.
   *
   * @param tableInfo The buffer to write to
   * @param columnWidths The column widths
   */
  private static void printPadding(StringBuilder tableInfo, int[] columnWidths) {
    for (int columnWidth : columnWidths) {
      if (columnWidth == 0) {
        tableInfo.append(FIELD_DELIM);
      } else {
        tableInfo.append(String.format("%" + columnWidth + "s" + FIELD_DELIM, ""));
      }
    }
  }

  /**
   * Helps to format tables in SHOW ... command outputs.
   */
  public static class TextMetaDataTable {
    private List<List<String>> table = new ArrayList<>();

    public void addRow(String ... values) {
      table.add(Arrays.asList(values));
    }

    public String renderTable(boolean isOutputPadded) {
      StringBuilder stringBuilder = new StringBuilder();
      for (List<String> row : table) {
        formatOutput(row.toArray(new String[0]), stringBuilder, isOutputPadded, isOutputPadded);
      }
      return stringBuilder.toString();
    }

    public void transpose() {
      if (table.size() == 0) {
        return;
      }
      List<List<String>> newTable = new ArrayList<>();
      for (int i = 0; i < table.get(0).size(); i++) {
        newTable.add(new ArrayList<>());
      }
      for (List<String> sourceRow : table) {
        if (newTable.size() != sourceRow.size()) {
          throw new RuntimeException("invalid table size");
        }
        for (int i = 0; i < sourceRow.size(); i++) {
          newTable.get(i).add(sourceRow.get(i));
        }
      }
      table = newTable;
    }
  }
}
