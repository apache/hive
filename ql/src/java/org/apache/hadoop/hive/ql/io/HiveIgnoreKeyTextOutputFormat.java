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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * HiveIgnoreKeyTextOutputFormat replaces key with null before feeding the &lt;key,
 * value&gt; to TextOutputFormat.RecordWriter.
 *
 */
public class HiveIgnoreKeyTextOutputFormat<K extends WritableComparable, V extends Writable>
    extends TextOutputFormat<K, V> implements HiveOutputFormat<K, V> {

  /**
   * create the final out file, and output row by row. After one row is
   * appended, a configured row separator is appended
   *
   * @param jc
   *          the job configuration file
   * @param outPath
   *          the final output file to be created
   * @param valueClass
   *          the value class used for create
   * @param isCompressed
   *          whether the content is compressed or not
   * @param tableProperties
   *          the tableProperties of this file's corresponding table
   * @param progress
   *          progress used for status report
   * @return the RecordWriter
   */
  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc, Path outPath,
      Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException {
    int rowSeparator = getRowSeparator(tableProperties);
    int headerCount = getHeaderOrFooterCount(tableProperties, serdeConstants.HEADER_COUNT);
    int footerCount = getHeaderOrFooterCount(tableProperties, serdeConstants.FOOTER_COUNT);
    if (footerCount > HiveConf.getIntVar(jc, HiveConf.ConfVars.HIVE_FILE_MAX_FOOTER)) {
      throw new IOException("footer number exceeds the limit defined in hive.file.max.footer");
    }

    final int finalRowSeparator = rowSeparator;
    FileSystem fs = outPath.getFileSystem(jc);
    final OutputStream outStream = Utilities.createCompressedStream(jc,
        fs.create(outPath, progress), isCompressed);
    writeHeaderLines(outStream, tableProperties, headerCount, finalRowSeparator);
    return new RecordWriter() {
      @Override
      public void write(Writable r) throws IOException {
        if (r instanceof Text) {
          Text tr = (Text) r;
          outStream.write(tr.getBytes(), 0, tr.getLength());
          outStream.write(finalRowSeparator);
        } else {
          // Binary SerDes always write out BytesWritable
          BytesWritable bw = (BytesWritable) r;
          outStream.write(bw.get(), 0, bw.getSize());
          outStream.write(finalRowSeparator);
        }
      }

      @Override
      public void close(boolean abort) throws IOException {
        if (!abort && footerCount > 0) {
          writeFooterLines(outStream, footerCount, finalRowSeparator);
        }
        outStream.close();
      }
    };
  }

  static int getRowSeparator(Properties tableProperties) {
    String rowSeparatorString = tableProperties.getProperty(
        serdeConstants.LINE_DELIM, "\n");
    try {
      return Byte.parseByte(rowSeparatorString);
    } catch (NumberFormatException e) {
      return rowSeparatorString.charAt(0);
    }
  }

  static int getHeaderOrFooterCount(Properties tableProperties, String propertyName) {
    return Integer.parseInt(tableProperties.getProperty(propertyName, "0"));
  }

  static void writeHeaderLines(OutputStream outStream, Properties tableProperties,
      int headerCount, int rowSeparator) throws IOException {
    if (headerCount <= 0) {
      return;
    }
    String columnHeaderLine = buildColumnNameHeaderLine(tableProperties);
    for (int i = 0; i < headerCount; i++) {
      if (i == 0 && !columnHeaderLine.isEmpty()) {
        writeLine(outStream, columnHeaderLine.getBytes(StandardCharsets.UTF_8), rowSeparator);
      } else {
        writeLine(outStream, new byte[0], rowSeparator);
      }
    }
  }

  static void writeFooterLines(OutputStream outStream, int footerCount, int rowSeparator)
      throws IOException {
    for (int i = 0; i < footerCount; i++) {
      writeLine(outStream, new byte[0], rowSeparator);
    }
  }

  static void writeLine(OutputStream outStream, byte[] lineBytes, int rowSeparator)
      throws IOException {
    if (lineBytes.length > 0) {
      outStream.write(lineBytes, 0, lineBytes.length);
    }
    outStream.write(rowSeparator);
  }

  static String buildColumnNameHeaderLine(Properties tableProperties) {
    String columnNameProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMNS, "");
    if (columnNameProperty.isEmpty()) {
      return "";
    }
    String columnNameDelimiter = tableProperties.getProperty(
        serdeConstants.COLUMN_NAME_DELIMITER, String.valueOf(SerDeUtils.COMMA));
    List<String> columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
    String fieldDelim = tableProperties.getProperty(serdeConstants.FIELD_DELIM, "\001");
    Character quoteChar = getFirstCharProperty(tableProperties, serdeConstants.QUOTE_CHAR);
    Character escapeChar = getFirstCharProperty(tableProperties, serdeConstants.ESCAPE_CHAR);
    return joinFields(columnNames, fieldDelim, quoteChar, escapeChar);
  }

  private static Character getFirstCharProperty(Properties tableProperties, String propertyName) {
    String value = tableProperties.getProperty(propertyName);
    if (value == null || value.isEmpty()) {
      return null;
    }
    return value.charAt(0);
  }

  static String joinFields(List<String> fields, String fieldDelim, Character quoteChar,
      Character escapeChar) {
    if (fields.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        sb.append(fieldDelim);
      }
      appendField(sb, fields.get(i), fieldDelim, quoteChar, escapeChar);
    }
    return sb.toString();
  }

  private static void appendField(StringBuilder sb, String field, String fieldDelim,
      Character quoteChar, Character escapeChar) {
    if (quoteChar != null && needsQuoting(field, fieldDelim, quoteChar)) {
      sb.append(quoteChar);
      for (int i = 0; i < field.length(); i++) {
        char c = field.charAt(i);
        if (c == quoteChar || (escapeChar != null && c == escapeChar)) {
          if (escapeChar != null) {
            sb.append(escapeChar);
          }
          sb.append(c);
        } else {
          sb.append(c);
        }
      }
      sb.append(quoteChar);
    } else {
      sb.append(field);
    }
  }

  private static boolean needsQuoting(String field, String fieldDelim, char quoteChar) {
    if (field.indexOf(quoteChar) >= 0) {
      return true;
    }
    if (field.indexOf('\n') >= 0 || field.indexOf('\r') >= 0) {
      return true;
    }
    return !fieldDelim.isEmpty() && field.contains(fieldDelim);
  }

  protected static class IgnoreKeyWriter<K extends WritableComparable, V extends Writable>
      implements org.apache.hadoop.mapred.RecordWriter<K, V> {

    private final org.apache.hadoop.mapred.RecordWriter<K, V> mWriter;

    public IgnoreKeyWriter(org.apache.hadoop.mapred.RecordWriter<K, V> writer) {
      this.mWriter = writer;
    }

    @Override
    public synchronized void write(K key, V value) throws IOException {
      this.mWriter.write(null, value);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      this.mWriter.close(reporter);
    }
  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<K, V> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {

    return new IgnoreKeyWriter<K, V>(super.getRecordWriter(ignored, job, name,
        progress));
  }

}
