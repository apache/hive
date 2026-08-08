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

package org.apache.hadoop.hive.ql.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde.serdeConstants;
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
    int rowSeparator = 0;
    String rowSeparatorString = tableProperties.getProperty(
        serdeConstants.LINE_DELIM, "\n");
    try {
      rowSeparator = Byte.parseByte(rowSeparatorString);
    } catch (NumberFormatException e) {
      rowSeparator = rowSeparatorString.charAt(0);
    }

    final int finalRowSeparator = rowSeparator;
    final int headerCount = getCount(tableProperties, serdeConstants.HEADER_COUNT);
    final int footerCount = getCount(tableProperties, serdeConstants.FOOTER_COUNT);

    FileSystem fs = outPath.getFileSystem(jc);
    final OutputStream outStream = Utilities.createCompressedStream(jc,
    fs.create(outPath, progress), isCompressed);

    if (headerCount > 0) {
      outStream.write(buildHeader(tableProperties, rowSeparator, headerCount));
    }

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
          for (int i = 0; i < footerCount; i++) {
            outStream.write(finalRowSeparator);
          }
        }
        outStream.close();
      }
    };
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

  private static int getCount(Properties tableProperties, String propertyName) {
    String value = tableProperties.getProperty(propertyName, "0");
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private static char getChar(Properties tableProperties, String propertyName, char defaultValue) {
    String value = tableProperties.getProperty(propertyName);
    if (value != null && !value.isEmpty()) {
      return value.charAt(0);
    }
    return defaultValue;
  }

  private static String escapeQuotes(String field, char quote, char escape) {
    if (quote == '\0' || field.indexOf(quote) < 0) {
      return field;
    }
    StringBuilder sb = new StringBuilder(field.length() * 2);
    for (int i = 0; i < field.length(); i++) {
      char c = field.charAt(i);
      if (c == quote) {
        if (escape == quote) {
          sb.append(quote);
          sb.append(quote);
        } else {
          sb.append(escape);
          sb.append(quote);
        }
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  private static byte[] buildHeader(Properties tableProperties, int rowSeparator, int headerCount)
      throws IOException {
    char separator = getChar(tableProperties, "separatorChar",
        getChar(tableProperties, serdeConstants.FIELD_DELIM, ','));
    char quote = getChar(tableProperties, "quoteChar",
        getChar(tableProperties, serdeConstants.QUOTE_CHAR, '"'));
    char escape = getChar(tableProperties, "escapeChar",
        getChar(tableProperties, serdeConstants.ESCAPE_CHAR, '"'));

    // OpenCSVSerde quotes all data fields by default, so mirror that for the
    // header unless the table explicitly disables it. For other text SerDes
    // (e.g. LazySimpleSerDe), only quote a field when the character set requires it.
    String serializationLib = tableProperties.getProperty("serialization.lib", "");
    boolean openCsv = serializationLib.contains("OpenCSV");
    String applyQuotesToAllProp = tableProperties.getProperty("applyQuotesToAll");
    boolean applyQuotesToAll = applyQuotesToAllProp != null
        ? Boolean.parseBoolean(applyQuotesToAllProp)
        : openCsv;

    String columns = tableProperties.getProperty(serdeConstants.LIST_COLUMNS);
    ByteArrayOutputStream header = new ByteArrayOutputStream();
    if (columns == null || columns.isEmpty()) {
      // No column names are available; write empty header lines so that the
      // configured number of header lines are skipped during reads.
      for (int i = 0; i < headerCount; i++) {
        header.write((byte) rowSeparator);
      }
      return header.toByteArray();
    }

    String[] colNames = columns.split(",");
    for (int line = 0; line < headerCount; line++) {
      if (line > 0) {
        header.write((byte) rowSeparator);
      }
      for (int i = 0; i < colNames.length; i++) {
        if (i > 0) {
          header.write(String.valueOf(separator).getBytes(StandardCharsets.UTF_8));
        }
        if (quote != '\0' && (applyQuotesToAll
            || needsQuoting(colNames[i], separator, quote, (char) rowSeparator))) {
          header.write(String.valueOf(quote).getBytes(StandardCharsets.UTF_8));
          header.write(escapeQuotes(colNames[i], quote, escape).getBytes(StandardCharsets.UTF_8));
          header.write(String.valueOf(quote).getBytes(StandardCharsets.UTF_8));
        } else {
          header.write(colNames[i].getBytes(StandardCharsets.UTF_8));
        }
      }
    }
    header.write((byte) rowSeparator);
    return header.toByteArray();
  }

  private static boolean needsQuoting(String field, char separator, char quote, char rowSeparator) {
    for (int i = 0; i < field.length(); i++) {
      char c = field.charAt(i);
      if (c == separator || c == quote || c == rowSeparator) {
        return true;
      }
    }
    return false;
  }

}
