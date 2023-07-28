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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.commons.io.EndianUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import static java.lang.String.format;

/**
 * https://cwiki.apache.org/confluence/display/Hive/TeradataBinarySerde.
 * FileOutputFormat for Teradata binary files.
 *
 * In the Teradata Binary File, each record constructs as below:
 * The first 2 bytes represents the length of the bytes next for this record (null bitmap and fields).
 * Then the null bitmap whose length is depended on the number of fields is followe.
 * Then each field of the record is serialized into bytes - the serialization strategy is decided by the type of field.
 * At last, there is one byte (0x0a) in the end of the record.
 *
 * Teradata binary files are using little endian.
 */
public class TeradataBinaryFileOutputFormat<K extends WritableComparable, V extends Writable>
    extends HiveIgnoreKeyTextOutputFormat<K, V> {
  private static final Log LOG = LogFactory.getLog(TeradataBinaryFileOutputFormat.class);

  static final byte RECORD_END_BYTE = (byte) 0x0a;

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
  @Override public RecordWriter getHiveRecordWriter(JobConf jc, Path outPath, Class<? extends Writable> valueClass,
      boolean isCompressed, Properties tableProperties, Progressable progress) throws IOException {
    FileSystem fs = outPath.getFileSystem(jc);
    final OutputStream outStream = Utilities.createCompressedStream(jc, fs.create(outPath, progress), isCompressed);
    return new RecordWriter() {
      @Override public void write(Writable r) throws IOException {
        BytesWritable bw = (BytesWritable) r;
        int recordLength = bw.getLength();

        //Based on the row length to decide if the length is int or short
        String rowLength = tableProperties
            .getProperty(TeradataBinaryRecordReader.TD_ROW_LENGTH, TeradataBinaryRecordReader.DEFAULT_TD_ROW_LENGTH)
            .toLowerCase();
        LOG.debug(format("The table property %s is: %s", TeradataBinaryRecordReader.TD_ROW_LENGTH, rowLength));

        if (TeradataBinaryRecordReader.TD_ROW_LENGTH_TO_BYTE_NUM.containsKey(rowLength)) {
          if (rowLength.equals(TeradataBinaryRecordReader.DEFAULT_TD_ROW_LENGTH)) {
            EndianUtils.writeSwappedShort(outStream, (short) recordLength); // write the length using little endian
          } else if (rowLength.equals(TeradataBinaryRecordReader.TD_ROW_LENGTH_1MB)) {
            EndianUtils.writeSwappedInteger(outStream, recordLength); // write the length using little endian
          }
        } else {
          throw new IllegalArgumentException(format("%s doesn't support the value %s, the supported values are %s",
              TeradataBinaryRecordReader.TD_ROW_LENGTH, rowLength,
              TeradataBinaryRecordReader.TD_ROW_LENGTH_TO_BYTE_NUM.keySet()));
        }

        outStream.write(bw.getBytes(), 0, bw.getLength()); // write the content (the content is in little endian)
        outStream.write(RECORD_END_BYTE); //write the record ending
      }

      @Override public void close(boolean abort) throws IOException {
        outStream.close();
      }
    };
  }
}
