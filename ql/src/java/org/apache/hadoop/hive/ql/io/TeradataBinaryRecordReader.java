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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.EndianUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import static java.lang.String.format;

/**
 * The TeradataBinaryRecordReader reads the record from Teradata binary files.
 *
 * In the Teradata Binary File, each record constructs as below:
 * The first 2 bytes represents the length of the bytes next for this record.
 * Then the null bitmap whose length is depended on the number of fields is followed.
 * Then each field of the record is serialized into bytes - the serialization strategy is decided by the type of field.
 * At last, there is one byte (0x0a) in the end of the record.
 *
 * This InputFormat currently doesn't support the split of the file.
 * Teradata binary files are using little endian.
 */
public class TeradataBinaryRecordReader implements RecordReader<NullWritable, BytesWritable> {

  private static final Log LOG = LogFactory.getLog(TeradataBinaryRecordReader.class);

  private CompressionCodecFactory compressionCodecs = null;
  private InputStream in;
  private long start;
  private long pos;
  private long end;
  private final Seekable filePosition;
  private CompressionCodec codec;

  static final String TD_ROW_LENGTH = "teradata.row.length";
  static final Map<String, Integer> TD_ROW_LENGTH_TO_BYTE_NUM = ImmutableMap.of("64kb", 2, "1mb", 4);
  static final String DEFAULT_TD_ROW_LENGTH = "64kb";
  static final String TD_ROW_LENGTH_1MB = "1mb";

  private byte[] recordLengthBytes;
  private byte[] valueByteArray = new byte[65536]; // max byte array
  private byte[] endOfRecord = new byte[1];

  private int recordLength = 0;

  public TeradataBinaryRecordReader(JobConf job, FileSplit fileSplit) throws IOException {
    LOG.debug("initialize the TeradataBinaryRecordReader");

    String rowLength = job.get(TD_ROW_LENGTH);
    if (rowLength == null) {
      LOG.debug("No table property in JobConf. Try to recover the table directly");
      Map<String, PartitionDesc> partitionDescMap = Utilities.getMapRedWork(job).getMapWork().getAliasToPartnInfo();
      for (String alias : Utilities.getMapRedWork(job).getMapWork().getAliasToPartnInfo().keySet()) {
        LOG.debug(format("the current alias: %s", alias));
        rowLength = partitionDescMap.get(alias).getTableDesc().getProperties().getProperty(TD_ROW_LENGTH);
        if (rowLength != null) {
          break;
        }
      }
    }

    if (rowLength == null) {
      rowLength = DEFAULT_TD_ROW_LENGTH;
    } else {
      rowLength = rowLength.toLowerCase();
    }

    if (TD_ROW_LENGTH_TO_BYTE_NUM.containsKey(rowLength)) {
      recordLengthBytes = new byte[TD_ROW_LENGTH_TO_BYTE_NUM.get(rowLength)];
    } else {
      throw new IllegalArgumentException(
          format("%s doesn't support the value %s, the supported values are %s", TD_ROW_LENGTH, rowLength,
              TD_ROW_LENGTH_TO_BYTE_NUM.keySet()));
    }

    start = fileSplit.getStart();
    end = start + fileSplit.getLength();

    LOG.debug(format("The start of the file split is: %s", start));
    LOG.debug(format("The end of the file split is: %s", end));

    final Path file = fileSplit.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    codec = compressionCodecs.getCodec(file);
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(fileSplit.getPath());

    /* currently the TeradataBinaryRecord file doesn't support file split at all */
    filePosition = fileIn;
    if (isCompressedInput()) {
      LOG.info(format("Input file is compressed. Using compression code %s", codec.getClass().getName()));
      in = codec.createInputStream(fileIn);
    } else {
      LOG.info("The input file is not compressed");
      in = fileIn;
    }
    pos = start;
  }

  /**
   * Reads the next key/value pair from the input for processing.
   *
   * @param key the key to read data into
   * @param value the value to read data into
   * @return true iff a key/value was read, false if at EOF
   */
  @Override public synchronized boolean next(NullWritable key, BytesWritable value) throws IOException {

    /* read the record length */
    int lengthExpected = recordLengthBytes.length;
    int hasConsumed = readExpectedBytes(recordLengthBytes, lengthExpected);
    if (hasConsumed == 0) {
      LOG.info("Reach the End of File. No more record");
      return false;
    } else if (hasConsumed < lengthExpected) {
      LOG.error(
          format("We expect %s bytes for the record length but read %d byte and reach the End of File.", lengthExpected,
              hasConsumed));
      LOG.error(format("The current position in the file : %s", getFilePosition()));
      LOG.error(format("The current consumed bytes: %s", pos));
      LOG.error(format("The bytes for the current record is: %s", Hex.encodeHexString(recordLengthBytes)));
      throw new EOFException("When reading the record length, reach the unexpected end of file.");
    }
    /* get the record contend length to prepare to read the content */
    recordLength = EndianUtils.readSwappedUnsignedShort(recordLengthBytes, 0);
    pos += lengthExpected;

    /* read the record content */
    lengthExpected = recordLength;
    hasConsumed = readExpectedBytes(valueByteArray, lengthExpected);
    if (hasConsumed < lengthExpected) {
      LOG.error(format("We expect %s bytes for the record content but read %d byte and reach the End of File.",
          lengthExpected, hasConsumed));
      LOG.error(format("The current position in the file : %s", getFilePosition()));
      LOG.error(format("The current consumed bytes: %s", pos));
      LOG.error(format("The bytes for the current record is: %s",
          Hex.encodeHexString(recordLengthBytes) + Hex.encodeHexString(valueByteArray)));
      throw new EOFException("When reading the contend of the record, reach the unexpected end of file.");
    }
    value.set(valueByteArray, 0, recordLength);
    pos += lengthExpected;

    /* read the record end */
    lengthExpected = endOfRecord.length;
    hasConsumed = readExpectedBytes(endOfRecord, lengthExpected);
    if (hasConsumed < lengthExpected) {
      LOG.error(format("We expect %s bytes for the record end symbol but read %d byte and reach the End of File.",
          lengthExpected, hasConsumed));
      LOG.error(format("The current position in the file : %s", getFilePosition()));
      LOG.error(format("The current consumed bytes: %s", pos));
      LOG.error(format("The bytes for the current record is: %s",
          Hex.encodeHexString(recordLengthBytes) + Hex.encodeHexString(valueByteArray) + Hex
              .encodeHexString(endOfRecord)));
      throw new EOFException("When reading the end of record, reach the unexpected end of file.");
    }

    if (endOfRecord[0] != TeradataBinaryFileOutputFormat.RECORD_END_BYTE) {
      throw new IOException(format("We expect 0x0a as the record end but get %s.", Hex.encodeHexString(endOfRecord)));
    }
    pos += lengthExpected;

    return true;
  }

  /**
   * Create an object of the appropriate type to be used as a key.
   *
   * @return a new key object.
   */
  @Override public NullWritable createKey() {
    return NullWritable.get();
  }

  /**
   * Create an object of the appropriate type to be used as a value.
   *
   * @return a new value object.
   */
  @Override public BytesWritable createValue() {
    return new BytesWritable();
  }

  /**
   * Returns the current position in the input.
   *
   * @return the current position in the input.
   * @throws IOException
   */
  @Override public long getPos() throws IOException {
    return pos;
  }

  /**
   *
   * @throws IOException
   */
  @Override public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }

  /**
   * How much of the input has the {@link RecordReader} consumed i.e.
   * has been processed by?
   *
   * @return progress from <code>0.0</code> to <code>1.0</code>.
   * @throws IOException
   */
  @Override public float getProgress() throws IOException {
    if (start == end) {
      return 0.0F;
    } else {
      return Math.min(1.0F, (float) (getFilePosition() - start) / (float) (end - start));
    }
  }

  private boolean isCompressedInput() {
    return codec != null;
  }

  private synchronized long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput() && filePosition != null) {
      retVal = filePosition.getPos();
    } else {
      retVal = getPos();
    }
    return retVal;
  }

  private synchronized int readExpectedBytes(byte[] toWrite, int lengthExpected) throws IOException {
    int curPos = 0;
    do {
      int numOfByteRead = in.read(toWrite, curPos, lengthExpected - curPos);
      if (numOfByteRead < 0) {
        return curPos;
      } else {
        curPos += numOfByteRead;
      }
    } while (curPos < lengthExpected);
    return curPos;
  }
}
