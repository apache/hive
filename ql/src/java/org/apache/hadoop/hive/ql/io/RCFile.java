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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.rmi.server.UID;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.LazyDecompressionCallback;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.*;

/**
 * <code>RCFile</code>s, short of Record Columnar File, are flat files
 * consisting of binary key/value pairs, which shares much similarity with
 * <code>SequenceFile</code>.
 *
 * RCFile stores columns of a table in a record columnar way. It first
 * partitions rows horizontally into row splits. and then it vertically
 * partitions each row split in a columnar way. RCFile first stores the meta
 * data of a row split, as the key part of a record, and all the data of a row
 * split as the value part. When writing, RCFile.Writer first holds records'
 * value bytes in memory, and determines a row split if the raw bytes size of
 * buffered records overflow a given parameter<tt>Writer.columnsBufferSize</tt>,
 * which can be set like: <code>conf.setInt(COLUMNS_BUFFER_SIZE_CONF_STR,
          4 * 1024 * 1024)</code> .
 * <p>
 * <code>RCFile</code> provides {@link Writer}, {@link Reader} and classes for
 * writing, reading respectively.
 * </p>
 *
 * <p>
 * RCFile stores columns of a table in a record columnar way. It first
 * partitions rows horizontally into row splits. and then it vertically
 * partitions each row split in a columnar way. RCFile first stores the meta
 * data of a row split, as the key part of a record, and all the data of a row
 * split as the value part.
 * </p>
 *
 * <p>
 * RCFile compresses values in a more fine-grained manner then record level
 * compression. However, It currently does not support compress the key part
 * yet. The actual compression algorithm used to compress key and/or values can
 * be specified by using the appropriate {@link CompressionCodec}.
 * </p>
 *
 * <p>
 * The {@link Reader} is used to read and explain the bytes of RCFile.
 * </p>
 *
 * <h3 id="Formats">RCFile Formats</h3>
 *
 *
 * <h4 id="Header">RC Header</h4>
 * <ul>
 * <li>version - 3 bytes of magic header <b>RCF</b>, followed by 1 byte of
 * actual version number (e.g. RCF1)</li>
 * <li>compression - A boolean which specifies if compression is turned on for
 * keys/values in this file.</li>
 * <li>compression codec - <code>CompressionCodec</code> class which is used
 * for compression of keys and/or values (if compression is enabled).</li>
 * <li>metadata - {@link Metadata} for this file.</li>
 * <li>sync - A sync marker to denote end of the header.</li>
 * </ul>
 *
 * <h4>RCFile Format</h4>
 * <ul>
 * <li><a href="#Header">Header</a></li>
 * <li>Record </li>
 * <li>Key part
 * <ul>
 * <li>Record length in bytes</li>
 * <li>Key length in bytes</li>
 * <li>Number_of_rows_in_this_record(vint)</li>
 * <li>Column_1_ondisk_length(vint)</li>
 * <li>Column_1_row_1_value_plain_length</li>
 * <li>Column_1_row_2_value_plain_length</li>
 * <li>...</li>
 * <li>Column_2_ondisk_length(vint)</li>
 * <li>Column_2_row_1_value_plain_length</li>
 * <li>Column_2_row_2_value_plain_length</li>
 * <li>...</li>
 * </ul>
 * </li>
 * <li>Value part
 * <ul>
 * <li>Compressed or plain data of [column_1_row_1_value,
 * column_1_row_2_value,....]</li>
 * <li>Compressed or plain data of [column_2_row_1_value,
 * column_2_row_2_value,....]</li>
 * </ul>
 * </li>
 * </ul>
 * <pre>
 * {@code
 * The following is a pseudo-BNF grammar for RCFile. Comments are prefixed
 * with dashes:
 *
 * rcfile ::=
 *   <file-header>
 *   <rcfile-rowgroup>+
 *
 * file-header ::=
 *   <file-version-header>
 *   <file-key-class-name>              (only exists if version is seq6)
 *   <file-value-class-name>            (only exists if version is seq6)
 *   <file-is-compressed>
 *   <file-is-block-compressed>         (only exists if version is seq6)
 *   [<file-compression-codec-class>]
 *   <file-header-metadata>
 *   <file-sync-field>
 *
 * -- The normative RCFile implementation included with Hive is actually
 * -- based on a modified version of Hadoop's SequenceFile code. Some
 * -- things which should have been modified were not, including the code
 * -- that writes out the file version header. Consequently, RCFile and
 * -- SequenceFile originally shared the same version header.  A newer
 * -- release has created a unique version string.
 *
 * file-version-header ::= Byte[4] {'S', 'E', 'Q', 6}
 *                     |   Byte[4] {'R', 'C', 'F', 1}
 *
 * -- The name of the Java class responsible for reading the key buffer
 * -- component of the rowgroup.
 *
 * file-key-class-name ::=
 *   Text {"org.apache.hadoop.hive.ql.io.RCFile$KeyBuffer"}
 *
 * -- The name of the Java class responsible for reading the value buffer
 * -- component of the rowgroup.
 *
 * file-value-class-name ::=
 *   Text {"org.apache.hadoop.hive.ql.io.RCFile$ValueBuffer"}
 *
 * -- Boolean variable indicating whether or not the file uses compression
 * -- for the key and column buffer sections.
 *
 * file-is-compressed ::= Byte[1]
 *
 * -- A boolean field indicating whether or not the file is block compressed.
 * -- This field is *always* false. According to comments in the original
 * -- RCFile implementation this field was retained for backwards
 * -- compatability with the SequenceFile format.
 *
 * file-is-block-compressed ::= Byte[1] {false}
 *
 * -- The Java class name of the compression codec iff <file-is-compressed>
 * -- is true. The named class must implement
 * -- org.apache.hadoop.io.compress.CompressionCodec.
 * -- The expected value is org.apache.hadoop.io.compress.GzipCodec.
 *
 * file-compression-codec-class ::= Text
 *
 * -- A collection of key-value pairs defining metadata values for the
 * -- file. The Map is serialized using standard JDK serialization, i.e.
 * -- an Int corresponding to the number of key-value pairs, followed by
 * -- Text key and value pairs. The following metadata properties are
 * -- mandatory for all RCFiles:
 * --
 * -- hive.io.rcfile.column.number: the number of columns in the RCFile
 *
 * file-header-metadata ::= Map<Text, Text>
 *
 * -- A 16 byte marker that is generated by the writer. This marker appears
 * -- at regular intervals at the beginning of rowgroup-headers, and is
 * -- intended to enable readers to skip over corrupted rowgroups.
 *
 * file-sync-hash ::= Byte[16]
 *
 * -- Each row group is split into three sections: a header, a set of
 * -- key buffers, and a set of column buffers. The header section includes
 * -- an optional sync hash, information about the size of the row group, and
 * -- the total number of rows in the row group. Each key buffer
 * -- consists of run-length encoding data which is used to decode
 * -- the length and offsets of individual fields in the corresponding column
 * -- buffer.
 *
 * rcfile-rowgroup ::=
 *   <rowgroup-header>
 *   <rowgroup-key-data>
 *   <rowgroup-column-buffers>
 *
 * rowgroup-header ::=
 *   [<rowgroup-sync-marker>, <rowgroup-sync-hash>]
 *   <rowgroup-record-length>
 *   <rowgroup-key-length>
 *   <rowgroup-compressed-key-length>
 *
 * -- rowgroup-key-data is compressed if the column data is compressed.
 * rowgroup-key-data ::=
 *   <rowgroup-num-rows>
 *   <rowgroup-key-buffers>
 *
 * -- An integer (always -1) signaling the beginning of a sync-hash
 * -- field.
 *
 * rowgroup-sync-marker ::= Int
 *
 * -- A 16 byte sync field. This must match the <file-sync-hash> value read
 * -- in the file header.
 *
 * rowgroup-sync-hash ::= Byte[16]
 *
 * -- The record-length is the sum of the number of bytes used to store
 * -- the key and column parts, i.e. it is the total length of the current
 * -- rowgroup.
 *
 * rowgroup-record-length ::= Int
 *
 * -- Total length in bytes of the rowgroup's key sections.
 *
 * rowgroup-key-length ::= Int
 *
 * -- Total compressed length in bytes of the rowgroup's key sections.
 *
 * rowgroup-compressed-key-length ::= Int
 *
 * -- Number of rows in the current rowgroup.
 *
 * rowgroup-num-rows ::= VInt
 *
 * -- One or more column key buffers corresponding to each column
 * -- in the RCFile.
 *
 * rowgroup-key-buffers ::= <rowgroup-key-buffer>+
 *
 * -- Data in each column buffer is stored using a run-length
 * -- encoding scheme that is intended to reduce the cost of
 * -- repeated column field values. This mechanism is described
 * -- in more detail in the following entries.
 *
 * rowgroup-key-buffer ::=
 *   <column-buffer-length>
 *   <column-buffer-uncompressed-length>
 *   <column-key-buffer-length>
 *   <column-key-buffer>
 *
 * -- The serialized length on disk of the corresponding column buffer.
 *
 * column-buffer-length ::= VInt
 *
 * -- The uncompressed length of the corresponding column buffer. This
 * -- is equivalent to column-buffer-length if the RCFile is not compressed.
 *
 * column-buffer-uncompressed-length ::= VInt
 *
 * -- The length in bytes of the current column key buffer
 *
 * column-key-buffer-length ::= VInt
 *
 * -- The column-key-buffer contains a sequence of serialized VInt values
 * -- corresponding to the byte lengths of the serialized column fields
 * -- in the corresponding rowgroup-column-buffer. For example, consider
 * -- an integer column that contains the consecutive values 1, 2, 3, 44.
 * -- The RCFile format stores these values as strings in the column buffer,
 * -- e.g. "12344". The length of each column field is recorded in
 * -- the column-key-buffer as a sequence of VInts: 1,1,1,2. However,
 * -- if the same length occurs repeatedly, then we replace repeated
 * -- run lengths with the complement (i.e. negative) of the number of
 * -- repetitions, so 1,1,1,2 becomes 1,~2,2.
 *
 * column-key-buffer ::= Byte[column-key-buffer-length]
 *
 * rowgroup-column-buffers ::= <rowgroup-value-buffer>+
 *
 * -- RCFile stores all column data as strings regardless of the
 * -- underlying column type. The strings are neither length-prefixed or
 * -- null-terminated, and decoding them into individual fields requires
 * -- the use of the run-length information contained in the corresponding
 * -- column-key-buffer.
 *
 * rowgroup-column-buffer ::= Byte[column-buffer-length]
 *
 * Byte ::= An eight-bit byte
 *
 * VInt ::= Variable length integer. The high-order bit of each byte
 * indicates whether more bytes remain to be read. The low-order seven
 * bits are appended as increasingly more significant bits in the
 * resulting integer value.
 *
 * Int ::= A four-byte integer in big-endian format.
 *
 * Text ::= VInt, Chars (Length prefixed UTF-8 characters)
 * }
 * </pre>
 */
public class RCFile {

  private static final Logger LOG = LoggerFactory.getLogger(RCFile.class);

  // internal variable
  public static final String COLUMN_NUMBER_METADATA_STR = "hive.io.rcfile.column.number";

  public static final String RECORD_INTERVAL_CONF_STR = HIVE_RCFILE_RECORD_INTERVAL.varname;

  public static final String COLUMN_NUMBER_CONF_STR = HIVE_RCFILE_COLUMN_NUMBER_CONF.varname;

  public static final String TOLERATE_CORRUPTIONS_CONF_STR = HIVE_RCFILE_TOLERATE_CORRUPTIONS.varname;

  // HACK: We actually need BlockMissingException, but that is not available
  // in all hadoop versions.
  public static final String BLOCK_MISSING_MESSAGE =
    "Could not obtain block";

  // All of the versions should be place in this list.
  private static final int ORIGINAL_VERSION = 0;  // version with SEQ
  private static final int NEW_MAGIC_VERSION = 1; // version with RCF

  private static final int CURRENT_VERSION = NEW_MAGIC_VERSION;

  // The first version of RCFile used the sequence file header.
  private static final byte[] ORIGINAL_MAGIC = new byte[] {
      (byte) 'S', (byte) 'E', (byte) 'Q'};
  // the version that was included with the original magic, which is mapped
  // into ORIGINAL_VERSION
  private static final byte ORIGINAL_MAGIC_VERSION_WITH_METADATA = 6;

  private static final byte[] ORIGINAL_MAGIC_VERSION = new byte[] {
    (byte) 'S', (byte) 'E', (byte) 'Q', ORIGINAL_MAGIC_VERSION_WITH_METADATA
  };

  // The 'magic' bytes at the beginning of the RCFile
  private static final byte[] MAGIC = new byte[] {
    (byte) 'R', (byte) 'C', (byte) 'F'};

  private static final int SYNC_ESCAPE = -1; // "length" of sync entries
  private static final int SYNC_HASH_SIZE = 16; // number of bytes in hash
  private static final int SYNC_SIZE = 4 + SYNC_HASH_SIZE; // escape + hash

  /** The number of bytes between sync points. */
  public static final int SYNC_INTERVAL = 100 * SYNC_SIZE;

  /**
   * KeyBuffer is the key of each record in RCFile. Its on-disk layout is as
   * below:
   *
   * <ul>
   * <li>record length in bytes,it is the sum of bytes used to store the key
   * part and the value part.</li>
   * <li>Key length in bytes, it is how many bytes used by the key part.</li>
   * <li>number_of_rows_in_this_record(vint),</li>
   * <li>column_1_ondisk_length(vint),</li>
   * <li>column_1_row_1_value_plain_length,</li>
   * <li>column_1_row_2_value_plain_length,</li>
   * <li>....</li>
   * <li>column_2_ondisk_length(vint),</li>
   * <li>column_2_row_1_value_plain_length,</li>
   * <li>column_2_row_2_value_plain_length,</li>
   * <li>.... .</li>
   * <li>{the end of the key part}</li>
   * </ul>
   */
  public static class KeyBuffer implements WritableComparable {
    // each column's length in the value
    private int[] eachColumnValueLen = null;
    private int[] eachColumnUncompressedValueLen = null;
    // stores each cell's length of a column in one DataOutputBuffer element
    private NonSyncDataOutputBuffer[] allCellValLenBuffer = null;
    // how many rows in this split
    private int numberRows = 0;
    // how many columns
    private int columnNumber = 0;

    // return the number of columns recorded in this file's header
    public int getColumnNumber() {
      return columnNumber;
    }

    @SuppressWarnings("unused")
    @Deprecated
    public KeyBuffer(){
    }

    KeyBuffer(int columnNum) {
      columnNumber = columnNum;
      eachColumnValueLen = new int[columnNumber];
      eachColumnUncompressedValueLen = new int[columnNumber];
      allCellValLenBuffer = new NonSyncDataOutputBuffer[columnNumber];
    }

    @SuppressWarnings("unused")
    @Deprecated
    KeyBuffer(int numberRows, int columnNum) {
      this(columnNum);
      this.numberRows = numberRows;
    }

    public void nullColumn(int columnIndex) {
      eachColumnValueLen[columnIndex] = 0;
      eachColumnUncompressedValueLen[columnIndex] = 0;
      allCellValLenBuffer[columnIndex] = new NonSyncDataOutputBuffer();
    }

    /**
     * add in a new column's meta data.
     *
     * @param columnValueLen
     *          this total bytes number of this column's values in this split
     * @param colValLenBuffer
     *          each cell's length of this column's in this split
     */
    void setColumnLenInfo(int columnValueLen,
        NonSyncDataOutputBuffer colValLenBuffer,
        int columnUncompressedValueLen, int columnIndex) {
      eachColumnValueLen[columnIndex] = columnValueLen;
      eachColumnUncompressedValueLen[columnIndex] = columnUncompressedValueLen;
      allCellValLenBuffer[columnIndex] = colValLenBuffer;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      eachColumnValueLen = new int[columnNumber];
      eachColumnUncompressedValueLen = new int[columnNumber];
      allCellValLenBuffer = new NonSyncDataOutputBuffer[columnNumber];

      numberRows = WritableUtils.readVInt(in);
      for (int i = 0; i < columnNumber; i++) {
        eachColumnValueLen[i] = WritableUtils.readVInt(in);
        eachColumnUncompressedValueLen[i] = WritableUtils.readVInt(in);
        int bufLen = WritableUtils.readVInt(in);
        if (allCellValLenBuffer[i] == null) {
          allCellValLenBuffer[i] = new NonSyncDataOutputBuffer();
        } else {
          allCellValLenBuffer[i].reset();
        }
        allCellValLenBuffer[i].write(in, bufLen);
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      // out.writeInt(numberRows);
      WritableUtils.writeVLong(out, numberRows);
      for (int i = 0; i < eachColumnValueLen.length; i++) {
        WritableUtils.writeVLong(out, eachColumnValueLen[i]);
        WritableUtils.writeVLong(out, eachColumnUncompressedValueLen[i]);
        NonSyncDataOutputBuffer colRowsLenBuf = allCellValLenBuffer[i];
        int bufLen = colRowsLenBuf.getLength();
        WritableUtils.writeVLong(out, bufLen);
        out.write(colRowsLenBuf.getData(), 0, bufLen);
      }
    }

    /**
     * get number of bytes to store the keyBuffer.
     *
     * @return number of bytes used to store this KeyBuffer on disk
     * @throws IOException
     */
    public int getSize() throws IOException {
      int ret = 0;
      ret += WritableUtils.getVIntSize(numberRows);
      for (int i = 0; i < eachColumnValueLen.length; i++) {
        ret += WritableUtils.getVIntSize(eachColumnValueLen[i]);
        ret += WritableUtils.getVIntSize(eachColumnUncompressedValueLen[i]);
        ret += WritableUtils.getVIntSize(allCellValLenBuffer[i].getLength());
        ret += allCellValLenBuffer[i].getLength();
      }

      return ret;
    }

    @Override
    public int compareTo(Object arg0) {
      throw new RuntimeException("compareTo not supported in class "
          + this.getClass().getName());
    }

    public int[] getEachColumnUncompressedValueLen() {
      return eachColumnUncompressedValueLen;
    }

    public int[] getEachColumnValueLen() {
      return eachColumnValueLen;
    }

    /**
     * @return the numberRows
     */
    public int getNumberRows() {
      return numberRows;
    }
  }

  /**
   * ValueBuffer is the value of each record in RCFile. Its on-disk layout is as
   * below:
   * <ul>
   * <li>Compressed or plain data of [column_1_row_1_value,
   * column_1_row_2_value,....]</li>
   * <li>Compressed or plain data of [column_2_row_1_value,
   * column_2_row_2_value,....]</li>
   * </ul>
   */
  public static class ValueBuffer implements WritableComparable {

    class LazyDecompressionCallbackImpl implements LazyDecompressionCallback {

      int index = -1;
      int colIndex = -1;

      public LazyDecompressionCallbackImpl(int index, int colIndex) {
        super();
        this.index = index;
        this.colIndex = colIndex;
      }

      @Override
      public byte[] decompress() throws IOException {

        if (decompressedFlag[index] || codec == null) {
          return loadedColumnsValueBuffer[index].getData();
        }

        NonSyncDataOutputBuffer compressedData = compressedColumnsValueBuffer[index];
        decompressBuffer.reset();
        DataInputStream valueIn = new DataInputStream(deflatFilter);
        deflatFilter.resetState();
        if (deflatFilter instanceof SchemaAwareCompressionInputStream) {
          ((SchemaAwareCompressionInputStream)deflatFilter).setColumnIndex(colIndex);
        }
        decompressBuffer.reset(compressedData.getData(),
            keyBuffer.eachColumnValueLen[colIndex]);

        NonSyncDataOutputBuffer decompressedColBuf = loadedColumnsValueBuffer[index];
        decompressedColBuf.reset();
        decompressedColBuf.write(valueIn,
            keyBuffer.eachColumnUncompressedValueLen[colIndex]);
        decompressedFlag[index] = true;
        numCompressed--;
        return decompressedColBuf.getData();
      }
    }

    // used to load columns' value into memory
    private NonSyncDataOutputBuffer[] loadedColumnsValueBuffer = null;
    private NonSyncDataOutputBuffer[] compressedColumnsValueBuffer = null;
    private boolean[] decompressedFlag = null;
    private int numCompressed;
    private LazyDecompressionCallbackImpl[] lazyDecompressCallbackObjs = null;
    private boolean lazyDecompress = true;

    boolean inited = false;

    // used for readFields
    KeyBuffer keyBuffer;
    private int columnNumber = 0;

    // set true for columns that needed to skip loading into memory.
    boolean[] skippedColIDs = null;

    CompressionCodec codec;

    Decompressor valDecompressor = null;
    NonSyncDataInputBuffer decompressBuffer = new NonSyncDataInputBuffer();
    CompressionInputStream deflatFilter = null;

    @SuppressWarnings("unused")
    @Deprecated
    public ValueBuffer() throws IOException {
    }

    @SuppressWarnings("unused")
    @Deprecated
    public ValueBuffer(KeyBuffer keyBuffer) throws IOException {
      this(keyBuffer, keyBuffer.columnNumber, null, null, true);
    }

    @SuppressWarnings("unused")
    @Deprecated
    public ValueBuffer(KeyBuffer keyBuffer, boolean[] skippedColIDs)
        throws IOException {
      this(keyBuffer, keyBuffer.columnNumber, skippedColIDs, null, true);
    }

    @SuppressWarnings("unused")
    @Deprecated
    public ValueBuffer(KeyBuffer currentKey, int columnNumber,
        boolean[] skippedCols, CompressionCodec codec) throws IOException {
      this(currentKey, columnNumber, skippedCols, codec, true);
    }

    public ValueBuffer(KeyBuffer currentKey, int columnNumber,
      boolean[] skippedCols, CompressionCodec codec, boolean lazyDecompress)
        throws IOException {
      this.lazyDecompress = lazyDecompress;
      keyBuffer = currentKey;
      this.columnNumber = columnNumber;

      if (skippedCols != null && skippedCols.length > 0) {
        skippedColIDs = skippedCols;
      } else {
        skippedColIDs = new boolean[columnNumber];
        for (int i = 0; i < skippedColIDs.length; i++) {
          skippedColIDs[i] = false;
        }
      }

      int skipped = 0;
      for (boolean currentSkip : skippedColIDs) {
        if (currentSkip) {
          skipped++;
        }
      }
      loadedColumnsValueBuffer = new NonSyncDataOutputBuffer[columnNumber
          - skipped];
      decompressedFlag = new boolean[columnNumber - skipped];
      lazyDecompressCallbackObjs = new LazyDecompressionCallbackImpl[columnNumber
          - skipped];
      compressedColumnsValueBuffer = new NonSyncDataOutputBuffer[columnNumber
                                                                 - skipped];
      this.codec = codec;
      if (codec != null) {
        valDecompressor = CodecPool.getDecompressor(codec);
        deflatFilter = codec.createInputStream(decompressBuffer,
            valDecompressor);
      }
      if (codec != null) {
        numCompressed = decompressedFlag.length;
      } else {
        numCompressed = 0;
      }
      for (int k = 0, readIndex = 0; k < columnNumber; k++) {
        if (skippedColIDs[k]) {
          continue;
        }
        loadedColumnsValueBuffer[readIndex] = new NonSyncDataOutputBuffer();
        if (codec != null) {
          decompressedFlag[readIndex] = false;
          lazyDecompressCallbackObjs[readIndex] = new LazyDecompressionCallbackImpl(
              readIndex, k);
          compressedColumnsValueBuffer[readIndex] = new NonSyncDataOutputBuffer();
        } else {
          decompressedFlag[readIndex] = true;
        }
        readIndex++;
      }
    }

    @SuppressWarnings("unused")
    @Deprecated
    public void setColumnValueBuffer(NonSyncDataOutputBuffer valBuffer,
        int addIndex) {
      loadedColumnsValueBuffer[addIndex] = valBuffer;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int addIndex = 0;
      int skipTotal = 0;
      for (int i = 0; i < columnNumber; i++) {
        int vaRowsLen = keyBuffer.eachColumnValueLen[i];
        // skip this column
        if (skippedColIDs[i]) {
          skipTotal += vaRowsLen;
          continue;
        }

        if (skipTotal != 0) {
          in.skipBytes(skipTotal);
          skipTotal = 0;
        }

        NonSyncDataOutputBuffer valBuf;
        if (codec != null){
           // load into compressed buf first
          valBuf = compressedColumnsValueBuffer[addIndex];
        } else {
          valBuf = loadedColumnsValueBuffer[addIndex];
        }
        valBuf.reset();
        valBuf.write(in, vaRowsLen);
        if (codec != null) {
          decompressedFlag[addIndex] = false;
          if (!lazyDecompress) {
            lazyDecompressCallbackObjs[addIndex].decompress();
            decompressedFlag[addIndex] = true;
          }
        }
        addIndex++;
      }
      if (codec != null) {
        numCompressed = decompressedFlag.length;
      }

      if (skipTotal != 0) {
        in.skipBytes(skipTotal);
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      if (codec != null) {
        for (NonSyncDataOutputBuffer currentBuf : compressedColumnsValueBuffer) {
          out.write(currentBuf.getData(), 0, currentBuf.getLength());
        }
      } else {
        for (NonSyncDataOutputBuffer currentBuf : loadedColumnsValueBuffer) {
          out.write(currentBuf.getData(), 0, currentBuf.getLength());
        }
      }
    }

    public void nullColumn(int columnIndex) {
      if (codec != null) {
        compressedColumnsValueBuffer[columnIndex].reset();
      } else {
        loadedColumnsValueBuffer[columnIndex].reset();
      }
    }

    public void clearColumnBuffer() throws IOException {
      decompressBuffer.reset();
    }

    public void close() {
      for (NonSyncDataOutputBuffer element : loadedColumnsValueBuffer) {
        IOUtils.closeStream(element);
      }
      if (codec != null) {
        IOUtils.closeStream(decompressBuffer);
        if (valDecompressor != null) {
          // Make sure we only return valDecompressor once.
          CodecPool.returnDecompressor(valDecompressor);
          valDecompressor = null;
        }
      }
    }

    @Override
    public int compareTo(Object arg0) {
      throw new RuntimeException("compareTo not supported in class "
          + this.getClass().getName());
    }
  }

  /**
   * Create a metadata object with alternating key-value pairs.
   * Eg. metadata(key1, value1, key2, value2)
   */
  public static Metadata createMetadata(Text... values) {
    if (values.length % 2 != 0) {
      throw new IllegalArgumentException("Must have a matched set of " +
                                         "key-value pairs. " + values.length+
                                         " strings supplied.");
    }
    Metadata result = new Metadata();
    for(int i=0; i < values.length; i += 2) {
      result.set(values[i], values[i+1]);
    }
    return result;
  }

  /**
   * Write KeyBuffer/ValueBuffer pairs to a RCFile. RCFile's format is
   * compatible with SequenceFile's.
   *
   */
  public static class Writer {

    Configuration conf;
    FSDataOutputStream out;

    CompressionCodec codec = null;
    Metadata metadata = null;

    // Insert a globally unique 16-byte value every few entries, so that one
    // can seek into the middle of a file and then synchronize with record
    // starts and ends by scanning for this value.
    long lastSyncPos; // position of last sync
    byte[] sync; // 16 random bytes
    {
      try {
        MessageDigest digester = MessageDigest.getInstance("MD5");
        long time = System.currentTimeMillis();
        digester.update((new UID() + "@" + time).getBytes());
        sync = digester.digest();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    // how many records the writer buffers before it writes to disk
    private int RECORD_INTERVAL = Integer.MAX_VALUE;
    // the max size of memory for buffering records before writes them out
    private int columnsBufferSize = 4 * 1024 * 1024; // 4M
    // the conf string for COLUMNS_BUFFER_SIZE
    public static final String COLUMNS_BUFFER_SIZE_CONF_STR = "hive.io.rcfile.record.buffer.size";

    // how many records already buffered
    private int bufferedRecords = 0;

    private final ColumnBuffer[] columnBuffers;

    private int columnNumber = 0;

    private final int[] columnValuePlainLength;

    KeyBuffer key = null;
    private final int[] plainTotalColumnLength;
    private final int[] comprTotalColumnLength;

    boolean useNewMagic = true;

    /*
     * used for buffering appends before flush them out
     */
    class ColumnBuffer {
      // used for buffer a column's values
      NonSyncDataOutputBuffer columnValBuffer;
      // used to store each value's length
      NonSyncDataOutputBuffer valLenBuffer;

      /*
       * use a run-length encoding. We only record run length if a same
       * 'prevValueLen' occurs more than one time. And we negative the run
       * length to distinguish a runLength and a normal value length. For
       * example, if the values' lengths are 1,1,1,2, we record 1, ~2,2. And for
       * value lengths 1,2,3 we record 1,2,3.
       */
      int runLength = 0;
      int prevValueLength = -1;

      ColumnBuffer() throws IOException {
        columnValBuffer = new NonSyncDataOutputBuffer();
        valLenBuffer = new NonSyncDataOutputBuffer();
      }

      public void append(BytesRefWritable data) throws IOException {
        data.writeDataTo(columnValBuffer);
        int currentLen = data.getLength();

        if (prevValueLength < 0) {
          startNewGroup(currentLen);
          return;
        }

        if (currentLen != prevValueLength) {
          flushGroup();
          startNewGroup(currentLen);
        } else {
          runLength++;
        }
      }

      private void startNewGroup(int currentLen) {
        prevValueLength = currentLen;
        runLength = 0;
      }

      public void clear() throws IOException {
        valLenBuffer.reset();
        columnValBuffer.reset();
        prevValueLength = -1;
        runLength = 0;
      }

      public void flushGroup() throws IOException {
        if (prevValueLength >= 0) {
          WritableUtils.writeVLong(valLenBuffer, prevValueLength);
          if (runLength > 0) {
            WritableUtils.writeVLong(valLenBuffer, ~runLength);
          }
          runLength = -1;
          prevValueLength = -1;
        }
      }
    }

    public long getLength() throws IOException {
      return out.getPos();
    }

    /** Constructs a RCFile Writer. */
    public Writer(FileSystem fs, Configuration conf, Path name) throws IOException {
      this(fs, conf, name, null, new Metadata(), null);
    }

    /**
     * Constructs a RCFile Writer.
     *
     * @param fs
     *          the file system used
     * @param conf
     *          the configuration file
     * @param name
     *          the file name
     * @throws IOException
     */
    public Writer(FileSystem fs, Configuration conf, Path name,
        Progressable progress, CompressionCodec codec) throws IOException {
      this(fs, conf, name, progress, new Metadata(), codec);
    }

    /**
     * Constructs a RCFile Writer.
     *
     * @param fs
     *          the file system used
     * @param conf
     *          the configuration file
     * @param name
     *          the file name
     * @param progress a progress meter to update as the file is written
     * @param metadata a string to string map in the file header
     * @throws IOException
     */
    public Writer(FileSystem fs, Configuration conf, Path name,
        Progressable progress, Metadata metadata, CompressionCodec codec) throws IOException {
      this(fs, conf, name, fs.getConf().getInt("io.file.buffer.size", 4096),
              ShimLoader.getHadoopShims().getDefaultReplication(fs, name),
              ShimLoader.getHadoopShims().getDefaultBlockSize(fs, name), progress,
          metadata, codec);
    }

    /**
     *
     * Constructs a RCFile Writer.
     *
     * @param fs
     *          the file system used
     * @param conf
     *          the configuration file
     * @param name
     *          the file name
     * @param bufferSize the size of the file buffer
     * @param replication the number of replicas for the file
     * @param blockSize the block size of the file
     * @param progress the progress meter for writing the file
     * @param metadata a string to string map in the file header
     * @throws IOException
     */
    public Writer(FileSystem fs, Configuration conf, Path name, int bufferSize,
        short replication, long blockSize, Progressable progress,
        Metadata metadata, CompressionCodec codec) throws IOException {
      RECORD_INTERVAL = HiveConf.getIntVar(conf, HIVE_RCFILE_RECORD_INTERVAL);
      columnNumber = HiveConf.getIntVar(conf, HIVE_RCFILE_COLUMN_NUMBER_CONF);

      if (metadata == null) {
        metadata = new Metadata();
      }
      metadata.set(new Text(COLUMN_NUMBER_METADATA_STR), new Text(""
          + columnNumber));

      columnsBufferSize = conf.getInt(COLUMNS_BUFFER_SIZE_CONF_STR,
          4 * 1024 * 1024);

      columnValuePlainLength = new int[columnNumber];

      columnBuffers = new ColumnBuffer[columnNumber];
      for (int i = 0; i < columnNumber; i++) {
        columnBuffers[i] = new ColumnBuffer();
      }

      init(conf, fs.create(name, true, bufferSize, replication,
        blockSize, progress), codec, metadata);
      initializeFileHeader();
      writeFileHeader();
      finalizeFileHeader();
      key = new KeyBuffer(columnNumber);

      plainTotalColumnLength = new int[columnNumber];
      comprTotalColumnLength = new int[columnNumber];
    }

    /** Write the initial part of file header. */
    void initializeFileHeader() throws IOException {
      if (useNewMagic) {
        out.write(MAGIC);
        out.write(CURRENT_VERSION);
      } else {
        out.write(ORIGINAL_MAGIC_VERSION);
      }
    }

    /** Write the final part of file header. */
    void finalizeFileHeader() throws IOException {
      out.write(sync); // write the sync bytes
      out.flush(); // flush header
    }

    boolean isCompressed() {
      return codec != null;
    }

    /** Write and flush the file header. */
    void writeFileHeader() throws IOException {
      if (useNewMagic) {
        out.writeBoolean(isCompressed());
      } else {
        Text.writeString(out, KeyBuffer.class.getName());
        Text.writeString(out, ValueBuffer.class.getName());
        out.writeBoolean(isCompressed());
        out.writeBoolean(false);
      }

      if (isCompressed()) {
        Text.writeString(out, (codec.getClass()).getName());
      }
      metadata.write(out);
    }

    void init(Configuration conf, FSDataOutputStream out,
        CompressionCodec codec, Metadata metadata) throws IOException {
      this.conf = conf;
      this.out = out;
      this.codec = codec;
      this.metadata = metadata;
      this.useNewMagic = conf.getBoolean(HIVE_USE_EXPLICIT_RCFILE_HEADER.varname, true);
    }

    /** Returns the compression codec of data in this file. */
    @SuppressWarnings("unused")
    @Deprecated
    public CompressionCodec getCompressionCodec() {
      return codec;
    }

    /** create a sync point. */
    public void sync() throws IOException {
      if (sync != null && lastSyncPos != out.getPos()) {
        out.writeInt(SYNC_ESCAPE); // mark the start of the sync
        out.write(sync); // write sync
        lastSyncPos = out.getPos(); // update lastSyncPos
      }
    }

    /** Returns the configuration of this file. */
    @SuppressWarnings("unused")
    @Deprecated
    Configuration getConf() {
      return conf;
    }

    private void checkAndWriteSync() throws IOException {
      if (sync != null && out.getPos() >= lastSyncPos + SYNC_INTERVAL) {
        sync();
      }
    }

    private int columnBufferSize = 0;

    /**
     * Append a row of values. Currently it only can accept &lt;
     * {@link BytesRefArrayWritable}. If its <code>size()</code> is less than the
     * column number in the file, zero bytes are appended for the empty columns.
     * If its size() is greater then the column number in the file, the exceeded
     * columns' bytes are ignored.
     *
     * @param val a BytesRefArrayWritable with the list of serialized columns
     * @throws IOException
     */
    public void append(Writable val) throws IOException {

      if (!(val instanceof BytesRefArrayWritable)) {
        throw new UnsupportedOperationException(
            "Currently the writer can only accept BytesRefArrayWritable");
      }

      BytesRefArrayWritable columns = (BytesRefArrayWritable) val;
      int size = columns.size();
      for (int i = 0; i < size; i++) {
        BytesRefWritable cu = columns.get(i);
        int plainLen = cu.getLength();
        columnBufferSize += plainLen;
        columnValuePlainLength[i] += plainLen;
        columnBuffers[i].append(cu);
      }

      if (size < columnNumber) {
        for (int i = columns.size(); i < columnNumber; i++) {
          columnBuffers[i].append(BytesRefWritable.ZeroBytesRefWritable);
        }
      }

      bufferedRecords++;
      if ((columnBufferSize > columnsBufferSize)
          || (bufferedRecords >= RECORD_INTERVAL)) {
        flushRecords();
      }
    }

    private void flushRecords() throws IOException {

      key.numberRows = bufferedRecords;

      Compressor compressor = null;
      NonSyncDataOutputBuffer valueBuffer = null;
      CompressionOutputStream deflateFilter = null;
      DataOutputStream deflateOut = null;
      boolean isCompressed = isCompressed();
      int valueLength = 0;
      if (isCompressed) {
        ReflectionUtils.setConf(codec, this.conf);
        compressor = CodecPool.getCompressor(codec);
        valueBuffer = new NonSyncDataOutputBuffer();
        deflateFilter = codec.createOutputStream(valueBuffer, compressor);
        deflateOut = new DataOutputStream(deflateFilter);
      }

      for (int columnIndex = 0; columnIndex < columnNumber; columnIndex++) {
        ColumnBuffer currentBuf = columnBuffers[columnIndex];
        currentBuf.flushGroup();

        NonSyncDataOutputBuffer columnValue = currentBuf.columnValBuffer;
        int colLen;
        int plainLen = columnValuePlainLength[columnIndex];

        if (isCompressed) {
          if (deflateFilter instanceof SchemaAwareCompressionOutputStream) {
            ((SchemaAwareCompressionOutputStream)deflateFilter).
              setColumnIndex(columnIndex);
          }
          deflateFilter.resetState();
          deflateOut.write(columnValue.getData(), 0, columnValue.getLength());
          deflateOut.flush();
          deflateFilter.finish();
          // find how much compressed data was added for this column
          colLen = valueBuffer.getLength() - valueLength;
        } else {
          colLen = columnValuePlainLength[columnIndex];
        }
        valueLength += colLen;
        key.setColumnLenInfo(colLen, currentBuf.valLenBuffer, plainLen,
          columnIndex);
        plainTotalColumnLength[columnIndex] += plainLen;
        comprTotalColumnLength[columnIndex] += colLen;
        columnValuePlainLength[columnIndex] = 0;
      }

      int keyLength = key.getSize();
      if (keyLength < 0) {
        throw new IOException("negative length keys not allowed: " + key);
      }
      if (compressor != null) {
        CodecPool.returnCompressor(compressor);
      }

      // Write the key out
      writeKey(key, keyLength + valueLength, keyLength);
      // write the value out
      if (isCompressed) {
        out.write(valueBuffer.getData(), 0, valueBuffer.getLength());
      } else {
        for(int columnIndex=0; columnIndex < columnNumber; ++columnIndex) {
          NonSyncDataOutputBuffer buf =
            columnBuffers[columnIndex].columnValBuffer;
          out.write(buf.getData(), 0, buf.getLength());
        }
      }

      // clear the columnBuffers
      clearColumnBuffers();

      bufferedRecords = 0;
      columnBufferSize = 0;
    }

    /**
     * flush a block out without doing anything except compressing the key part.
     */
    public void flushBlock(KeyBuffer keyBuffer, ValueBuffer valueBuffer,
        int recordLen, int keyLength,
        @SuppressWarnings("unused") int compressedKeyLen) throws IOException {
      writeKey(keyBuffer, recordLen, keyLength);
      valueBuffer.write(out);
    }

    private void writeKey(KeyBuffer keyBuffer, int recordLen,
                          int keyLength) throws IOException {
      checkAndWriteSync(); // sync
      out.writeInt(recordLen); // total record length
      out.writeInt(keyLength); // key portion length

      if(this.isCompressed()) {
        Compressor compressor = CodecPool.getCompressor(codec);
        NonSyncDataOutputBuffer compressionBuffer =
          new NonSyncDataOutputBuffer();
        CompressionOutputStream deflateFilter =
          codec.createOutputStream(compressionBuffer, compressor);
        DataOutputStream deflateOut = new DataOutputStream(deflateFilter);
        //compress key and write key out
        compressionBuffer.reset();
        deflateFilter.resetState();
        keyBuffer.write(deflateOut);
        deflateOut.flush();
        deflateFilter.finish();
        int compressedKeyLen = compressionBuffer.getLength();
        out.writeInt(compressedKeyLen);
        out.write(compressionBuffer.getData(), 0, compressedKeyLen);
        CodecPool.returnCompressor(compressor);
      } else {
        out.writeInt(keyLength);
        keyBuffer.write(out);
      }
    }

    private void clearColumnBuffers() throws IOException {
      for (int i = 0; i < columnNumber; i++) {
        columnBuffers[i].clear();
      }
    }

    public synchronized void close() throws IOException {
      if (bufferedRecords > 0) {
        flushRecords();
      }
      clearColumnBuffers();

      if (out != null) {

        // Close the underlying stream if we own it...
        out.flush();
        out.close();
        out = null;
      }
      for (int i = 0; i < columnNumber; i++) {
        LOG.info("Column#" + i + " : Plain Total Column Value Length: "
          + plainTotalColumnLength[i]
          + ",  Compr Total Column Value Length: " + comprTotalColumnLength[i]);
      }
    }
  }

  /**
   * Read KeyBuffer/ValueBuffer pairs from a RCFile.
   *
   */
  public static class Reader {
    private static class SelectedColumn {
      public int colIndex;
      public int rowReadIndex;
      public int runLength;
      public int prvLength;
      public boolean isNulled;
    }
    private final Path file;
    private final FSDataInputStream in;

    private byte version;

    private CompressionCodec codec = null;
    private Metadata metadata = null;

    private final byte[] sync = new byte[SYNC_HASH_SIZE];
    private final byte[] syncCheck = new byte[SYNC_HASH_SIZE];
    private boolean syncSeen;
    private long lastSeenSyncPos = 0;

    private long headerEnd;
    private final long end;
    private int currentKeyLength;
    private int currentRecordLength;

    private final Configuration conf;

    private final ValueBuffer currentValue;

    private int readRowsIndexInBuffer = 0;

    private int recordsNumInValBuffer = 0;

    private int columnNumber = 0;

    private int loadColumnNum;

    private int passedRowsNum = 0;

    // Should we try to tolerate corruption? Default is No.
    private boolean tolerateCorruptions = false;

    private boolean decompress = false;

    private Decompressor keyDecompressor;
    NonSyncDataOutputBuffer keyDecompressedData = new NonSyncDataOutputBuffer();

    //Current state of each selected column - e.g. current run length, etc.
    // The size of the array is equal to the number of selected columns
    private final SelectedColumn[] selectedColumns;

    // map of original column id -> index among selected columns
    private final int[] revPrjColIDs;

    // column value lengths for each of the selected columns
    private final NonSyncDataInputBuffer[] colValLenBufferReadIn;

    /** Create a new RCFile reader. */
    public Reader(FileSystem fs, Path file, Configuration conf) throws IOException {
      this(fs, file, conf.getInt("io.file.buffer.size", 4096), conf, 0, fs
          .getFileStatus(file).getLen());
    }

    /** Create a new RCFile reader. */
    public Reader(FileSystem fs, Path file, int bufferSize, Configuration conf,
        long start, long length) throws IOException {
      tolerateCorruptions = HiveConf.getBoolVar(conf, HIVE_RCFILE_TOLERATE_CORRUPTIONS);
      conf.setInt("io.file.buffer.size", bufferSize);
      this.file = file;
      in = openFile(fs, file, bufferSize, length);
      this.conf = conf;
      end = start + length;
      boolean succeed = false;
      try {
        if (start > 0) {
          seek(0);
          init();
          seek(start);
        } else {
          init();
        }
        succeed = true;
      } finally {
        if (!succeed) {
          if (in != null) {
            try {
              in.close();
            } catch(IOException e) {
              if (LOG != null && LOG.isDebugEnabled()) {
                LOG.debug("Exception in closing " + in, e);
              }
            }
          }
        }
      }

      columnNumber = Integer.parseInt(metadata.get(
          new Text(COLUMN_NUMBER_METADATA_STR)).toString());

      List<Integer> notSkipIDs = ColumnProjectionUtils
          .getReadColumnIDs(conf);
      boolean[] skippedColIDs = new boolean[columnNumber];
      if(ColumnProjectionUtils.isReadAllColumns(conf)) {
        Arrays.fill(skippedColIDs, false);
      } else if (notSkipIDs.size() > 0) {
        Arrays.fill(skippedColIDs, true);
        for (int read : notSkipIDs) {
          if (read < columnNumber) {
            skippedColIDs[read] = false;
          }
        }
      } else {
        // select count(1)
        Arrays.fill(skippedColIDs, true);
      }
      loadColumnNum = columnNumber;
      if (skippedColIDs.length > 0) {
        for (boolean skippedColID : skippedColIDs) {
          if (skippedColID) {
            loadColumnNum -= 1;
          }
        }
      }


      revPrjColIDs = new int[columnNumber];
      // get list of selected column IDs
      selectedColumns = new SelectedColumn[loadColumnNum];
      colValLenBufferReadIn = new NonSyncDataInputBuffer[loadColumnNum];
      for (int i = 0, j = 0; i < columnNumber; ++i) {
        if (!skippedColIDs[i]) {
          SelectedColumn col = new SelectedColumn();
          col.colIndex = i;
          col.runLength = 0;
          col.prvLength = -1;
          col.rowReadIndex = 0;
          selectedColumns[j] = col;
          colValLenBufferReadIn[j] = new NonSyncDataInputBuffer();
          revPrjColIDs[i] = j;
          j++;
        } else {
          revPrjColIDs[i] = -1;
        }
      }

      currentKey = createKeyBuffer();
      boolean lazyDecompress = !tolerateCorruptions;
      currentValue = new ValueBuffer(
        null, columnNumber, skippedColIDs, codec, lazyDecompress);
    }

    /**
     * Return the metadata (Text to Text map) that was written into the
     * file.
     */
    public Metadata getMetadata() {
      return metadata;
    }

    /**
     * Return the metadata value associated with the given key.
     * @param key the metadata key to retrieve
     */
    public Text getMetadataValueOf(Text key) {
      return metadata.get(key);
    }

    /**
     * Override this method to specialize the type of
     * {@link FSDataInputStream} returned.
     */
    protected FSDataInputStream openFile(FileSystem fs, Path file,
        int bufferSize, long length) throws IOException {
      return fs.open(file, bufferSize);
    }

    private void init() throws IOException {
      byte[] magic = new byte[MAGIC.length];
      in.readFully(magic);

      if (Arrays.equals(magic, ORIGINAL_MAGIC)) {
        byte vers = in.readByte();
        if (vers != ORIGINAL_MAGIC_VERSION_WITH_METADATA) {
          throw new IOException(file + " is a version " + vers +
                                " SequenceFile instead of an RCFile.");
        }
        version = ORIGINAL_VERSION;
      } else {
        if (!Arrays.equals(magic, MAGIC)) {
          throw new IOException(file + " not a RCFile and has magic of " +
                                new String(magic));
        }

        // Set 'version'
        version = in.readByte();
        if (version > CURRENT_VERSION) {
          throw new VersionMismatchException((byte) CURRENT_VERSION, version);
        }
      }

      if (version == ORIGINAL_VERSION) {
        try {
          Class<?> keyCls = conf.getClassByName(Text.readString(in));
          Class<?> valCls = conf.getClassByName(Text.readString(in));
          if (!keyCls.equals(KeyBuffer.class)
              || !valCls.equals(ValueBuffer.class)) {
            throw new IOException(file + " not a RCFile");
          }
        } catch (ClassNotFoundException e) {
          throw new IOException(file + " not a RCFile", e);
        }
      }

      decompress = in.readBoolean(); // is compressed?

      if (version == ORIGINAL_VERSION) {
        // is block-compressed? it should be always false.
        boolean blkCompressed = in.readBoolean();
        if (blkCompressed) {
          throw new IOException(file + " not a RCFile.");
        }
      }

      // setup the compression codec
      if (decompress) {
        String codecClassname = Text.readString(in);
        try {
          Class<? extends CompressionCodec> codecClass = conf.getClassByName(
              codecClassname).asSubclass(CompressionCodec.class);
          codec = ReflectionUtils.newInstance(codecClass, conf);
        } catch (ClassNotFoundException cnfe) {
          throw new IllegalArgumentException(
              "Unknown codec: " + codecClassname, cnfe);
        }
        keyDecompressor = CodecPool.getDecompressor(codec);
      }

      metadata = new Metadata();
      metadata.readFields(in);

      in.readFully(sync); // read sync bytes
      headerEnd = in.getPos();
    }

    /** Return the current byte position in the input file. */
    public synchronized long getPosition() throws IOException {
      return in.getPos();
    }

    /**
     * Set the current byte position in the input file.
     *
     * <p>
     * The position passed must be a position returned by
     * {@link RCFile.Writer#getLength()} when writing this file. To seek to an
     * arbitrary position, use {@link RCFile.Reader#sync(long)}. In another
     * words, the current seek can only seek to the end of the file. For other
     * positions, use {@link RCFile.Reader#sync(long)}.
     */
    public synchronized void seek(long position) throws IOException {
      in.seek(position);
    }

    /**
     * Resets the values which determine if there are more rows in the buffer
     *
     * This can be used after one calls seek or sync, if one called next before that.
     * Otherwise, the seek or sync will have no effect, it will continue to get rows from the
     * buffer built up from the call to next.
     */
    public synchronized void resetBuffer() {
      readRowsIndexInBuffer = 0;
      recordsNumInValBuffer = 0;
    }

    /** Seek to the next sync mark past a given position. */
    public synchronized void sync(long position) throws IOException {
      if (position + SYNC_SIZE >= end) {
        seek(end);
        return;
      }

      //this is to handle syn(pos) where pos < headerEnd.
      if (position < headerEnd) {
        // seek directly to first record
        in.seek(headerEnd);
        // note the sync marker "seen" in the header
        syncSeen = true;
        return;
      }

      try {
        seek(position + 4); // skip escape

        int prefix = sync.length;
        int n = conf.getInt("io.bytes.per.checksum", 512);
        byte[] buffer = new byte[prefix+n];
        n = (int)Math.min(n, end - in.getPos());
        /* fill array with a pattern that will never match sync */
        Arrays.fill(buffer, (byte)(~sync[0]));
        while(n > 0 && (in.getPos() + n) <= end) {
          position = in.getPos();
          in.readFully(buffer, prefix, n);
          /* the buffer has n+sync bytes */
          for(int i = 0; i < n; i++) {
            int j;
            for(j = 0; j < sync.length && sync[j] == buffer[i+j]; j++) {
              /* nothing */
            }
            if(j == sync.length) {
              /* simplified from (position + (i - prefix) + sync.length) - SYNC_SIZE */
              in.seek(position + i - SYNC_SIZE);
              return;
            }
          }
          /* move the last 16 bytes to the prefix area */
          System.arraycopy(buffer, buffer.length - prefix, buffer, 0, prefix);
          n = (int)Math.min(n, end - in.getPos());
        }
      } catch (ChecksumException e) { // checksum failure
        handleChecksumException(e);
      }
    }

    private void handleChecksumException(ChecksumException e) throws IOException {
      if (conf.getBoolean("io.skip.checksum.errors", false)) {
        LOG.warn("Bad checksum at " + getPosition() + ". Skipping entries.");
        sync(getPosition() + conf.getInt("io.bytes.per.checksum", 512));
      } else {
        throw e;
      }
    }

    private KeyBuffer createKeyBuffer() {
      return new KeyBuffer(columnNumber);
    }

    /**
     * Read and return the next record length, potentially skipping over a sync
     * block.
     *
     * @return the length of the next record or -1 if there is no next record
     * @throws IOException
     */
    private synchronized int readRecordLength() throws IOException {
      if (in.getPos() >= end) {
        return -1;
      }
      int length = in.readInt();
      if (sync != null && length == SYNC_ESCAPE) { // process
        // a
        // sync entry
        lastSeenSyncPos = in.getPos() - 4; // minus SYNC_ESCAPE's length
        in.readFully(syncCheck); // read syncCheck
        if (!Arrays.equals(sync, syncCheck)) {
          throw new IOException("File is corrupt!");
        }
        syncSeen = true;
        if (in.getPos() >= end) {
          return -1;
        }
        length = in.readInt(); // re-read length
      } else {
        syncSeen = false;
      }
      return length;
    }

    private void seekToNextKeyBuffer() throws IOException {
      if (!keyInit) {
        return;
      }
      if (!currentValue.inited) {
        in.skip(currentRecordLength - currentKeyLength);
      }
    }

    private int compressedKeyLen = 0;
    NonSyncDataInputBuffer keyDataIn = new NonSyncDataInputBuffer();
    NonSyncDataInputBuffer keyDecompressBuffer = new NonSyncDataInputBuffer();
    NonSyncDataOutputBuffer keyTempBuffer = new NonSyncDataOutputBuffer();

    KeyBuffer currentKey = null;
    boolean keyInit = false;

    protected int nextKeyBuffer() throws IOException {
      seekToNextKeyBuffer();
      currentRecordLength = readRecordLength();
      if (currentRecordLength == -1) {
        keyInit = false;
        return -1;
      }
      currentKeyLength = in.readInt();
      compressedKeyLen = in.readInt();
      if (decompress) {
        keyTempBuffer.reset();
        keyTempBuffer.write(in, compressedKeyLen);
        keyDecompressBuffer.reset(keyTempBuffer.getData(), compressedKeyLen);
        CompressionInputStream deflatFilter = codec.createInputStream(
            keyDecompressBuffer, keyDecompressor);
        DataInputStream compressedIn = new DataInputStream(deflatFilter);
        deflatFilter.resetState();
        keyDecompressedData.reset();
        keyDecompressedData.write(compressedIn, currentKeyLength);
        keyDataIn.reset(keyDecompressedData.getData(), currentKeyLength);
        currentKey.readFields(keyDataIn);
      } else {
        currentKey.readFields(in);
      }

      keyInit = true;
      currentValue.inited = false;

      readRowsIndexInBuffer = 0;
      recordsNumInValBuffer = currentKey.numberRows;

      for (int selIx = 0; selIx < selectedColumns.length; selIx++) {
        SelectedColumn col = selectedColumns[selIx];
        int colIx = col.colIndex;
        NonSyncDataOutputBuffer buf = currentKey.allCellValLenBuffer[colIx];
        colValLenBufferReadIn[selIx].reset(buf.getData(), buf.getLength());
        col.rowReadIndex = 0;
        col.runLength = 0;
        col.prvLength = -1;
        col.isNulled = colValLenBufferReadIn[selIx].getLength() == 0;
      }

      return currentKeyLength;
    }

    protected void currentValueBuffer() throws IOException {
      if (!keyInit) {
        nextKeyBuffer();
      }
      currentValue.keyBuffer = currentKey;
      currentValue.clearColumnBuffer();
      currentValue.readFields(in);
      currentValue.inited = true;
    }

    public boolean nextBlock() throws IOException {
      int keyLength = nextKeyBuffer();
      if(keyLength > 0) {
        currentValueBuffer();
        return true;
      }
      return false;
    }

    private boolean rowFetched = false;

    // use this buffer to hold column's cells value length for usages in
    // getColumn(), instead of using colValLenBufferReadIn directly.
    private final NonSyncDataInputBuffer fetchColumnTempBuf = new NonSyncDataInputBuffer();

    /**
     * Fetch all data in the buffer for a given column. This is useful for
     * columnar operators, which perform operations on an array data of one
     * column. It should be used together with {@link #nextColumnsBatch()}.
     * Calling getColumn() with not change the result of
     * {@link #next(LongWritable)} and
     * {@link #getCurrentRow(BytesRefArrayWritable)}.
     *
     * @param columnID the number of the column to get 0 to N-1
     * @throws IOException
     */
    public BytesRefArrayWritable getColumn(int columnID,
        BytesRefArrayWritable rest) throws IOException {
      int selColIdx = revPrjColIDs[columnID];
      if (selColIdx == -1) {
        return null;
      }

      if (rest == null) {
        rest = new BytesRefArrayWritable();
      }

      rest.resetValid(recordsNumInValBuffer);

      if (!currentValue.inited) {
        currentValueBuffer();
      }

      int columnNextRowStart = 0;
      fetchColumnTempBuf.reset(currentKey.allCellValLenBuffer[columnID]
          .getData(), currentKey.allCellValLenBuffer[columnID].getLength());
      SelectedColumn selCol = selectedColumns[selColIdx];
      byte[] uncompData = null;
      ValueBuffer.LazyDecompressionCallbackImpl decompCallBack = null;
      boolean decompressed = currentValue.decompressedFlag[selColIdx];
      if (decompressed) {
        uncompData =
              currentValue.loadedColumnsValueBuffer[selColIdx].getData();
      } else {
        decompCallBack = currentValue.lazyDecompressCallbackObjs[selColIdx];
      }
      for (int i = 0; i < recordsNumInValBuffer; i++) {
        colAdvanceRow(selColIdx, selCol);
        int length = selCol.prvLength;

        BytesRefWritable currentCell = rest.get(i);

        if (decompressed) {
          currentCell.set(uncompData, columnNextRowStart, length);
        } else {
          currentCell.set(decompCallBack, columnNextRowStart, length);
        }
        columnNextRowStart = columnNextRowStart + length;
      }
      return rest;
    }

    /**
     * Read in next key buffer and throw any data in current key buffer and
     * current value buffer. It will influence the result of
     * {@link #next(LongWritable)} and
     * {@link #getCurrentRow(BytesRefArrayWritable)}
     *
     * @return whether there still has records or not
     * @throws IOException
     */
    @SuppressWarnings("unused")
    @Deprecated
    public synchronized boolean nextColumnsBatch() throws IOException {
      passedRowsNum += (recordsNumInValBuffer - readRowsIndexInBuffer);
      return nextKeyBuffer() > 0;
    }

    /**
     * Returns how many rows we fetched with next(). It only means how many rows
     * are read by next(). The returned result may be smaller than actual number
     * of rows passed by, because {@link #seek(long)},
     * {@link #nextColumnsBatch()} can change the underlying key buffer and
     * value buffer.
     *
     * @return next row number
     * @throws IOException
     */
    public synchronized boolean next(LongWritable readRows) throws IOException {
      if (hasRecordsInBuffer()) {
        readRows.set(passedRowsNum);
        readRowsIndexInBuffer++;
        passedRowsNum++;
        rowFetched = false;
        return true;
      } else {
        keyInit = false;
      }

      int ret = -1;
      if (tolerateCorruptions) {
        ret = nextKeyValueTolerateCorruptions();
      } else {
        try {
          ret = nextKeyBuffer();
        } catch (EOFException eof) {
          LOG.warn("Failed to get next key buffer", eof);
        }
      }
      return (ret > 0) && next(readRows);
    }

    private int nextKeyValueTolerateCorruptions() throws IOException {
      long currentOffset = in.getPos();
      int ret;
      try {
        ret = nextKeyBuffer();
        this.currentValueBuffer();
      } catch (IOException ioe) {
        // A BlockMissingException indicates a temporary error,
        // not a corruption. Re-throw this exception.
        String msg = ioe.getMessage();
        if (msg != null && msg.startsWith(BLOCK_MISSING_MESSAGE)) {
          LOG.warn("Re-throwing block-missing exception" + ioe);
          throw ioe;
        }
        // We have an IOException other than a BlockMissingException.
        LOG.warn("Ignoring IOException in file " + file +
                 " after offset " + currentOffset, ioe);
        ret = -1;
      } catch (Throwable t) {
        // We got an exception that is not IOException
        // (typically OOM, IndexOutOfBounds, InternalError).
        // This is most likely a corruption.
        LOG.warn("Ignoring unknown error in " + file +
                 " after offset " + currentOffset, t);
        ret = -1;
      }
      return ret;
    }

    public boolean hasRecordsInBuffer() {
      return readRowsIndexInBuffer < recordsNumInValBuffer;
    }

    /**
     * get the current row used,make sure called {@link #next(LongWritable)}
     * first.
     *
     * @throws IOException
     */
    public synchronized void getCurrentRow(BytesRefArrayWritable ret) throws IOException {

      if (!keyInit || rowFetched) {
        return;
      }

      if (tolerateCorruptions) {
        if (!currentValue.inited) {
          currentValueBuffer();
        }
        ret.resetValid(columnNumber);
      } else {
        if (!currentValue.inited) {
          currentValueBuffer();
          // do this only when not initialized, but we may need to find a way to
          // tell the caller how to initialize the valid size
          ret.resetValid(columnNumber);
        }
      }

      // we do not use BytesWritable here to avoid the byte-copy from
      // DataOutputStream to BytesWritable
      if (currentValue.numCompressed > 0) {
        for (int j = 0; j < selectedColumns.length; ++j) {
          SelectedColumn col = selectedColumns[j];
          int i = col.colIndex;

          if (col.isNulled) {
            ret.set(i, null);
          } else {
            BytesRefWritable ref = ret.unCheckedGet(i);

            colAdvanceRow(j, col);

            if (currentValue.decompressedFlag[j]) {
              ref.set(currentValue.loadedColumnsValueBuffer[j].getData(),
                  col.rowReadIndex, col.prvLength);
            } else {
              ref.set(currentValue.lazyDecompressCallbackObjs[j],
                  col.rowReadIndex, col.prvLength);
            }
            col.rowReadIndex += col.prvLength;
          }
        }
      } else {
        // This version of the loop eliminates a condition check and branch
        // and is measurably faster (20% or so)
        for (int j = 0; j < selectedColumns.length; ++j) {
          SelectedColumn col = selectedColumns[j];
          int i = col.colIndex;

          if (col.isNulled) {
            ret.set(i, null);
          } else {
            BytesRefWritable ref = ret.unCheckedGet(i);

            colAdvanceRow(j, col);
            ref.set(currentValue.loadedColumnsValueBuffer[j].getData(),
                  col.rowReadIndex, col.prvLength);
            col.rowReadIndex += col.prvLength;
          }
        }
      }
      rowFetched = true;
    }

    /**
     * Advance column state to the next now: update offsets, run lengths etc
     * @param selCol - index among selectedColumns
     * @param col - column object to update the state of.  prvLength will be
     *        set to the new read position
     * @throws IOException
     */
    private void colAdvanceRow(int selCol, SelectedColumn col) throws IOException {
      if (col.runLength > 0) {
        --col.runLength;
      } else {
        int length = (int) WritableUtils.readVLong(colValLenBufferReadIn[selCol]);
        if (length < 0) {
          // we reach a runlength here, use the previous length and reset
          // runlength
          col.runLength = (~length) - 1;
        } else {
          col.prvLength = length;
          col.runLength = 0;
        }
      }
    }

    /** Returns true iff the previous call to next passed a sync mark. */
    @SuppressWarnings("unused")
    public boolean syncSeen() {
      return syncSeen;
    }

    /** Returns the last seen sync position. */
    public long lastSeenSyncPos() {
      return lastSeenSyncPos;
    }

    /** Returns the name of the file. */
    @Override
    public String toString() {
      return file.toString();
    }

    @SuppressWarnings("unused")
    public boolean isCompressedRCFile() {
      return this.decompress;
    }

    /** Close the reader. */
    public void close() {
      IOUtils.closeStream(in);
      currentValue.close();
      if (decompress) {
        IOUtils.closeStream(keyDecompressedData);
        if (keyDecompressor != null) {
          // Make sure we only return keyDecompressor once.
          CodecPool.returnDecompressor(keyDecompressor);
          keyDecompressor = null;
        }
      }
    }

    /**
     * return the KeyBuffer object used in the reader. Internally in each
     * reader, there is only one KeyBuffer object, which gets reused for every
     * block.
     */
    public KeyBuffer getCurrentKeyBufferObj() {
      return this.currentKey;
    }

    /**
     * return the ValueBuffer object used in the reader. Internally in each
     * reader, there is only one ValueBuffer object, which gets reused for every
     * block.
     */
    public ValueBuffer getCurrentValueBufferObj() {
      return this.currentValue;
    }

    //return the current block's length
    public int getCurrentBlockLength() {
      return this.currentRecordLength;
    }

    //return the current block's key length
    public int getCurrentKeyLength() {
      return this.currentKeyLength;
    }

    //return the current block's compressed key length
    public int getCurrentCompressedKeyLen() {
      return this.compressedKeyLen;
    }

    //return the CompressionCodec used for this file
    public CompressionCodec getCompressionCodec() {
      return this.codec;
    }

  }
}
