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
package org.apache.hadoop.hive.ql.io.orc;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ORC_ZEROCOPY;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hive.common.DiskRange;
import org.apache.hadoop.hive.common.DiskRangeList;
import org.apache.hadoop.hive.common.DiskRangeList.DiskRangeListCreateHelper;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampUtils;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.io.filters.BloomFilter;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderUtils.ByteBufferAllocatorPool;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.shims.HadoopShims.ZeroCopyReaderShim;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class RecordReaderImpl implements RecordReader {

  static final Log LOG = LogFactory.getLog(RecordReaderImpl.class);
  private static final boolean isLogTraceEnabled = LOG.isTraceEnabled();

  private final long fileId;
  private final FSDataInputStream file;
  private final long firstRow;
  private final List<StripeInformation> stripes =
    new ArrayList<StripeInformation>();
  private OrcProto.StripeFooter stripeFooter;
  private final long totalRowCount;
  private final CompressionCodec codec;
  private final List<OrcProto.Type> types;
  private final int bufferSize;
  private final boolean[] included;
  private final long rowIndexStride;
  private long rowInStripe = 0;
  private int currentStripe = -1;
  private long rowBaseInStripe = 0;
  private long rowCountInStripe = 0;
  private final Map<StreamName, InStream> streams =
      new HashMap<StreamName, InStream>();
  DiskRangeList bufferChunks = null;
  private final TreeReader reader;
  private final OrcProto.RowIndex[] indexes;
  private final OrcProto.BloomFilterIndex[] bloomFilterIndices;
  private final SargApplier sargApp;
  // same as the above array, but indices are set to true
  private final boolean[] sargColumns;
  // an array about which row groups aren't skipped
  private boolean[] includedRowGroups = null;
  private final Configuration conf;
  private final MetadataReader metadata;

  private final ByteBufferAllocatorPool pool = new ByteBufferAllocatorPool();
  private final ZeroCopyReaderShim zcr;

  public final static class Index {
    OrcProto.RowIndex[] rowGroupIndex;
    OrcProto.BloomFilterIndex[] bloomFilterIndex;

    Index(OrcProto.RowIndex[] rgIndex, OrcProto.BloomFilterIndex[] bfIndex) {
      this.rowGroupIndex = rgIndex;
      this.bloomFilterIndex = bfIndex;
    }

    public OrcProto.RowIndex[] getRowGroupIndex() {
      return rowGroupIndex;
    }

    public OrcProto.BloomFilterIndex[] getBloomFilterIndex() {
      return bloomFilterIndex;
    }

    public void setRowGroupIndex(OrcProto.RowIndex[] rowGroupIndex) {
      this.rowGroupIndex = rowGroupIndex;
    }
  }

  /**
   * Given a list of column names, find the given column and return the index.
   * @param columnNames the list of potential column names
   * @param columnName the column name to look for
   * @param rootColumn offset the result with the rootColumn
   * @return the column number or -1 if the column wasn't found
   */
  static int findColumns(String[] columnNames,
                         String columnName,
                         int rootColumn) {
    for(int i=0; i < columnNames.length; ++i) {
      if (columnName.equals(columnNames[i])) {
        return i + rootColumn;
      }
    }
    return -1;
  }

  /**
   * Find the mapping from predicate leaves to columns.
   * @param sargLeaves the search argument that we need to map
   * @param columnNames the names of the columns
   * @param rootColumn the offset of the top level row, which offsets the
   *                   result
   * @return an array mapping the sarg leaves to concrete column numbers
   */
  public static int[] mapSargColumns(List<PredicateLeaf> sargLeaves,
                             String[] columnNames,
                             int rootColumn) {
    int[] result = new int[sargLeaves.size()];
    Arrays.fill(result, -1);
    for(int i=0; i < result.length; ++i) {
      String colName = sargLeaves.get(i).getColumnName();
      result[i] = findColumns(columnNames, colName, rootColumn);
    }
    return result;
  }

  protected RecordReaderImpl(List<StripeInformation> stripes,
      FileSystem fileSystem,
      Path path,
      Reader.Options options,
      List<OrcProto.Type> types,
      CompressionCodec codec,
      int bufferSize,
      long strideRate,
      Configuration conf
  ) throws IOException {
    this.file = fileSystem.open(path);
    this.fileId = RecordReaderUtils.getFileId(fileSystem, path);
    this.codec = codec;
    this.types = types;
    this.bufferSize = bufferSize;
    this.included = options.getInclude();
    this.conf = conf;
    this.rowIndexStride = strideRate;
    this.metadata = new MetadataReader(file, codec, bufferSize, types.size());
    SearchArgument sarg = options.getSearchArgument();
    if (sarg != null && strideRate != 0) {
      sargApp = new SargApplier(sarg, options.getColumnNames(), strideRate, types);
      // included will not be null, row options will fill the array with trues if null
      sargColumns = new boolean[included.length];
      for (int i : sargApp.filterColumns) {
        // filter columns may have -1 as index which could be partition column in SARG.
        if (i > 0) {
          sargColumns[i] = true;
        }
      }
    } else {
      sargApp = null;
      sargColumns = null;
    }
    long rows = 0;
    long skippedRows = 0;
    long offset = options.getOffset();
    long maxOffset = options.getMaxOffset();
    for(StripeInformation stripe: stripes) {
      long stripeStart = stripe.getOffset();
      if (offset > stripeStart) {
        skippedRows += stripe.getNumberOfRows();
      } else if (stripeStart < maxOffset) {
        this.stripes.add(stripe);
        rows += stripe.getNumberOfRows();
      }
    }

    final boolean zeroCopy = (conf != null)
        && (HiveConf.getBoolVar(conf, HIVE_ORC_ZEROCOPY));
    zcr = zeroCopy ? RecordReaderUtils.createZeroCopyShim(file, codec, pool) : null;

    firstRow = skippedRows;
    totalRowCount = rows;
    boolean skipCorrupt = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ORC_SKIP_CORRUPT_DATA);
    reader = createTreeReader(0, types, included, skipCorrupt);
    indexes = new OrcProto.RowIndex[types.size()];
    bloomFilterIndices = new OrcProto.BloomFilterIndex[types.size()];
    advanceToNextRow(reader, 0L, true);
  }

  public static final class PositionProviderImpl implements PositionProvider {
    private final OrcProto.RowIndexEntry entry;
    private int index;

    public PositionProviderImpl(OrcProto.RowIndexEntry entry) {
      this(entry, 0);
    }

    public PositionProviderImpl(OrcProto.RowIndexEntry entry, int startPos) {
      this.entry = entry;
      this.index = startPos;
    }

    @Override
    public long getNext() {
      return entry.getPositions(index++);
    }
  }

  public abstract static class TreeReader {
    protected final int columnId;
    public BitFieldReader present = null;
    protected boolean valuePresent = false;

    public TreeReader(int columnId) throws IOException {
      this(columnId, null);
    }

    public TreeReader(int columnId, InStream in) throws IOException {
      this.columnId = columnId;
      if (in == null) {
        present = null;
        valuePresent = true;
      } else {
        present = new BitFieldReader(in, 1);
      }
    }

    void setInStream(InStream inStream) {
      if (present != null) {
        present.setInStream(inStream);
      }
    }

    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    IntegerReader createIntegerReader(OrcProto.ColumnEncoding.Kind kind,
        InStream in,
        boolean signed,
        boolean skipCorrupt) throws IOException {
      switch (kind) {
        case DIRECT_V2:
        case DICTIONARY_V2:
          return new RunLengthIntegerReaderV2(in, signed, skipCorrupt);
        case DIRECT:
        case DICTIONARY:
          return new RunLengthIntegerReader(in, signed);
        default:
          throw new IllegalArgumentException("Unknown encoding " + kind);
      }
    }

    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encoding
                    ) throws IOException {
      checkEncoding(encoding.get(columnId));
      InStream in = streams.get(new StreamName(columnId,
          OrcProto.Stream.Kind.PRESENT));
      if (in == null) {
        present = null;
        valuePresent = true;
      } else {
        present = new BitFieldReader(in, 1);
      }
    }

    /**
     * Seek to the given position.
     * @param index the indexes loaded from the file
     * @throws IOException
     */
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        present.seek(index);
      }
    }

    protected long countNonNulls(long rows) throws IOException {
      if (present != null) {
        long result = 0;
        for(long c=0; c < rows; ++c) {
          if (present.next() == 1) {
            result += 1;
          }
        }
        return result;
      } else {
        return rows;
      }
    }

    abstract void skipRows(long rows) throws IOException;

    Object next(Object previous) throws IOException {
      if (present != null) {
        valuePresent = present.next() == 1;
      }
      return previous;
    }
    /**
     * Populates the isNull vector array in the previousVector object based on
     * the present stream values. This function is called from all the child
     * readers, and they all set the values based on isNull field value.
     * @param previousVector The columnVector object whose isNull value is populated
     * @param batchSize Size of the column vector
     * @return
     * @throws IOException
     */
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      ColumnVector result = (ColumnVector) previousVector;
      if (present != null) {
        // Set noNulls and isNull vector of the ColumnVector based on
        // present stream
        result.noNulls = true;
        for (int i = 0; i < batchSize; i++) {
          result.isNull[i] = (present.next() != 1);
          if (result.noNulls && result.isNull[i]) {
            result.noNulls = false;
          }
        }
      } else {
        // There is not present stream, this means that all the values are
        // present.
        result.noNulls = true;
        for (int i = 0; i < batchSize; i++) {
          result.isNull[i] = false;
        }
      }
      return previousVector;
    }
  }

  public static class BooleanTreeReader extends TreeReader {
    protected BitFieldReader reader = null;

    public BooleanTreeReader(int columnId) throws IOException {
      this(columnId, null, null);
    }

    public BooleanTreeReader(int columnId, InStream present, InStream data) throws IOException {
      super(columnId, present);
      if (data != null) {
        reader = new BitFieldReader(data, 1);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                     ) throws IOException {
      super.startStripe(streams, encodings);
      reader = new BitFieldReader(streams.get(new StreamName(columnId,
          OrcProto.Stream.Kind.DATA)), 1);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      BooleanWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new BooleanWritable();
        } else {
          result = (BooleanWritable) previous;
        }
        result.set(reader.next() == 1);
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      LongColumnVector result = null;
      if (previousVector == null) {
        result = new LongColumnVector();
      } else {
        result = (LongColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, batchSize);
      return result;
    }
  }

  public static class ByteTreeReader extends TreeReader{
    protected RunLengthByteReader reader = null;

    ByteTreeReader(int columnId) throws IOException {
      this(columnId, null, null);
    }

    public ByteTreeReader(int columnId, InStream present, InStream data) throws IOException {
      super(columnId, present);
      this.reader = new RunLengthByteReader(data);
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      reader = new RunLengthByteReader(streams.get(new StreamName(columnId,
          OrcProto.Stream.Kind.DATA)));
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      ByteWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new ByteWritable();
        } else {
          result = (ByteWritable) previous;
        }
        result.set(reader.next());
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      LongColumnVector result = null;
      if (previousVector == null) {
        result = new LongColumnVector();
      } else {
        result = (LongColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  public static class ShortTreeReader extends TreeReader{
    protected IntegerReader reader = null;

    public ShortTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null);
    }

    public ShortTreeReader(int columnId, InStream present, InStream data,
        OrcProto.ColumnEncoding encoding)
        throws IOException {
      super(columnId, present);
      if (data != null && encoding != null) {
        checkEncoding(encoding);
        this.reader = createIntegerReader(encoding.getKind(), data, true, false);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(encodings.get(columnId).getKind(), streams.get(name), true,
          false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      ShortWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new ShortWritable();
        } else {
          result = (ShortWritable) previous;
        }
        result.set((short) reader.next());
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      LongColumnVector result = null;
      if (previousVector == null) {
        result = new LongColumnVector();
      } else {
        result = (LongColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  public static class IntTreeReader extends TreeReader{
    protected IntegerReader reader = null;

    public IntTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null);
    }

    public IntTreeReader(int columnId, InStream present, InStream data,
        OrcProto.ColumnEncoding encoding)
        throws IOException {
      super(columnId, present);
      if (data != null && encoding != null) {
        checkEncoding(encoding);
        this.reader = createIntegerReader(encoding.getKind(), data, true, false);
      }
    }

    void setInStream(InStream inStream) {
      if (reader != null) {
        reader.setInStream(inStream);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(encodings.get(columnId).getKind(), streams.get(name), true,
          false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      IntWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new IntWritable();
        } else {
          result = (IntWritable) previous;
        }
        result.set((int) reader.next());
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      LongColumnVector result = null;
      if (previousVector == null) {
        result = new LongColumnVector();
      } else {
        result = (LongColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  public static class LongTreeReader extends TreeReader{
    protected IntegerReader reader = null;

    LongTreeReader(int columnId, boolean skipCorrupt) throws IOException {
      this(columnId, null, null, null, skipCorrupt);
    }

    public LongTreeReader(int columnId, InStream present, InStream data,
        OrcProto.ColumnEncoding encoding,
        boolean skipCorrupt)
        throws IOException {
      super(columnId, present);
      if (data != null && encoding != null) {
        checkEncoding(encoding);
        this.reader = createIntegerReader(encoding.getKind(), data, true, skipCorrupt);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(encodings.get(columnId).getKind(), streams.get(name), true,
          false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      LongWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new LongWritable();
        } else {
          result = (LongWritable) previous;
        }
        result.set(reader.next());
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      LongColumnVector result = null;
      if (previousVector == null) {
        result = new LongColumnVector();
      } else {
        result = (LongColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  public static class FloatTreeReader extends TreeReader{
    protected InStream stream;
    private final SerializationUtils utils;

    public FloatTreeReader(int columnId) throws IOException {
      this(columnId, null, null);
    }

    public FloatTreeReader(int columnId, InStream present, InStream data) throws IOException {
      super(columnId, present);
      this.utils = new SerializationUtils();
      this.stream = data;
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      stream = streams.get(name);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      stream.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      FloatWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new FloatWritable();
        } else {
          result = (FloatWritable) previous;
        }
        result.set(utils.readFloat(stream));
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      DoubleColumnVector result = null;
      if (previousVector == null) {
        result = new DoubleColumnVector();
      } else {
        result = (DoubleColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      for (int i = 0; i < batchSize; i++) {
        if (!result.isNull[i]) {
          result.vector[i] = utils.readFloat(stream);
        } else {

          // If the value is not present then set NaN
          result.vector[i] = Double.NaN;
        }
      }

      // Set isRepeating flag
      result.isRepeating = true;
      for (int i = 0; (i < batchSize - 1 && result.isRepeating); i++) {
        if (result.vector[i] != result.vector[i + 1]) {
          result.isRepeating = false;
        }
      }
      return result;
    }

    @Override
    protected void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      for(int i=0; i < items; ++i) {
        utils.readFloat(stream);
      }
    }
  }

  public static class DoubleTreeReader extends TreeReader{
    protected InStream stream;
    private final SerializationUtils utils;

    public DoubleTreeReader(int columnId) throws IOException {
      this(columnId, null, null);
    }

    public DoubleTreeReader(int columnId, InStream present, InStream data) throws IOException {
      super(columnId, present);
      this.utils = new SerializationUtils();
      this.stream = data;
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      StreamName name =
        new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      stream = streams.get(name);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      stream.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      DoubleWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new DoubleWritable();
        } else {
          result = (DoubleWritable) previous;
        }
        result.set(utils.readDouble(stream));
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      DoubleColumnVector result = null;
      if (previousVector == null) {
        result = new DoubleColumnVector();
      } else {
        result = (DoubleColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      for (int i = 0; i < batchSize; i++) {
        if (!result.isNull[i]) {
          result.vector[i] = utils.readDouble(stream);
        } else {
          // If the value is not present then set NaN
          result.vector[i] = Double.NaN;
        }
      }

      // Set isRepeating flag
      result.isRepeating = true;
      for (int i = 0; (i < batchSize - 1 && result.isRepeating); i++) {
        if (result.vector[i] != result.vector[i + 1]) {
          result.isRepeating = false;
        }
      }
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      stream.skip(items * 8);
    }
  }

  public static class BinaryTreeReader extends TreeReader{
    protected InStream stream;
    protected IntegerReader lengths = null;

    protected final LongColumnVector scratchlcv;

    BinaryTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null, null);
    }

    public BinaryTreeReader(int columnId, InStream present, InStream data, InStream length,
        OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present);
      scratchlcv = new LongColumnVector();
      this.stream = data;
      if (length != null && encoding != null) {
        checkEncoding(encoding);
        this.lengths = createIntegerReader(encoding.getKind(), length, false, false);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      stream = streams.get(name);
      lengths = createIntegerReader(encodings.get(columnId).getKind(), streams.get(new
          StreamName(columnId, OrcProto.Stream.Kind.LENGTH)), false, false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      stream.seek(index);
      lengths.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      BytesWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new BytesWritable();
        } else {
          result = (BytesWritable) previous;
        }
        int len = (int) lengths.next();
        result.setSize(len);
        int offset = 0;
        while (len > 0) {
          int written = stream.read(result.getBytes(), offset, len);
          if (written < 0) {
            throw new EOFException("Can't finish byte read from " + stream);
          }
          len -= written;
          offset += written;
        }
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      BytesColumnVector result = null;
      if (previousVector == null) {
        result = new BytesColumnVector();
      } else {
        result = (BytesColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      BytesColumnVectorUtil.readOrcByteArrays(stream, lengths, scratchlcv, result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long lengthToSkip = 0;
      for(int i=0; i < items; ++i) {
        lengthToSkip += lengths.next();
      }
      stream.skip(lengthToSkip);
    }
  }

  public static class TimestampTreeReader extends TreeReader {
    protected IntegerReader data = null;
    protected IntegerReader nanos = null;
    private final boolean skipCorrupt;

    TimestampTreeReader(int columnId, boolean skipCorrupt) throws IOException {
      this(columnId, null, null, null, null, skipCorrupt);
    }

    public TimestampTreeReader(int columnId, InStream presentStream, InStream dataStream,
        InStream nanosStream, OrcProto.ColumnEncoding encoding, boolean skipCorrupt)
        throws IOException {
      super(columnId, presentStream);
      this.skipCorrupt = skipCorrupt;
      if (encoding != null) {
        checkEncoding(encoding);

        if (dataStream != null) {
          this.data = createIntegerReader(encoding.getKind(), dataStream, true, skipCorrupt);
        }

        if (nanosStream != null) {
          this.nanos = createIntegerReader(encoding.getKind(), nanosStream, false, skipCorrupt);
        }
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      data = createIntegerReader(encodings.get(columnId).getKind(),
          streams.get(new StreamName(columnId,
              OrcProto.Stream.Kind.DATA)), true, skipCorrupt);
      nanos = createIntegerReader(encodings.get(columnId).getKind(),
          streams.get(new StreamName(columnId,
              OrcProto.Stream.Kind.SECONDARY)), false, skipCorrupt);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      data.seek(index);
      nanos.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      TimestampWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new TimestampWritable();
        } else {
          result = (TimestampWritable) previous;
        }
        Timestamp ts = new Timestamp(0);
        long millis = (data.next() + WriterImpl.BASE_TIMESTAMP) *
            WriterImpl.MILLIS_PER_SECOND;
        int newNanos = parseNanos(nanos.next());
        // fix the rounding when we divided by 1000.
        if (millis >= 0) {
          millis += newNanos / 1000000;
        } else {
          millis -= newNanos / 1000000;
        }
        ts.setTime(millis);
        ts.setNanos(newNanos);
        result.set(ts);
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      LongColumnVector result = null;
      if (previousVector == null) {
        result = new LongColumnVector();
      } else {
        result = (LongColumnVector) previousVector;
      }

      result.reset();
      Object obj = null;
      for (int i = 0; i < batchSize; i++) {
        obj = next(obj);
        if (obj == null) {
          result.noNulls = false;
          result.isNull[i] = true;
        } else {
          TimestampWritable writable = (TimestampWritable) obj;
          Timestamp  timestamp = writable.getTimestamp();
          result.vector[i] = TimestampUtils.getTimeNanoSec(timestamp);
        }
      }

      return result;
    }

    private static int parseNanos(long serialized) {
      int zeros = 7 & (int) serialized;
      int result = (int) (serialized >>> 3);
      if (zeros != 0) {
        for(int i =0; i <= zeros; ++i) {
          result *= 10;
        }
      }
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      data.skip(items);
      nanos.skip(items);
    }
  }

  public static class DateTreeReader extends TreeReader{
    protected IntegerReader reader = null;

    DateTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null);
    }

    public DateTreeReader(int columnId, InStream present, InStream data,
        OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present);
      if (data != null && encoding != null) {
        checkEncoding(encoding);
        reader = createIntegerReader(encoding.getKind(), data, true, false);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(encodings.get(columnId).getKind(), streams.get(name), true, false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      DateWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new DateWritable();
        } else {
          result = (DateWritable) previous;
        }
        result.set((int) reader.next());
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      LongColumnVector result = null;
      if (previousVector == null) {
        result = new LongColumnVector();
      } else {
        result = (LongColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      reader.nextVector(result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  public static class DecimalTreeReader extends TreeReader{
    protected InStream value;
    protected IntegerReader scaleReader = null;
    private LongColumnVector scratchScaleVector;

    private final int precision;
    private final int scale;

    DecimalTreeReader(int columnId, int precision, int scale) throws IOException {
      this(columnId, precision, scale, null, null, null, null);
    }

    public DecimalTreeReader(int columnId, int precision, int scale, InStream present,
        InStream valueStream, InStream scaleStream, OrcProto.ColumnEncoding encoding)
        throws IOException {
      super(columnId, present);
      this.precision = precision;
      this.scale = scale;
      this.scratchScaleVector = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      this.value = valueStream;
      if (scaleStream != null && encoding != null) {
        checkEncoding(encoding);
        this.scaleReader = createIntegerReader(encoding.getKind(), scaleStream, true, false);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
    ) throws IOException {
      super.startStripe(streams, encodings);
      value = streams.get(new StreamName(columnId,
          OrcProto.Stream.Kind.DATA));
      scaleReader = createIntegerReader(encodings.get(columnId).getKind(), streams.get(
          new StreamName(columnId, OrcProto.Stream.Kind.SECONDARY)), true, false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      value.seek(index);
      scaleReader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      HiveDecimalWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new HiveDecimalWritable();
        } else {
          result = (HiveDecimalWritable) previous;
        }
        result.set(HiveDecimal.create(SerializationUtils.readBigInteger(value),
            (int) scaleReader.next()));
        return HiveDecimalUtils.enforcePrecisionScale(result, precision, scale);
      }
      return null;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      DecimalColumnVector result = null;
      if (previousVector == null) {
        result = new DecimalColumnVector(precision, scale);
      } else {
        result = (DecimalColumnVector) previousVector;
      }

      // Save the reference for isNull in the scratch vector
      boolean [] scratchIsNull = scratchScaleVector.isNull;

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      if (result.isRepeating) {
        if (!result.isNull[0]) {
          BigInteger bInt = SerializationUtils.readBigInteger(value);
          short scaleInData = (short) scaleReader.next();
          HiveDecimal dec = HiveDecimal.create(bInt, scaleInData);
          dec = HiveDecimalUtils.enforcePrecisionScale(dec, precision, scale);
          result.set(0, dec);
        }
      } else {
        // result vector has isNull values set, use the same to read scale vector.
        scratchScaleVector.isNull = result.isNull;
        scaleReader.nextVector(scratchScaleVector, batchSize);
        for (int i = 0; i < batchSize; i++) {
          if (!result.isNull[i]) {
            BigInteger bInt = SerializationUtils.readBigInteger(value);
            short scaleInData = (short) scratchScaleVector.vector[i];
            HiveDecimal dec = HiveDecimal.create(bInt, scaleInData);
            dec = HiveDecimalUtils.enforcePrecisionScale(dec, precision, scale);
            result.set(i, dec);
          }
        }
      }
      // Switch back the null vector.
      scratchScaleVector.isNull = scratchIsNull;
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      for(int i=0; i < items; i++) {
        SerializationUtils.readBigInteger(value);
      }
      scaleReader.skip(items);
    }
  }

  /**
   * A tree reader that will read string columns. At the start of the
   * stripe, it creates an internal reader based on whether a direct or
   * dictionary encoding was used.
   */
  public static class StringTreeReader extends TreeReader {
    protected TreeReader reader;

    public StringTreeReader(int columnId) throws IOException {
      super(columnId);
    }

    public StringTreeReader(int columnId, InStream present, InStream data, InStream length,
        InStream dictionary, OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present);
      if (encoding != null) {
        switch (encoding.getKind()) {
          case DIRECT:
          case DIRECT_V2:
            reader = new StringDirectTreeReader(columnId, present, data, length,
                encoding.getKind());
            break;
          case DICTIONARY:
          case DICTIONARY_V2:
            reader = new StringDictionaryTreeReader(columnId, present, data, length, dictionary,
                encoding);
            break;
          default:
            throw new IllegalArgumentException("Unsupported encoding " +
                encoding.getKind());
        }
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      reader.checkEncoding(encoding);
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      // For each stripe, checks the encoding and initializes the appropriate
      // reader
      switch (encodings.get(columnId).getKind()) {
        case DIRECT:
        case DIRECT_V2:
          reader = new StringDirectTreeReader(columnId);
          break;
        case DICTIONARY:
        case DICTIONARY_V2:
          reader = new StringDictionaryTreeReader(columnId);
          break;
        default:
          throw new IllegalArgumentException("Unsupported encoding " +
              encodings.get(columnId).getKind());
      }
      reader.startStripe(streams, encodings);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      reader.seek(index);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      reader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      return reader.next(previous);
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      return reader.nextVector(previousVector, batchSize);
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skipRows(items);
    }
  }

  // This class collects together very similar methods for reading an ORC vector of byte arrays and
  // creating the BytesColumnVector.
  //
   public static class BytesColumnVectorUtil {

    private static byte[] commonReadByteArrays(InStream stream, IntegerReader lengths, LongColumnVector scratchlcv,
            BytesColumnVector result, long batchSize) throws IOException {
      // Read lengths
      scratchlcv.isNull = result.isNull;  // Notice we are replacing the isNull vector here...
      lengths.nextVector(scratchlcv, batchSize);
      int totalLength = 0;
      if (!scratchlcv.isRepeating) {
        for (int i = 0; i < batchSize; i++) {
          if (!scratchlcv.isNull[i]) {
            totalLength += (int) scratchlcv.vector[i];
          }
        }
      } else {
        if (!scratchlcv.isNull[0]) {
          totalLength = (int) (batchSize * scratchlcv.vector[0]);
        }
      }

      // Read all the strings for this batch
      byte[] allBytes = new byte[totalLength];
      int offset = 0;
      int len = totalLength;
      while (len > 0) {
        int bytesRead = stream.read(allBytes, offset, len);
        if (bytesRead < 0) {
          throw new EOFException("Can't finish byte read from " + stream);
        }
        len -= bytesRead;
        offset += bytesRead;
      }

      return allBytes;
    }

    // This method has the common code for reading in bytes into a BytesColumnVector.
    public static void readOrcByteArrays(InStream stream, IntegerReader lengths, LongColumnVector scratchlcv,
            BytesColumnVector result, long batchSize) throws IOException {

      byte[] allBytes = commonReadByteArrays(stream, lengths, scratchlcv, result, batchSize);

      // Too expensive to figure out 'repeating' by comparisons.
      result.isRepeating = false;
      int offset = 0;
      if (!scratchlcv.isRepeating) {
        for (int i = 0; i < batchSize; i++) {
          if (!scratchlcv.isNull[i]) {
            result.setRef(i, allBytes, offset, (int) scratchlcv.vector[i]);
            offset += scratchlcv.vector[i];
          } else {
            result.setRef(i, allBytes, 0, 0);
          }
        }
      } else {
        for (int i = 0; i < batchSize; i++) {
          if (!scratchlcv.isNull[i]) {
            result.setRef(i, allBytes, offset, (int) scratchlcv.vector[0]);
            offset += scratchlcv.vector[0];
          } else {
            result.setRef(i, allBytes, 0, 0);
          }
        }
      }
    }
  }

  /**
   * A reader for string columns that are direct encoded in the current
   * stripe.
   */
  public static class StringDirectTreeReader extends TreeReader {
    public InStream stream;
    public IntegerReader lengths;
    private final LongColumnVector scratchlcv;

    StringDirectTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null, null);
    }

    public StringDirectTreeReader(int columnId, InStream present, InStream data, InStream length,
        OrcProto.ColumnEncoding.Kind encoding) throws IOException {
      super(columnId, present);
      this.scratchlcv = new LongColumnVector();
      this.stream = data;
      if (length != null && encoding != null) {
        this.lengths = createIntegerReader(encoding, length, false, false);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT &&
          encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      stream = streams.get(name);
      lengths = createIntegerReader(encodings.get(columnId).getKind(),
          streams.get(new StreamName(columnId, OrcProto.Stream.Kind.LENGTH)),
          false, false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      stream.seek(index);
      lengths.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      Text result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new Text();
        } else {
          result = (Text) previous;
        }
        int len = (int) lengths.next();
        int offset = 0;
        byte[] bytes = new byte[len];
        while (len > 0) {
          int written = stream.read(bytes, offset, len);
          if (written < 0) {
            throw new EOFException("Can't finish byte read from " + stream);
          }
          len -= written;
          offset += written;
        }
        result.set(bytes);
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      BytesColumnVector result = null;
      if (previousVector == null) {
        result = new BytesColumnVector();
      } else {
        result = (BytesColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      BytesColumnVectorUtil.readOrcByteArrays(stream, lengths, scratchlcv, result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long lengthToSkip = 0;
      for(int i=0; i < items; ++i) {
        lengthToSkip += lengths.next();
      }
      stream.skip(lengthToSkip);
    }
  }

  /**
   * A reader for string columns that are dictionary encoded in the current
   * stripe.
   */
  public static class StringDictionaryTreeReader extends TreeReader {
    private DynamicByteArray dictionaryBuffer;
    private int[] dictionaryOffsets;
    public IntegerReader reader;

    private byte[] dictionaryBufferInBytesCache = null;
    private final LongColumnVector scratchlcv;

    StringDictionaryTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null, null, null);
    }

    public StringDictionaryTreeReader(int columnId, InStream present, InStream data,
        InStream length, InStream dictionary, OrcProto.ColumnEncoding encoding)
        throws IOException{
      super(columnId, present);
      scratchlcv = new LongColumnVector();
      if (data != null && encoding != null) {
        this.reader = createIntegerReader(encoding.getKind(), data, false, false);
      }

      if (dictionary != null && encoding != null) {
        readDictionaryStream(dictionary);
      }

      if (length != null && encoding != null) {
        readDictionaryLengthStream(length, encoding);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DICTIONARY &&
          encoding.getKind() != OrcProto.ColumnEncoding.Kind.DICTIONARY_V2) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);

      // read the dictionary blob
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DICTIONARY_DATA);
      InStream in = streams.get(name);
      readDictionaryStream(in);

      // read the lengths
      name = new StreamName(columnId, OrcProto.Stream.Kind.LENGTH);
      in = streams.get(name);
      readDictionaryLengthStream(in, encodings.get(columnId));

      // set up the row reader
      name = new StreamName(columnId, OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(encodings.get(columnId).getKind(),
          streams.get(name), false, false);
    }

    private void readDictionaryLengthStream(InStream in, OrcProto.ColumnEncoding encoding)
        throws IOException {
      int dictionarySize = encoding.getDictionarySize();
      if (in != null) { // Guard against empty LENGTH stream.
        IntegerReader lenReader = createIntegerReader(encoding.getKind(), in, false, false);
        int offset = 0;
        if (dictionaryOffsets == null ||
            dictionaryOffsets.length < dictionarySize + 1) {
          dictionaryOffsets = new int[dictionarySize + 1];
        }
        for (int i = 0; i < dictionarySize; ++i) {
          dictionaryOffsets[i] = offset;
          offset += (int) lenReader.next();
        }
        dictionaryOffsets[dictionarySize] = offset;
        in.close();
      }

    }

    private void readDictionaryStream(InStream in) throws IOException {
      if (in != null) { // Guard against empty dictionary stream.
        if (in.available() > 0) {
          dictionaryBuffer = new DynamicByteArray(64, in.available());
          dictionaryBuffer.readAll(in);
          // Since its start of strip invalidate the cache.
          dictionaryBufferInBytesCache = null;
        }
        in.close();
      } else {
        dictionaryBuffer = null;
      }
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      Text result = null;
      if (valuePresent) {
        int entry = (int) reader.next();
        if (previous == null) {
          result = new Text();
        } else {
          result = (Text) previous;
        }
        int offset = dictionaryOffsets[entry];
        int length = getDictionaryEntryLength(entry, offset);
        // If the column is just empty strings, the size will be zero,
        // so the buffer will be null, in that case just return result
        // as it will default to empty
        if (dictionaryBuffer != null) {
          dictionaryBuffer.setText(result, offset, length);
        } else {
          result.clear();
        }
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      BytesColumnVector result = null;
      int offset = 0, length = 0;
      if (previousVector == null) {
        result = new BytesColumnVector();
      } else {
        result = (BytesColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      if (dictionaryBuffer != null) {

        // Load dictionaryBuffer into cache.
        if (dictionaryBufferInBytesCache == null) {
          dictionaryBufferInBytesCache = dictionaryBuffer.get();
        }

        // Read string offsets
        scratchlcv.isNull = result.isNull;
        reader.nextVector(scratchlcv, batchSize);
        if (!scratchlcv.isRepeating) {

          // The vector has non-repeating strings. Iterate thru the batch
          // and set strings one by one
          for (int i = 0; i < batchSize; i++) {
            if (!scratchlcv.isNull[i]) {
              offset = dictionaryOffsets[(int) scratchlcv.vector[i]];
              length = getDictionaryEntryLength((int) scratchlcv.vector[i], offset);
              result.setRef(i, dictionaryBufferInBytesCache, offset, length);
            } else {
              // If the value is null then set offset and length to zero (null string)
              result.setRef(i, dictionaryBufferInBytesCache, 0, 0);
            }
          }
        } else {
          // If the value is repeating then just set the first value in the
          // vector and set the isRepeating flag to true. No need to iterate thru and
          // set all the elements to the same value
          offset = dictionaryOffsets[(int) scratchlcv.vector[0]];
          length = getDictionaryEntryLength((int) scratchlcv.vector[0], offset);
          result.setRef(0, dictionaryBufferInBytesCache, offset, length);
        }
        result.isRepeating = scratchlcv.isRepeating;
      } else {
        // Entire stripe contains null strings.
        result.isRepeating = true;
        result.noNulls = false;
        result.isNull[0] = true;
        result.setRef(0, "".getBytes(), 0, 0);
      }
      return result;
    }

    int getDictionaryEntryLength(int entry, int offset) {
      int length = 0;
      // if it isn't the last entry, subtract the offsets otherwise use
      // the buffer length.
      if (entry < dictionaryOffsets.length - 1) {
        length = dictionaryOffsets[entry + 1] - offset;
      } else {
        length = dictionaryBuffer.size() - offset;
      }
      return length;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  public static class CharTreeReader extends StringTreeReader {
    int maxLength;

    public CharTreeReader(int columnId, int maxLength) throws IOException {
      this(columnId, maxLength, null, null, null, null, null);
    }

    public CharTreeReader(int columnId, int maxLength, InStream present, InStream data,
        InStream length, InStream dictionary, OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present, data, length, dictionary, encoding);
      this.maxLength = maxLength;
    }

    @Override
    Object next(Object previous) throws IOException {
      HiveCharWritable result = null;
      if (previous == null) {
        result = new HiveCharWritable();
      } else {
        result = (HiveCharWritable) previous;
      }
      // Use the string reader implementation to populate the internal Text value
      Object textVal = super.next(result.getTextValue());
      if (textVal == null) {
        return null;
      }
      // result should now hold the value that was read in.
      // enforce char length
      result.enforceMaxLength(maxLength);
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      // Get the vector of strings from StringTreeReader, then make a 2nd pass to
      // adjust down the length (right trim and truncate) if necessary.
      BytesColumnVector result = (BytesColumnVector) super.nextVector(previousVector, batchSize);

      int adjustedDownLen;
      if (result.isRepeating) {
        if (result.noNulls || !result.isNull[0]) {
          adjustedDownLen = StringExpr.rightTrimAndTruncate(result.vector[0], result.start[0], result.length[0], maxLength);
          if (adjustedDownLen < result.length[0]) {
            result.setRef(0, result.vector[0], result.start[0], adjustedDownLen);
          }
        }
      } else {
        if (result.noNulls){
          for (int i = 0; i < batchSize; i++) {
            adjustedDownLen = StringExpr.rightTrimAndTruncate(result.vector[i], result.start[i], result.length[i], maxLength);
            if (adjustedDownLen < result.length[i]) {
              result.setRef(i, result.vector[i], result.start[i], adjustedDownLen);
            }
          }
        } else {
          for (int i = 0; i < batchSize; i++) {
            if (!result.isNull[i]) {
              adjustedDownLen = StringExpr.rightTrimAndTruncate(result.vector[i], result.start[i], result.length[i], maxLength);
              if (adjustedDownLen < result.length[i]) {
                result.setRef(i, result.vector[i], result.start[i], adjustedDownLen);
              }
            }
          }
        }
      }
      return result;
    }
  }

  public static class VarcharTreeReader extends StringTreeReader {
    int maxLength;

    public VarcharTreeReader(int columnId, int maxLength) throws IOException {
      this(columnId, maxLength, null, null, null, null, null);
    }

    public VarcharTreeReader(int columnId, int maxLength, InStream present, InStream data,
        InStream length, InStream dictionary, OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present, data, length, dictionary, encoding);
      this.maxLength = maxLength;
    }

    @Override
    Object next(Object previous) throws IOException {
      HiveVarcharWritable result = null;
      if (previous == null) {
        result = new HiveVarcharWritable();
      } else {
        result = (HiveVarcharWritable) previous;
      }
      // Use the string reader implementation to populate the internal Text value
      Object textVal = super.next(result.getTextValue());
      if (textVal == null) {
        return null;
      }
      // result should now hold the value that was read in.
      // enforce varchar length
      result.enforceMaxLength(maxLength);
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      // Get the vector of strings from StringTreeReader, then make a 2nd pass to
      // adjust down the length (truncate) if necessary.
      BytesColumnVector result = (BytesColumnVector) super.nextVector(previousVector, batchSize);

      int adjustedDownLen;
      if (result.isRepeating) {
      if (result.noNulls || !result.isNull[0]) {
          adjustedDownLen = StringExpr.truncate(result.vector[0], result.start[0], result.length[0], maxLength);
          if (adjustedDownLen < result.length[0]) {
            result.setRef(0, result.vector[0], result.start[0], adjustedDownLen);
          }
        }
      } else {
        if (result.noNulls){
          for (int i = 0; i < batchSize; i++) {
            adjustedDownLen = StringExpr.truncate(result.vector[i], result.start[i], result.length[i], maxLength);
            if (adjustedDownLen < result.length[i]) {
              result.setRef(i, result.vector[i], result.start[i], adjustedDownLen);
            }
          }
        } else {
          for (int i = 0; i < batchSize; i++) {
            if (!result.isNull[i]) {
              adjustedDownLen = StringExpr.truncate(result.vector[i], result.start[i], result.length[i], maxLength);
              if (adjustedDownLen < result.length[i]) {
                result.setRef(i, result.vector[i], result.start[i], adjustedDownLen);
              }
            }
          }
        }
      }
      return result;
    }
  }

  private static class StructTreeReader extends TreeReader {
    private final TreeReader[] fields;
    private final String[] fieldNames;
    private final List<TreeReader> readers;

    StructTreeReader(int columnId,
                     List<OrcProto.Type> types,
                     boolean[] included,
                     boolean skipCorrupt) throws IOException {
      super(columnId);
      OrcProto.Type type = types.get(columnId);
      int fieldCount = type.getFieldNamesCount();
      this.fields = new TreeReader[fieldCount];
      this.fieldNames = new String[fieldCount];
      this.readers = new ArrayList<TreeReader>();
      for(int i=0; i < fieldCount; ++i) {
        int subtype = type.getSubtypes(i);
        if (included == null || included[subtype]) {
          this.fields[i] = createTreeReader(subtype, types, included, skipCorrupt);
          readers.add(this.fields[i]);
        }
        this.fieldNames[i] = type.getFieldNames(i);
      }
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      for(TreeReader kid: fields) {
        if (kid != null) {
          kid.seek(index);
        }
      }
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      OrcStruct result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new OrcStruct(fields.length);
        } else {
          result = (OrcStruct) previous;

          // If the input format was initialized with a file with a
          // different number of fields, the number of fields needs to
          // be updated to the correct number
          if (result.getNumFields() != fields.length) {
            result.setNumFields(fields.length);
          }
        }
        for(int i=0; i < fields.length; ++i) {
          if (fields[i] != null) {
            result.setFieldValue(i, fields[i].next(result.getFieldValue(i)));
          }
        }
      }
      return result;
    }

    /**
     * @return Total count of <b>non-null</b> field readers.
     */
    int getReaderCount() {
      return readers.size();
    }

    /**
     * @param readerIndex Index among <b>non-null</b> readers. Not a column index!
     * @return The readerIndex-s <b>non-null</b> field reader.
     */
    TreeReader getColumnReader(int readerIndex) {
      return readers.get(readerIndex);
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      ColumnVector[] result = null;
      if (previousVector == null) {
        result = new ColumnVector[fields.length];
      } else {
        result = (ColumnVector[]) previousVector;
      }

      // Read all the members of struct as column vectors
      for (int i = 0; i < fields.length; i++) {
        if (fields[i] != null) {
          if (result[i] == null) {
            result[i] = (ColumnVector) fields[i].nextVector(null, batchSize);
          } else {
            fields[i].nextVector(result[i], batchSize);
          }
        }
      }
      return result;
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      for(TreeReader field: fields) {
        if (field != null) {
          field.startStripe(streams, encodings);
        }
      }
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      for(TreeReader field: fields) {
        if (field != null) {
          field.skipRows(items);
        }
      }
    }
  }

  private static class UnionTreeReader extends TreeReader {
    private final TreeReader[] fields;
    private RunLengthByteReader tags;

    UnionTreeReader(int columnId,
                    List<OrcProto.Type> types,
                    boolean[] included,
                    boolean skipCorrupt) throws IOException {
      super(columnId);
      OrcProto.Type type = types.get(columnId);
      int fieldCount = type.getSubtypesCount();
      this.fields = new TreeReader[fieldCount];
      for(int i=0; i < fieldCount; ++i) {
        int subtype = type.getSubtypes(i);
        if (included == null || included[subtype]) {
          this.fields[i] = createTreeReader(subtype, types, included, skipCorrupt);
        }
      }
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      tags.seek(index[columnId]);
      for(TreeReader kid: fields) {
        kid.seek(index);
      }
    }

    @Override
    Object next(Object previous) throws IOException {
      super.next(previous);
      OrcUnion result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new OrcUnion();
        } else {
          result = (OrcUnion) previous;
        }
        byte tag = tags.next();
        Object previousVal = result.getObject();
        result.set(tag, fields[tag].next(tag == result.getTag() ?
            previousVal : null));
      }
      return result;
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      throw new UnsupportedOperationException(
          "NextVector is not supported operation for Union type");
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                     ) throws IOException {
      super.startStripe(streams, encodings);
      tags = new RunLengthByteReader(streams.get(new StreamName(columnId,
          OrcProto.Stream.Kind.DATA)));
      for(TreeReader field: fields) {
        if (field != null) {
          field.startStripe(streams, encodings);
        }
      }
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long[] counts = new long[fields.length];
      for(int i=0; i < items; ++i) {
        counts[tags.next()] += 1;
      }
      for(int i=0; i < counts.length; ++i) {
        fields[i].skipRows(counts[i]);
      }
    }
  }

  private static class ListTreeReader extends TreeReader {
    private final TreeReader elementReader;
    private IntegerReader lengths = null;

    ListTreeReader(int columnId,
                   List<OrcProto.Type> types,
                   boolean[] included,
                   boolean skipCorrupt) throws IOException {
      super(columnId);
      OrcProto.Type type = types.get(columnId);
      elementReader = createTreeReader(type.getSubtypes(0), types, included, skipCorrupt);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      lengths.seek(index[columnId]);
      elementReader.seek(index);
    }

    @Override
    @SuppressWarnings("unchecked")
    Object next(Object previous) throws IOException {
      super.next(previous);
      List<Object> result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new ArrayList<Object>();
        } else {
          result = (ArrayList<Object>) previous;
        }
        int prevLength = result.size();
        int length = (int) lengths.next();
        // extend the list to the new length
        for(int i=prevLength; i < length; ++i) {
          result.add(null);
        }
        // read the new elements into the array
        for(int i=0; i< length; i++) {
          result.set(i, elementReader.next(i < prevLength ?
              result.get(i) : null));
        }
        // remove any extra elements
        for(int i=prevLength - 1; i >= length; --i) {
          result.remove(i);
        }
      }
      return result;
    }

    @Override
    public Object nextVector(Object previous, long batchSize) throws IOException {
      throw new UnsupportedOperationException(
          "NextVector is not supported operation for List type");
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      lengths = createIntegerReader(encodings.get(columnId).getKind(),
          streams.get(new StreamName(columnId,
              OrcProto.Stream.Kind.LENGTH)), false, false);
      if (elementReader != null) {
        elementReader.startStripe(streams, encodings);
      }
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long childSkip = 0;
      for(long i=0; i < items; ++i) {
        childSkip += lengths.next();
      }
      elementReader.skipRows(childSkip);
    }
  }

  private static class MapTreeReader extends TreeReader {
    private final TreeReader keyReader;
    private final TreeReader valueReader;
    private IntegerReader lengths = null;

    MapTreeReader(int columnId,
                  List<OrcProto.Type> types,
                  boolean[] included,
                  boolean skipCorrupt) throws IOException {
      super(columnId);
      OrcProto.Type type = types.get(columnId);
      int keyColumn = type.getSubtypes(0);
      int valueColumn = type.getSubtypes(1);
      if (included == null || included[keyColumn]) {
        keyReader = createTreeReader(keyColumn, types, included, skipCorrupt);
      } else {
        keyReader = null;
      }
      if (included == null || included[valueColumn]) {
        valueReader = createTreeReader(valueColumn, types, included, skipCorrupt);
      } else {
        valueReader = null;
      }
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      lengths.seek(index[columnId]);
      keyReader.seek(index);
      valueReader.seek(index);
    }

    @Override
    @SuppressWarnings("unchecked")
    Object next(Object previous) throws IOException {
      super.next(previous);
      Map<Object, Object> result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new LinkedHashMap<Object, Object>();
        } else {
          result = (LinkedHashMap<Object, Object>) previous;
        }
        // for now just clear and create new objects
        result.clear();
        int length = (int) lengths.next();
        // read the new elements into the array
        for(int i=0; i< length; i++) {
          result.put(keyReader.next(null), valueReader.next(null));
        }
      }
      return result;
    }

    @Override
    public Object nextVector(Object previous, long batchSize) throws IOException {
      throw new UnsupportedOperationException(
          "NextVector is not supported operation for Map type");
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     List<OrcProto.ColumnEncoding> encodings
                    ) throws IOException {
      super.startStripe(streams, encodings);
      lengths = createIntegerReader(encodings.get(columnId).getKind(),
          streams.get(new StreamName(columnId,
              OrcProto.Stream.Kind.LENGTH)), false, false);
      if (keyReader != null) {
        keyReader.startStripe(streams, encodings);
      }
      if (valueReader != null) {
        valueReader.startStripe(streams, encodings);
      }
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long childSkip = 0;
      for(long i=0; i < items; ++i) {
        childSkip += lengths.next();
      }
      keyReader.skipRows(childSkip);
      valueReader.skipRows(childSkip);
    }
  }

  private static TreeReader createTreeReader(int columnId,
                                             List<OrcProto.Type> types,
                                             boolean[] included,
                                             boolean skipCorrupt
                                            ) throws IOException {
    OrcProto.Type type = types.get(columnId);
    switch (type.getKind()) {
      case BOOLEAN:
        return new BooleanTreeReader(columnId);
      case BYTE:
        return new ByteTreeReader(columnId);
      case DOUBLE:
        return new DoubleTreeReader(columnId);
      case FLOAT:
        return new FloatTreeReader(columnId);
      case SHORT:
        return new ShortTreeReader(columnId);
      case INT:
        return new IntTreeReader(columnId);
      case LONG:
        return new LongTreeReader(columnId, skipCorrupt);
      case STRING:
        return new StringTreeReader(columnId);
      case CHAR:
        if (!type.hasMaximumLength()) {
          throw new IllegalArgumentException("ORC char type has no length specified");
        }
        return new CharTreeReader(columnId, type.getMaximumLength());
      case VARCHAR:
        if (!type.hasMaximumLength()) {
          throw new IllegalArgumentException("ORC varchar type has no length specified");
        }
        return new VarcharTreeReader(columnId, type.getMaximumLength());
      case BINARY:
        return new BinaryTreeReader(columnId);
      case TIMESTAMP:
        return new TimestampTreeReader(columnId, skipCorrupt);
      case DATE:
        return new DateTreeReader(columnId);
      case DECIMAL:
        int precision = type.hasPrecision() ? type.getPrecision() : HiveDecimal.SYSTEM_DEFAULT_PRECISION;
        int scale =  type.hasScale()? type.getScale() : HiveDecimal.SYSTEM_DEFAULT_SCALE;
        return new DecimalTreeReader(columnId, precision, scale);
      case STRUCT:
        return new StructTreeReader(columnId, types, included, skipCorrupt);
      case LIST:
        return new ListTreeReader(columnId, types, included, skipCorrupt);
      case MAP:
        return new MapTreeReader(columnId, types, included, skipCorrupt);
      case UNION:
        return new UnionTreeReader(columnId, types, included, skipCorrupt);
      default:
        throw new IllegalArgumentException("Unsupported type " +
          type.getKind());
    }
  }

  OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) throws IOException {
    return metadata.readStripeFooter(stripe);
  }

  static enum Location {
    BEFORE, MIN, MIDDLE, MAX, AFTER
  }

  /**
   * Given a point and min and max, determine if the point is before, at the
   * min, in the middle, at the max, or after the range.
   * @param point the point to test
   * @param min the minimum point
   * @param max the maximum point
   * @param <T> the type of the comparision
   * @return the location of the point
   */
  static <T> Location compareToRange(Comparable<T> point, T min, T max) {
    int minCompare = point.compareTo(min);
    if (minCompare < 0) {
      return Location.BEFORE;
    } else if (minCompare == 0) {
      return Location.MIN;
    }
    int maxCompare = point.compareTo(max);
    if (maxCompare > 0) {
      return Location.AFTER;
    } else if (maxCompare == 0) {
      return Location.MAX;
    }
    return Location.MIDDLE;
  }

  /**
   * Get the maximum value out of an index entry.
   * @param index
   *          the index entry
   * @return the object for the maximum value or null if there isn't one
   */
  static Object getMax(ColumnStatistics index) {
    if (index instanceof IntegerColumnStatistics) {
      return ((IntegerColumnStatistics) index).getMaximum();
    } else if (index instanceof DoubleColumnStatistics) {
      return ((DoubleColumnStatistics) index).getMaximum();
    } else if (index instanceof StringColumnStatistics) {
      return ((StringColumnStatistics) index).getMaximum();
    } else if (index instanceof DateColumnStatistics) {
      return ((DateColumnStatistics) index).getMaximum();
    } else if (index instanceof DecimalColumnStatistics) {
      return ((DecimalColumnStatistics) index).getMaximum();
    } else if (index instanceof TimestampColumnStatistics) {
      return ((TimestampColumnStatistics) index).getMaximum();
    } else if (index instanceof BooleanColumnStatistics) {
      if (((BooleanColumnStatistics)index).getTrueCount()!=0) {
        return "true";
      } else {
        return "false";
      }
    } else {
      return null;
    }
  }

  /**
   * Get the minimum value out of an index entry.
   * @param index
   *          the index entry
   * @return the object for the minimum value or null if there isn't one
   */
  static Object getMin(ColumnStatistics index) {
    if (index instanceof IntegerColumnStatistics) {
      return ((IntegerColumnStatistics) index).getMinimum();
    } else if (index instanceof DoubleColumnStatistics) {
      return ((DoubleColumnStatistics) index).getMinimum();
    } else if (index instanceof StringColumnStatistics) {
      return ((StringColumnStatistics) index).getMinimum();
    } else if (index instanceof DateColumnStatistics) {
      return ((DateColumnStatistics) index).getMinimum();
    } else if (index instanceof DecimalColumnStatistics) {
      return ((DecimalColumnStatistics) index).getMinimum();
    } else if (index instanceof TimestampColumnStatistics) {
      return ((TimestampColumnStatistics) index).getMinimum();
    } else if (index instanceof BooleanColumnStatistics) {
      if (((BooleanColumnStatistics)index).getFalseCount()!=0) {
        return "false";
      } else {
        return "true";
      }
    } else {
      return null;
    }
  }

  /**
   * Evaluate a predicate with respect to the statistics from the column
   * that is referenced in the predicate.
   * @param statsProto the statistics for the column mentioned in the predicate
   * @param predicate the leaf predicate we need to evaluation
   * @param bloomFilter
   * @return the set of truth values that may be returned for the given
   *   predicate.
   */
  static TruthValue evaluatePredicateProto(OrcProto.ColumnStatistics statsProto,
      PredicateLeaf predicate, OrcProto.BloomFilter bloomFilter) {
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(statsProto);
    Object minValue = getMin(cs);
    Object maxValue = getMax(cs);
    BloomFilter bf = null;
    if (bloomFilter != null) {
      bf = new BloomFilter(bloomFilter);
    }
    return evaluatePredicateRange(predicate, minValue, maxValue, cs.hasNull(), bf);
  }

  /**
   * Evaluate a predicate with respect to the statistics from the column
   * that is referenced in the predicate.
   * @param stats the statistics for the column mentioned in the predicate
   * @param predicate the leaf predicate we need to evaluation
   * @return the set of truth values that may be returned for the given
   *   predicate.
   */
  static TruthValue evaluatePredicate(ColumnStatistics stats,
      PredicateLeaf predicate, BloomFilter bloomFilter) {
    Object minValue = getMin(stats);
    Object maxValue = getMax(stats);
    return evaluatePredicateRange(predicate, minValue, maxValue, stats.hasNull(), bloomFilter);
  }

  static TruthValue evaluatePredicateRange(PredicateLeaf predicate, Object min,
      Object max, boolean hasNull, BloomFilter bloomFilter) {
    // if we didn't have any values, everything must have been null
    if (min == null) {
      if (predicate.getOperator() == PredicateLeaf.Operator.IS_NULL) {
        return TruthValue.YES;
      } else {
        return TruthValue.NULL;
      }
    }

    TruthValue result;
    // Predicate object and stats object can be one of the following base types
    // LONG, DOUBLE, STRING, DATE, DECIMAL
    // Out of these DATE is not implicitly convertible to other types and rest
    // others are implicitly convertible. In cases where DATE cannot be converted
    // the stats object is converted to text and comparison is performed.
    // When STRINGs are converted to other base types, NumberFormat exception
    // can occur in which case TruthValue.YES_NO_NULL value is returned
    try {
      Object baseObj = predicate.getLiteral(PredicateLeaf.FileFormat.ORC);
      Object minValue = getConvertedStatsObj(min, baseObj);
      Object maxValue = getConvertedStatsObj(max, baseObj);
      Object predObj = getBaseObjectForComparison(baseObj, minValue);

      result = evaluatePredicateMinMax(predicate, predObj, minValue, maxValue, hasNull);
      if (bloomFilter != null && result != TruthValue.NO_NULL && result != TruthValue.NO) {
        result = evaluatePredicateBloomFilter(predicate, predObj, bloomFilter, hasNull);
      }
      // in case failed conversion, return the default YES_NO_NULL truth value
    } catch (NumberFormatException nfe) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("NumberFormatException when type matching predicate object" +
            " and statistics object. Exception: " + ExceptionUtils.getStackTrace(nfe));
      }
      result = hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
    }
    return result;
  }

  private static TruthValue evaluatePredicateMinMax(PredicateLeaf predicate, Object predObj,
      Object minValue,
      Object maxValue,
      boolean hasNull) {
    Location loc;

    switch (predicate.getOperator()) {
      case NULL_SAFE_EQUALS:
        loc = compareToRange((Comparable) predObj, minValue, maxValue);
        if (loc == Location.BEFORE || loc == Location.AFTER) {
          return TruthValue.NO;
        } else {
          return TruthValue.YES_NO;
        }
      case EQUALS:
        loc = compareToRange((Comparable) predObj, minValue, maxValue);
        if (minValue.equals(maxValue) && loc == Location.MIN) {
          return hasNull ? TruthValue.YES_NULL : TruthValue.YES;
        } else if (loc == Location.BEFORE || loc == Location.AFTER) {
          return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
        } else {
          return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
        }
      case LESS_THAN:
        loc = compareToRange((Comparable) predObj, minValue, maxValue);
        if (loc == Location.AFTER) {
          return hasNull ? TruthValue.YES_NULL : TruthValue.YES;
        } else if (loc == Location.BEFORE || loc == Location.MIN) {
          return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
        } else {
          return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
        }
      case LESS_THAN_EQUALS:
        loc = compareToRange((Comparable) predObj, minValue, maxValue);
        if (loc == Location.AFTER || loc == Location.MAX) {
          return hasNull ? TruthValue.YES_NULL : TruthValue.YES;
        } else if (loc == Location.BEFORE) {
          return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
        } else {
          return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
        }
      case IN:
        if (minValue.equals(maxValue)) {
          // for a single value, look through to see if that value is in the
          // set
          for (Object arg : predicate.getLiteralList(PredicateLeaf.FileFormat.ORC)) {
            predObj = getBaseObjectForComparison(arg, minValue);
            loc = compareToRange((Comparable) predObj, minValue, maxValue);
            if (loc == Location.MIN) {
              return hasNull ? TruthValue.YES_NULL : TruthValue.YES;
            }
          }
          return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
        } else {
          // are all of the values outside of the range?
          for (Object arg : predicate.getLiteralList(PredicateLeaf.FileFormat.ORC)) {
            predObj = getBaseObjectForComparison(arg, minValue);
            loc = compareToRange((Comparable) predObj, minValue, maxValue);
            if (loc == Location.MIN || loc == Location.MIDDLE ||
                loc == Location.MAX) {
              return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
            }
          }
          return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
        }
      case BETWEEN:
        List<Object> args = predicate.getLiteralList(PredicateLeaf.FileFormat.ORC);
        Object predObj1 = getBaseObjectForComparison(args.get(0), minValue);

        loc = compareToRange((Comparable) predObj1, minValue, maxValue);
        if (loc == Location.BEFORE || loc == Location.MIN) {
          Object predObj2 = getBaseObjectForComparison(args.get(1), minValue);

          Location loc2 = compareToRange((Comparable) predObj2, minValue, maxValue);
          if (loc2 == Location.AFTER || loc2 == Location.MAX) {
            return hasNull ? TruthValue.YES_NULL : TruthValue.YES;
          } else if (loc2 == Location.BEFORE) {
            return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
          } else {
            return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
          }
        } else if (loc == Location.AFTER) {
          return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
        } else {
          return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
        }
      case IS_NULL:
        // min = null condition above handles the all-nulls YES case
        return hasNull ? TruthValue.YES_NO : TruthValue.NO;
      default:
        return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
    }
  }

  private static TruthValue evaluatePredicateBloomFilter(PredicateLeaf predicate, Object predObj,
      BloomFilter bloomFilter, boolean hasNull) {
    switch (predicate.getOperator()) {
      case NULL_SAFE_EQUALS:
        // null safe equals does not return *_NULL variant. So set hasNull to false
        return checkInBloomFilter(bloomFilter, predObj, false);
      case EQUALS:
        return checkInBloomFilter(bloomFilter, predObj, hasNull);
      case IN:
        for (Object arg : predicate.getLiteralList(PredicateLeaf.FileFormat.ORC)) {
          // if atleast one value in IN list exist in bloom filter, qualify the row group/stripe
          TruthValue result = checkInBloomFilter(bloomFilter, arg, hasNull);
          if (result == TruthValue.YES_NO_NULL || result == TruthValue.YES_NO) {
            return result;
          }
        }
        return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
      default:
        return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
    }
  }

  private static TruthValue checkInBloomFilter(BloomFilter bf, Object predObj, boolean hasNull) {
    TruthValue result = hasNull ? TruthValue.NO_NULL : TruthValue.NO;

    if (predObj instanceof Long) {
      if (bf.testLong(((Long) predObj).longValue())) {
        result = TruthValue.YES_NO_NULL;
      }
    } else if (predObj instanceof Double) {
      if (bf.testDouble(((Double) predObj).doubleValue())) {
        result = TruthValue.YES_NO_NULL;
      }
    } else if (predObj instanceof String || predObj instanceof Text ||
        predObj instanceof HiveDecimal || predObj instanceof BigDecimal) {
      if (bf.testString(predObj.toString())) {
        result = TruthValue.YES_NO_NULL;
      }
    } else if (predObj instanceof Date) {
      if (bf.testLong(DateWritable.dateToDays((Date) predObj))) {
        result = TruthValue.YES_NO_NULL;
      }
    } else if (predObj instanceof DateWritable) {
      if (bf.testLong(((DateWritable) predObj).getDays())) {
        result = TruthValue.YES_NO_NULL;
      }
    } else if (predObj instanceof Timestamp) {
      if (bf.testLong(((Timestamp) predObj).getTime())) {
        result = TruthValue.YES_NO_NULL;
      }
    } else if (predObj instanceof TimestampWritable) {
      if (bf.testLong(((TimestampWritable) predObj).getTimestamp().getTime())) {
        result = TruthValue.YES_NO_NULL;
      }
    } else {
      // if the predicate object is null and if hasNull says there are no nulls then return NO
      if (predObj == null && !hasNull) {
        result = TruthValue.NO;
      } else {
        result = TruthValue.YES_NO_NULL;
      }
    }

    if (result == TruthValue.YES_NO_NULL && !hasNull) {
      result = TruthValue.YES_NO;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Bloom filter evaluation: " + result.toString());
    }

    return result;
  }

  private static Object getBaseObjectForComparison(Object predObj, Object statsObj) {
    if (predObj != null) {
      if (predObj instanceof ExprNodeConstantDesc) {
        predObj = ((ExprNodeConstantDesc) predObj).getValue();
      }
      // following are implicitly convertible
      if (statsObj instanceof Long) {
        if (predObj instanceof Double) {
          return ((Double) predObj).longValue();
        } else if (predObj instanceof HiveDecimal) {
          return ((HiveDecimal) predObj).longValue();
        } else if (predObj instanceof String) {
          return Long.valueOf(predObj.toString());
        }
      } else if (statsObj instanceof Double) {
        if (predObj instanceof Long) {
          return ((Long) predObj).doubleValue();
        } else if (predObj instanceof HiveDecimal) {
          return ((HiveDecimal) predObj).doubleValue();
        } else if (predObj instanceof String) {
          return Double.valueOf(predObj.toString());
        }
      } else if (statsObj instanceof String) {
        return predObj.toString();
      } else if (statsObj instanceof HiveDecimal) {
        if (predObj instanceof Long) {
          return HiveDecimal.create(((Long) predObj));
        } else if (predObj instanceof Double) {
          return HiveDecimal.create(predObj.toString());
        } else if (predObj instanceof String) {
          return HiveDecimal.create(predObj.toString());
        } else if (predObj instanceof BigDecimal) {
          return HiveDecimal.create((BigDecimal)predObj);
        }
      }
    }
    return predObj;
  }

  private static Object getConvertedStatsObj(Object statsObj, Object predObj) {

    // converting between date and other types is not implicit, so convert to
    // text for comparison
    if (((predObj instanceof DateWritable) && !(statsObj instanceof DateWritable))
        || ((statsObj instanceof DateWritable) && !(predObj instanceof DateWritable))) {
      return StringUtils.stripEnd(statsObj.toString(), null);
    }

    if (statsObj instanceof String) {
      return StringUtils.stripEnd(statsObj.toString(), null);
    }
    return statsObj;
  }
  
  public static class SargApplier {
    private final SearchArgument sarg;
    private final List<PredicateLeaf> sargLeaves;
    private final int[] filterColumns;
    private final long rowIndexStride;
    private final OrcProto.BloomFilterIndex[] bloomFilterIndices;

    public SargApplier(SearchArgument sarg, String[] columnNames, long rowIndexStride,
        List<OrcProto.Type> types) {
      this.sarg = sarg;
      sargLeaves = sarg.getLeaves();
      filterColumns = mapSargColumns(sargLeaves, columnNames, 0);
      bloomFilterIndices = new OrcProto.BloomFilterIndex[types.size()];
      this.rowIndexStride = rowIndexStride;
    }

    /**
     * Pick the row groups that we need to load from the current stripe.
     *
     * @return an array with a boolean for each row group or null if all of the
     * row groups must be read.
     * @throws IOException
     */
    public boolean[] pickRowGroups(
        StripeInformation stripe, OrcProto.RowIndex[] indexes) throws IOException {
      long rowsInStripe = stripe.getNumberOfRows();
      int groupsInStripe = (int) ((rowsInStripe + rowIndexStride - 1) / rowIndexStride);
      boolean[] result = new boolean[groupsInStripe]; // TODO: avoid alloc?
      TruthValue[] leafValues = new TruthValue[sargLeaves.size()];
      for (int rowGroup = 0; rowGroup < result.length; ++rowGroup) {
        for (int pred = 0; pred < leafValues.length; ++pred) {
          if (filterColumns[pred] != -1) {
            OrcProto.ColumnStatistics stats =
                indexes[filterColumns[pred]].getEntry(rowGroup).getStatistics();
            OrcProto.BloomFilter bf = null;
            if (bloomFilterIndices[filterColumns[pred]] != null) {
              bf = bloomFilterIndices[filterColumns[pred]].getBloomFilter(rowGroup);
            }
            leafValues[pred] = evaluatePredicateProto(stats, sargLeaves.get(pred), bf);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Stats = " + stats);
              LOG.debug("Setting " + sargLeaves.get(pred) + " to " +
                  leafValues[pred]);
            }
          } else {
            // the column is a virtual column
            leafValues[pred] = TruthValue.YES_NO_NULL;
          }
        }
        result[rowGroup] = sarg.evaluate(leafValues).isNeeded();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Row group " + (rowIndexStride * rowGroup) + " to " +
              (rowIndexStride * (rowGroup + 1) - 1) + " is " +
              (result[rowGroup] ? "" : "not ") + "included.");
        }
      }

      // if we found something to skip, use the array. otherwise, return null.
      for (boolean b : result) {
        if (!b) {
          return result;
        }
      }
      return null;
    }
  }

  /**
   * Pick the row groups that we need to load from the current stripe.
   * @return an array with a boolean for each row group or null if all of the
   *    row groups must be read.
   * @throws IOException
   */
  protected boolean[] pickRowGroups() throws IOException {
    // if we don't have a sarg or indexes, we read everything
    if (sargApp == null) {
      return null;
    }
    readRowIndex(currentStripe, included);
    return sargApp.pickRowGroups(stripes.get(currentStripe), indexes);
  }

  private void clearStreams() throws IOException {
    // explicit close of all streams to de-ref ByteBuffers
    for(InStream is: streams.values()) {
      is.close();
    }
    if (bufferChunks != null) {
      if (zcr != null) {
        DiskRangeList range = bufferChunks;
        while (range != null) {
          if (range instanceof BufferChunk) {
            zcr.releaseBuffer(((BufferChunk)range).chunk);
          }
          range = range.next;
        }
      }
      bufferChunks = null;
    }
    streams.clear();
  }

  /**
   * Read the current stripe into memory.
   * @throws IOException
   */
  private void readStripe() throws IOException {
    StripeInformation stripe = beginReadStripe();
    includedRowGroups = pickRowGroups();

    // move forward to the first unskipped row
    if (includedRowGroups != null) {
      while (rowInStripe < rowCountInStripe &&
             !includedRowGroups[(int) (rowInStripe / rowIndexStride)]) {
        rowInStripe = Math.min(rowCountInStripe, rowInStripe + rowIndexStride);
      }
    }

    // if we haven't skipped the whole stripe, read the data
    if (rowInStripe < rowCountInStripe) {
      // if we aren't projecting columns or filtering rows, just read it all
      if (included == null && includedRowGroups == null) {
        readAllDataStreams(stripe);
      } else {
        readPartialDataStreams(stripe);
      }
      reader.startStripe(streams, stripeFooter.getColumnsList());
      // if we skipped the first row group, move the pointers forward
      if (rowInStripe != 0) {
        seekToRowEntry(reader, (int) (rowInStripe / rowIndexStride));
      }
    }
  }

  private StripeInformation beginReadStripe() throws IOException {
    StripeInformation stripe = stripes.get(currentStripe);
    stripeFooter = readStripeFooter(stripe);
    clearStreams();
    // setup the position in the stripe
    rowCountInStripe = stripe.getNumberOfRows();
    rowInStripe = 0;
    rowBaseInStripe = 0;
    for(int i=0; i < currentStripe; ++i) {
      rowBaseInStripe += stripes.get(i).getNumberOfRows();
    }
    // reset all of the indexes
    for(int i=0; i < indexes.length; ++i) {
      indexes[i] = null;
    }
    return stripe;
  }

  private void readAllDataStreams(StripeInformation stripe) throws IOException {
    long start = stripe.getIndexLength();
    long end = start + stripe.getDataLength();
    // explicitly trigger 1 big read
    DiskRangeList toRead = new DiskRangeList(start, end);
    if (this.cache != null) {
      toRead = cache.getFileData(fileId, toRead, stripe.getOffset());
    }
    bufferChunks = RecordReaderUtils.readDiskRanges(file, zcr, stripe.getOffset(), toRead, false);
    List<OrcProto.Stream> streamDescriptions = stripeFooter.getStreamsList();
    createStreams(
        streamDescriptions, bufferChunks, null, codec, bufferSize, streams, cache);
    // TODO: decompressed data from streams should be put in cache
  }

  /**
   * The sections of stripe that we have read.
   * This might not match diskRange - 1 disk range can be multiple buffer chunks, depending on DFS block boundaries.
   */
  public static class BufferChunk extends DiskRangeList {
    final ByteBuffer chunk;

    public BufferChunk(ByteBuffer chunk, long offset) {
      super(offset, offset + chunk.remaining());
      this.chunk = chunk;
    }

    @Override
    public boolean hasData() {
      return chunk != null;
    }

    @Override
    public final String toString() {
      boolean makesSense = chunk.remaining() == (end - offset);
      return "data range [" + offset + ", " + end + "), size: " + chunk.remaining()
          + (makesSense ? "" : "(!)") + " type: " + (chunk.isDirect() ? "direct" : "array-backed");
    }

    @Override
    public DiskRange slice(long offset, long end) {
      assert offset <= end && offset >= this.offset && end <= this.end;
      ByteBuffer sliceBuf = chunk.slice();
      int newPos = (int)(offset - this.offset);
      int newLimit = newPos + (int)(end - offset);
      // TODO: temporary
      try {
        sliceBuf.position(newPos);
        sliceBuf.limit(newLimit);
      } catch (Throwable t) {
        LOG.error("Failed to slice buffer chunk with range" + " [" + this.offset + ", " + this.end
            + "), position: " + chunk.position() + " limit: " + chunk.limit() + ", "
            + (chunk.isDirect() ? "direct" : "array") + "; to [" + offset + ", " + end + ") " + t.getClass());
        throw new RuntimeException(t);
      }
      return new BufferChunk(sliceBuf, offset);
    }

    @Override
    public ByteBuffer getData() {
      return chunk;
    }
  }

  public static class CacheChunk extends DiskRangeList {
    public LlapMemoryBuffer buffer;

    public CacheChunk(LlapMemoryBuffer buffer, long offset, long end) {
      super(offset, end);
      this.buffer = buffer;
    }

    @Override
    public boolean hasData() {
      return buffer != null;
    }

    @Override
    public ByteBuffer getData() {
      return buffer.byteBuffer;
    }

    @Override
    public String toString() {
      return "start: " + offset + " end: " + end + " cache buffer: " + buffer;
    }
  }

  /**
   * Plan the ranges of the file that we need to read given the list of
   * columns and row groups.
   * @param streamList the list of streams available
   * @param indexes the indexes that have been loaded
   * @param includedColumns which columns are needed
   * @param includedRowGroups which row groups are needed
   * @param isCompressed does the file have generic compression
   * @param encodings the encodings for each column
   * @param types the types of the columns
   * @param compressionSize the compression block size
   * @return the list of disk ranges that will be loaded
   */
  static DiskRangeList planReadPartialDataStreams
      (List<OrcProto.Stream> streamList,
       OrcProto.RowIndex[] indexes,
       boolean[] includedColumns,
       boolean[] includedRowGroups,
       boolean isCompressed,
       List<OrcProto.ColumnEncoding> encodings,
       List<OrcProto.Type> types,
       int compressionSize,
       boolean doMergeBuffers) {
    long offset = 0;
    // figure out which columns have a present stream
    boolean[] hasNull = RecordReaderUtils.findPresentStreamsByColumn(streamList, types);
    DiskRangeListCreateHelper list = new DiskRangeListCreateHelper();
    for (OrcProto.Stream stream : streamList) {
      long length = stream.getLength();
      int column = stream.getColumn();
      OrcProto.Stream.Kind streamKind = stream.getKind();
      // since stream kind is optional, first check if it exists
      if (stream.hasKind() &&
          (StreamName.getArea(streamKind) == StreamName.Area.DATA) &&
          includedColumns[column]) {
        // if we aren't filtering or it is a dictionary, load it.
        if (includedRowGroups == null
            || RecordReaderUtils.isDictionary(streamKind, encodings.get(column))) {
          RecordReaderUtils.addEntireStreamToRanges(offset, length, list, doMergeBuffers);
        } else {
          RecordReaderUtils.addRgFilteredStreamToRanges(stream, includedRowGroups,
              isCompressed, indexes[column], encodings.get(column), types.get(column),
              compressionSize, hasNull[column], offset, length, list, doMergeBuffers);
        }
      }
      offset += length;
    }
    return list.extract();
  }

  /**
   * Update the disk ranges to collapse adjacent or overlapping ranges. It
   * assumes that the ranges are sorted.
   * @param ranges the list of disk ranges to merge
   */
  static void mergeDiskRanges(DiskRangeList range) {
    while (range != null && range.next != null) {
      DiskRangeList next = range.next;
      if (RecordReaderUtils.overlap(range.offset, range.end, next.offset, next.end)) {
        range.offset = Math.min(range.offset, next.offset);
        range.end = Math.max(range.end, next.end);
        range.removeAfter();
      } else {
        range = next;
      }
    }
  }

  void createStreams(List<OrcProto.Stream> streamDescriptions,
                            DiskRangeList ranges,
                            boolean[] includeColumn,
                            CompressionCodec codec,
                            int bufferSize,
                            Map<StreamName, InStream> streams,
                            LowLevelCache cache) throws IOException {
    long streamOffset = 0;
    for (OrcProto.Stream streamDesc: streamDescriptions) {
      int column = streamDesc.getColumn();
      if ((includeColumn != null && !includeColumn[column]) ||
          streamDesc.hasKind() &&
          (StreamName.getArea(streamDesc.getKind()) != StreamName.Area.DATA)) {
        streamOffset += streamDesc.getLength();
        continue;
      }
      List<DiskRange> buffers = RecordReaderUtils.getStreamBuffers(
          ranges, streamOffset, streamDesc.getLength());
      StreamName name = new StreamName(column, streamDesc.getKind());
      streams.put(name, InStream.create(fileId, name.toString(), buffers,
          streamDesc.getLength(), codec, bufferSize, cache));
      streamOffset += streamDesc.getLength();
    }
  }

  private LowLevelCache cache = null;
  public void setCache(LowLevelCache cache) {
    this.cache = cache;
  }

  private void readPartialDataStreams(StripeInformation stripe) throws IOException {
    List<OrcProto.Stream> streamList = stripeFooter.getStreamsList();
    DiskRangeList toRead = planReadPartialDataStreams(streamList,
            indexes, included, includedRowGroups, codec != null,
            stripeFooter.getColumnsList(), types, bufferSize, true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("chunks = " + RecordReaderUtils.stringifyDiskRanges(toRead));
    }
    mergeDiskRanges(toRead);
    if (this.cache != null) {
      toRead = cache.getFileData(fileId, toRead, stripe.getOffset());
    }
    bufferChunks = RecordReaderUtils.readDiskRanges(file, zcr, stripe.getOffset(), toRead, false);
    if (LOG.isDebugEnabled()) {
      LOG.debug("merge = " + RecordReaderUtils.stringifyDiskRanges(bufferChunks));
    }

    createStreams(streamList, bufferChunks, included, codec, bufferSize, streams, cache);
  }

  @Override
  public boolean hasNext() throws IOException {
    return rowInStripe < rowCountInStripe;
  }

  /**
   * Read the next stripe until we find a row that we don't skip.
   * @throws IOException
   */
  private void advanceStripe() throws IOException {
    rowInStripe = rowCountInStripe;
    while (rowInStripe >= rowCountInStripe &&
        currentStripe < stripes.size() - 1) {
      currentStripe += 1;
      readStripe();
    }
  }

  /**
   * Skip over rows that we aren't selecting, so that the next row is
   * one that we will read.
   * @param nextRow the row we want to go to
   * @throws IOException
   */
  private boolean advanceToNextRow(
      TreeReader reader, long nextRow, boolean canAdvanceStripe) throws IOException {
    long nextRowInStripe = nextRow - rowBaseInStripe;
    // check for row skipping
    if (rowIndexStride != 0 &&
        includedRowGroups != null &&
        nextRowInStripe < rowCountInStripe) {
      int rowGroup = (int) (nextRowInStripe / rowIndexStride);
      if (!includedRowGroups[rowGroup]) {
        while (rowGroup < includedRowGroups.length && !includedRowGroups[rowGroup]) {
          rowGroup += 1;
        }
        if (rowGroup >= includedRowGroups.length) {
          if (canAdvanceStripe) {
            advanceStripe();
          }
          return canAdvanceStripe;
        }
        nextRowInStripe = Math.min(rowCountInStripe, rowGroup * rowIndexStride);
      }
    }
    if (nextRowInStripe >= rowCountInStripe) {
      if (canAdvanceStripe) {
        advanceStripe();
      }
      return canAdvanceStripe;
    }
    if (nextRowInStripe != rowInStripe) {
      if (rowIndexStride != 0) {
        int rowGroup = (int) (nextRowInStripe / rowIndexStride);
        seekToRowEntry(reader, rowGroup);
        reader.skipRows(nextRowInStripe - rowGroup * rowIndexStride);
      } else {
        reader.skipRows(nextRowInStripe - rowInStripe);
      }
      rowInStripe = nextRowInStripe;
    }
    return true;
  }

  @Override
  public Object next(Object previous) throws IOException {
    Object result = reader.next(previous);
    // find the next row
    rowInStripe += 1;
    advanceToNextRow(reader, rowInStripe + rowBaseInStripe, true);
    return result;
  }

  @Override
  public VectorizedRowBatch nextBatch(VectorizedRowBatch previous) throws IOException {
    VectorizedRowBatch result = null;
    if (rowInStripe >= rowCountInStripe) {
      currentStripe += 1;
      readStripe();
    }

    long batchSize = computeBatchSize(VectorizedRowBatch.DEFAULT_SIZE);

    rowInStripe += batchSize;
    if (previous == null) {
      ColumnVector[] cols = (ColumnVector[]) reader.nextVector(null, (int) batchSize);
      result = new VectorizedRowBatch(cols.length);
      result.cols = cols;
    } else {
      result = (VectorizedRowBatch) previous;
      result.selectedInUse = false;
      reader.nextVector(result.cols, (int) batchSize);
    }

    result.size = (int) batchSize;
    advanceToNextRow(reader, rowInStripe + rowBaseInStripe, true);
    return result;
  }

  private long computeBatchSize(long targetBatchSize) {
    long batchSize = 0;
    // In case of PPD, batch size should be aware of row group boundaries. If only a subset of row
    // groups are selected then marker position is set to the end of range (subset of row groups
    // within strip). Batch size computed out of marker position makes sure that batch size is
    // aware of row group boundary and will not cause overflow when reading rows
    // illustration of this case is here https://issues.apache.org/jira/browse/HIVE-6287
    if (rowIndexStride != 0 && includedRowGroups != null && rowInStripe < rowCountInStripe) {
      int startRowGroup = (int) (rowInStripe / rowIndexStride);
      if (!includedRowGroups[startRowGroup]) {
        while (startRowGroup < includedRowGroups.length && !includedRowGroups[startRowGroup]) {
          startRowGroup += 1;
        }
      }

      int endRowGroup = startRowGroup;
      while (endRowGroup < includedRowGroups.length && includedRowGroups[endRowGroup]) {
        endRowGroup += 1;
      }

      final long markerPosition = (endRowGroup * rowIndexStride) < rowCountInStripe ? (endRowGroup * rowIndexStride)
          : rowCountInStripe;
      batchSize = Math.min(targetBatchSize, (markerPosition - rowInStripe));

      if (LOG.isDebugEnabled() && batchSize < targetBatchSize) {
        LOG.debug("markerPosition: " + markerPosition + " batchSize: " + batchSize);
      }
    } else {
      batchSize = Math.min(targetBatchSize, (rowCountInStripe - rowInStripe));
    }
    return batchSize;
  }

  @Override
  public void close() throws IOException {
    clearStreams();
    pool.clear();
    file.close();
  }

  @Override
  public long getRowNumber() {
    return rowInStripe + rowBaseInStripe + firstRow;
  }

  /**
   * Return the fraction of rows that have been read from the selected.
   * section of the file
   * @return fraction between 0.0 and 1.0 of rows consumed
   */
  @Override
  public float getProgress() {
    return ((float) rowBaseInStripe + rowInStripe) / totalRowCount;
  }

  private int findStripe(long rowNumber) {
    for(int i=0; i < stripes.size(); i++) {
      StripeInformation stripe = stripes.get(i);
      if (stripe.getNumberOfRows() > rowNumber) {
        return i;
      }
      rowNumber -= stripe.getNumberOfRows();
    }
    throw new IllegalArgumentException("Seek after the end of reader range");
  }

  Index readRowIndex(int stripeIndex, boolean[] included) throws IOException {
    return readRowIndex(stripeIndex, included, null, null, null);
  }

  Index readRowIndex(int stripeIndex, boolean[] included, OrcProto.RowIndex[] indexes,
      OrcProto.BloomFilterIndex[] bloomFilterIndex, boolean[] sargColumns) throws IOException {
    StripeInformation stripe = stripes.get(stripeIndex);
    OrcProto.StripeFooter stripeFooter = null;
    // if this is the current stripe, use the cached objects.
    if (stripeIndex == currentStripe) {
      stripeFooter = this.stripeFooter;
      indexes = indexes == null ? this.indexes : indexes;
      bloomFilterIndex = bloomFilterIndex == null ? this.bloomFilterIndices : bloomFilterIndex;
    }
    return metadata.readRowIndex(stripe, stripeFooter, included, indexes, sargColumns,
        bloomFilterIndex);
  }

  private void seekToRowEntry(TreeReader reader, int rowEntry) throws IOException {
    PositionProvider[] index = new PositionProvider[indexes.length];
    for (int i = 0; i < indexes.length; ++i) {
      if (indexes[i] != null) {
        index[i] = new PositionProviderImpl(indexes[i].getEntry(rowEntry));
      }
    }
    reader.seek(index);
  }

  @Override
  public void seekToRow(long rowNumber) throws IOException {
    if (rowNumber < 0) {
      throw new IllegalArgumentException("Seek to a negative row number " +
          rowNumber);
    } else if (rowNumber < firstRow) {
      throw new IllegalArgumentException("Seek before reader range " +
          rowNumber);
    }
    // convert to our internal form (rows from the beginning of slice)
    rowNumber -= firstRow;

    // move to the right stripe
    int rightStripe = findStripe(rowNumber);
    if (rightStripe != currentStripe) {
      currentStripe = rightStripe;
      readStripe();
    }
    readRowIndex(currentStripe, included);

    // if we aren't to the right row yet, advance in the stripe.
    advanceToNextRow(reader, rowNumber, true);
  }
}
