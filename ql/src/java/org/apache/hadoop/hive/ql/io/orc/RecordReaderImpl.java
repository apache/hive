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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.DiskRange;
import org.apache.hadoop.hive.common.DiskRangeList;
import org.apache.hadoop.hive.common.DiskRangeList.DiskRangeListCreateHelper;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.filters.BloomFilterIO;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderUtils.ByteBufferAllocatorPool;
import org.apache.hadoop.hive.ql.io.orc.TreeReaderFactory.TreeReader;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.shims.HadoopShims.ZeroCopyReaderShim;
import org.apache.hadoop.io.Text;

class RecordReaderImpl implements RecordReader {

  static final Log LOG = LogFactory.getLog(RecordReaderImpl.class);
  private static final boolean isLogDebugEnabled = LOG.isDebugEnabled();

  private final Path path;
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
    this.path = path;
    this.file = fileSystem.open(path);
    this.codec = codec;
    this.types = types;
    this.bufferSize = bufferSize;
    this.included = options.getInclude();
    this.conf = conf;
    this.rowIndexStride = strideRate;
    this.metadata = new MetadataReader(file, codec, bufferSize, types.size());
    SearchArgument sarg = options.getSearchArgument();
    if (sarg != null && strideRate != 0) {
      sargApp = new SargApplier(
          sarg, options.getColumnNames(), strideRate, types, included.length);
    } else {
      sargApp = null;
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
    reader = RecordReaderFactory.createTreeReader(0, conf, types, included, skipCorrupt);
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
        return Boolean.TRUE;
      } else {
        return Boolean.FALSE;
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
        return Boolean.FALSE;
      } else {
        return Boolean.TRUE;
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
    BloomFilterIO bf = null;
    if (bloomFilter != null) {
      bf = new BloomFilterIO(bloomFilter);
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
      PredicateLeaf predicate, BloomFilterIO bloomFilter) {
    Object minValue = getMin(stats);
    Object maxValue = getMax(stats);
    return evaluatePredicateRange(predicate, minValue, maxValue, stats.hasNull(), bloomFilter);
  }

  static TruthValue evaluatePredicateRange(PredicateLeaf predicate, Object min,
      Object max, boolean hasNull, BloomFilterIO bloomFilter) {
    // if we didn't have any values, everything must have been null
    if (min == null) {
      if (predicate.getOperator() == PredicateLeaf.Operator.IS_NULL) {
        return TruthValue.YES;
      } else {
        return TruthValue.NULL;
      }
    }

    TruthValue result;
    try {
      // Predicate object and stats objects are converted to the type of the predicate object.
      Object baseObj = predicate.getLiteral();
      Object minValue = getBaseObjectForComparison(predicate.getType(), min);
      Object maxValue = getBaseObjectForComparison(predicate.getType(), max);
      Object predObj = getBaseObjectForComparison(predicate.getType(), baseObj);

      result = evaluatePredicateMinMax(predicate, predObj, minValue, maxValue, hasNull);
      if (bloomFilter != null && result != TruthValue.NO_NULL && result != TruthValue.NO) {
        result = evaluatePredicateBloomFilter(predicate, predObj, bloomFilter, hasNull);
      }
      // in case failed conversion, return the default YES_NO_NULL truth value
    } catch (Exception e) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Exception when evaluating predicate. Skipping ORC PPD." +
            " Exception: " + ExceptionUtils.getStackTrace(e));
      }
      if (predicate.getOperator().equals(PredicateLeaf.Operator.NULL_SAFE_EQUALS) || !hasNull) {
        result = TruthValue.YES_NO;
      } else {
        result = TruthValue.YES_NO_NULL;
      }
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
          for (Object arg : predicate.getLiteralList()) {
            predObj = getBaseObjectForComparison(predicate.getType(), arg);
            loc = compareToRange((Comparable) predObj, minValue, maxValue);
            if (loc == Location.MIN) {
              return hasNull ? TruthValue.YES_NULL : TruthValue.YES;
            }
          }
          return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
        } else {
          // are all of the values outside of the range?
          for (Object arg : predicate.getLiteralList()) {
            predObj = getBaseObjectForComparison(predicate.getType(), arg);
            loc = compareToRange((Comparable) predObj, minValue, maxValue);
            if (loc == Location.MIN || loc == Location.MIDDLE ||
                loc == Location.MAX) {
              return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
            }
          }
          return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
        }
      case BETWEEN:
        List<Object> args = predicate.getLiteralList();
        Object predObj1 = getBaseObjectForComparison(predicate.getType(), args.get(0));

        loc = compareToRange((Comparable) predObj1, minValue, maxValue);
        if (loc == Location.BEFORE || loc == Location.MIN) {
          Object predObj2 = getBaseObjectForComparison(predicate.getType(), args.get(1));

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

  private static TruthValue evaluatePredicateBloomFilter(PredicateLeaf predicate,
      final Object predObj, BloomFilterIO bloomFilter, boolean hasNull) {
    switch (predicate.getOperator()) {
      case NULL_SAFE_EQUALS:
        // null safe equals does not return *_NULL variant. So set hasNull to false
        return checkInBloomFilter(bloomFilter, predObj, false);
      case EQUALS:
        return checkInBloomFilter(bloomFilter, predObj, hasNull);
      case IN:
        for (Object arg : predicate.getLiteralList()) {
          // if atleast one value in IN list exist in bloom filter, qualify the row group/stripe
          Object predObjItem = getBaseObjectForComparison(predicate.getType(), arg);
          TruthValue result = checkInBloomFilter(bloomFilter, predObjItem, hasNull);
          if (result == TruthValue.YES_NO_NULL || result == TruthValue.YES_NO) {
            return result;
          }
        }
        return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
      default:
        return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
    }
  }

  private static TruthValue checkInBloomFilter(BloomFilterIO bf, Object predObj, boolean hasNull) {
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
    } else if (predObj instanceof Timestamp) {
      if (bf.testLong(((Timestamp) predObj).getTime())) {
        result = TruthValue.YES_NO_NULL;
      }
    } else if (predObj instanceof TimestampWritable) {
      if (bf.testLong(((TimestampWritable) predObj).getTimestamp().getTime())) {
        result = TruthValue.YES_NO_NULL;
      }
    } else if (predObj instanceof Date) {
      if (bf.testLong(DateWritable.dateToDays((Date) predObj))) {
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

  private static Object getBaseObjectForComparison(PredicateLeaf.Type type, Object obj) {
    if (obj != null) {
      if (obj instanceof ExprNodeConstantDesc) {
        obj = ((ExprNodeConstantDesc) obj).getValue();
      }
    } else {
      return null;
    }
    switch (type) {
      case BOOLEAN:
        if (obj instanceof Boolean) {
          return obj;
        } else {
          // will only be true if the string conversion yields "true", all other values are
          // considered false
          return Boolean.valueOf(obj.toString());
        }
      case DATE:
        if (obj instanceof Date) {
          return obj;
        } else if (obj instanceof String) {
          return Date.valueOf((String) obj);
        } else if (obj instanceof Timestamp) {
          return DateWritable.timeToDate(((Timestamp) obj).getTime() / 1000L);
        }
        // always string, but prevent the comparison to numbers (are they days/seconds/milliseconds?)
        break;
      case DECIMAL:
        if (obj instanceof Boolean) {
          return ((Boolean) obj).booleanValue() ? HiveDecimal.ONE : HiveDecimal.ZERO;
        } else if (obj instanceof Integer) {
          return HiveDecimal.create(((Integer) obj).intValue());
        } else if (obj instanceof Long) {
          return HiveDecimal.create(((Long) obj));
        } else if (obj instanceof Float || obj instanceof Double ||
            obj instanceof String) {
          return HiveDecimal.create(obj.toString());
        } else if (obj instanceof BigDecimal) {
          return HiveDecimal.create((BigDecimal) obj);
        } else if (obj instanceof HiveDecimal) {
          return obj;
        } else if (obj instanceof Timestamp) {
          return HiveDecimal.create(
              new Double(new TimestampWritable((Timestamp) obj).getDouble()).toString());
        }
        break;
      case FLOAT:
        if (obj instanceof Number) {
          // widening conversion
          return ((Number) obj).doubleValue();
        } else if (obj instanceof HiveDecimal) {
          return ((HiveDecimal) obj).doubleValue();
        } else if (obj instanceof String) {
          return Double.valueOf(obj.toString());
        } else if (obj instanceof Timestamp) {
          return new TimestampWritable((Timestamp)obj).getDouble();
        } else if (obj instanceof HiveDecimal) {
          return ((HiveDecimal) obj).doubleValue();
        } else if (obj instanceof BigDecimal) {
          return ((BigDecimal) obj).doubleValue();
        }
        break;
      case INTEGER:
        // fall through
      case LONG:
        if (obj instanceof Number) {
          // widening conversion
          return ((Number) obj).longValue();
        } else if (obj instanceof HiveDecimal) {
          return ((HiveDecimal) obj).longValue();
        } else if (obj instanceof String) {
          return Long.valueOf(obj.toString());
        }
        break;
      case STRING:
        if (obj != null) {
          return (obj.toString());
        }
        break;
      case TIMESTAMP:
        if (obj instanceof Timestamp) {
          return obj;
        } else if (obj instanceof Float) {
          return TimestampWritable.doubleToTimestamp(((Float) obj).doubleValue());
        } else if (obj instanceof Double) {
          return TimestampWritable.doubleToTimestamp(((Double) obj).doubleValue());
        } else if (obj instanceof HiveDecimal) {
          return TimestampWritable.decimalToTimestamp((HiveDecimal) obj);
        } else if (obj instanceof Date) {
          return new Timestamp(((Date) obj).getTime());
        }
        // float/double conversion to timestamp is interpreted as seconds whereas integer conversion
        // to timestamp is interpreted as milliseconds by default. The integer to timestamp casting
        // is also config driven. The filter operator changes its promotion based on config:
        // "int.timestamp.conversion.in.seconds". Disable PPD for integer cases.
        break;
      default:
        break;
    }

    throw new IllegalArgumentException(String.format(
        "ORC SARGS could not convert from %s to %s", obj == null ? "(null)" : obj.getClass()
            .getSimpleName(), type));
  }

  public static class SargApplier {
    private final SearchArgument sarg;
    private final List<PredicateLeaf> sargLeaves;
    private final int[] filterColumns;
    private final long rowIndexStride;
    // same as the above array, but indices are set to true
    private final boolean[] sargColumns;
    public SargApplier(SearchArgument sarg, String[] columnNames, long rowIndexStride,
        List<OrcProto.Type> types, int includedCount) {
      this.sarg = sarg;
      sargLeaves = sarg.getLeaves();
      filterColumns = mapSargColumns(sargLeaves, columnNames, 0);
      this.rowIndexStride = rowIndexStride;
      // included will not be null, row options will fill the array with trues if null
      sargColumns = new boolean[includedCount];
      for (int i : filterColumns) {
        // filter columns may have -1 as index which could be partition column in SARG.
        if (i > 0) {
          sargColumns[i] = true;
        }
      }
    }

    /**
     * Pick the row groups that we need to load from the current stripe.
     *
     * @return an array with a boolean for each row group or null if all of the
     * row groups must be read.
     * @throws IOException
     */
    public boolean[] pickRowGroups(
        StripeInformation stripe, OrcProto.RowIndex[] indexes,
        OrcProto.BloomFilterIndex[] bloomFilterIndices) throws IOException {
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
            if (bloomFilterIndices != null && bloomFilterIndices[filterColumns[pred]] != null) {
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
    readRowIndex(currentStripe, included, sargApp.sargColumns);
    return sargApp.pickRowGroups(stripes.get(currentStripe), indexes, bloomFilterIndices);
  }

  private void clearStreams() throws IOException {
    // explicit close of all streams to de-ref ByteBuffers
    for(InStream is: streams.values()) {
      is.close();
    }
    if (bufferChunks != null) {
      if (zcr != null) {
        for (DiskRangeList range = bufferChunks; range != null; range = range.next) {
          if (!(range instanceof BufferChunk)) continue;
          zcr.releaseBuffer(((BufferChunk)range).chunk);
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
      reader.startStripe(streams, stripeFooter);
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
    bufferChunks = RecordReaderUtils.readDiskRanges(file, zcr, stripe.getOffset(), toRead, false);
    List<OrcProto.Stream> streamDescriptions = stripeFooter.getStreamsList();
    createStreams(
        streamDescriptions, bufferChunks, null, codec, bufferSize, streams);
  }

  /**
   * The sections of stripe that we have read.
   * This might not match diskRange - 1 disk range can be multiple buffer chunks, depending on DFS block boundaries.
   */
  public static class BufferChunk extends DiskRangeList {
    final ByteBuffer chunk;

    BufferChunk(ByteBuffer chunk, long offset) {
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
    public DiskRange sliceAndShift(long offset, long end, long shiftBy) {
      assert offset <= end && offset >= this.offset && end <= this.end;
      assert offset + shiftBy >= 0;
      ByteBuffer sliceBuf = chunk.slice();
      int newPos = (int)(offset - this.offset);
      int newLimit = newPos + (int)(end - offset);
      try {
        sliceBuf.position(newPos);
        sliceBuf.limit(newLimit);
      } catch (Throwable t) {
        LOG.error("Failed to slice buffer chunk with range" + " [" + this.offset + ", " + this.end
            + "), position: " + chunk.position() + " limit: " + chunk.limit() + ", "
            + (chunk.isDirect() ? "direct" : "array") + "; to [" + offset + ", " + end + ") "
            + t.getClass());
        throw new RuntimeException(t);
      }
      return new BufferChunk(sliceBuf, offset + shiftBy);
    }

    @Override
    public ByteBuffer getData() {
      return chunk;
    }
  }

  /**
   * Plan the ranges of the file that we need to read given the list of
   * columns and row groups.
   * @param streamList the list of streams avaiable
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

  void createStreams(List<OrcProto.Stream> streamDescriptions,
                            DiskRangeList ranges,
                            boolean[] includeColumn,
                            CompressionCodec codec,
                            int bufferSize,
                            Map<StreamName, InStream> streams) throws IOException {
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
      streams.put(name, InStream.create(name.toString(), buffers,
          streamDesc.getLength(), codec, bufferSize));
      streamOffset += streamDesc.getLength();
    }
  }

  private void readPartialDataStreams(StripeInformation stripe) throws IOException {
    List<OrcProto.Stream> streamList = stripeFooter.getStreamsList();
    DiskRangeList toRead = planReadPartialDataStreams(streamList,
            indexes, included, includedRowGroups, codec != null,
            stripeFooter.getColumnsList(), types, bufferSize, true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("chunks = " + RecordReaderUtils.stringifyDiskRanges(toRead));
    }
    bufferChunks = RecordReaderUtils.readDiskRanges(file, zcr, stripe.getOffset(), toRead, false);
    if (LOG.isDebugEnabled()) {
      LOG.debug("merge = " + RecordReaderUtils.stringifyDiskRanges(bufferChunks));
    }

    createStreams(streamList, bufferChunks, included, codec, bufferSize, streams);
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
    try {
      final Object result = reader.next(previous);
      // find the next row
      rowInStripe += 1;
      advanceToNextRow(reader, rowInStripe + rowBaseInStripe, true);
      return result;
    } catch (IOException e) {
      // Rethrow exception with file name in log message
      throw new IOException("Error reading file: " + path, e);
    }
  }

  @Override
  public VectorizedRowBatch nextBatch(VectorizedRowBatch previous) throws IOException {
    try {
      final VectorizedRowBatch result;
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
    } catch (IOException e) {
      // Rethrow exception with file name in log message
      throw new IOException("Error reading file: " + path, e);
    }
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

      final long markerPosition =
          (endRowGroup * rowIndexStride) < rowCountInStripe ? (endRowGroup * rowIndexStride)
              : rowCountInStripe;
      batchSize = Math.min(targetBatchSize, (markerPosition - rowInStripe));

      if (isLogDebugEnabled && batchSize < targetBatchSize) {
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

  Index readRowIndex(
      int stripeIndex, boolean[] included, boolean[] sargColumns) throws IOException {
    return readRowIndex(stripeIndex, included, null, null, sargColumns);
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
      sargColumns = sargColumns == null ?
          (sargApp == null ? null : sargApp.sargColumns) : sargColumns;
    }
    return metadata.readRowIndex(
        stripe, stripeFooter, included, indexes, sargColumns, bloomFilterIndex);
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
    readRowIndex(currentStripe, included, sargApp == null ? null : sargApp.sargColumns);

    // if we aren't to the right row yet, advance in the stripe.
    advanceToNextRow(reader, rowNumber, true);
  }
}
