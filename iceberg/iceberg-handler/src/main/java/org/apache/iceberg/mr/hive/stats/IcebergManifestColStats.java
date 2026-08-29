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

package org.apache.iceberg.mr.hive.stats;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Timestamp;
import org.apache.hadoop.hive.metastore.api.TimestampColumnStatsData;
import org.apache.hadoop.hive.ql.parse.TransformSpec;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.mr.hive.SchemaUtils;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The column statistics a table's own manifests already hold. Every writer of every engine records
 * the bounds and the null count of each column in the file it writes, and a scan reads those
 * manifests to plan itself, so what they state costs nothing more to collect and describes the
 * table as it stands, with nothing to keep up to date.
 *
 * <p>They are bounds rather than measurements, and only some of them can be believed. A bound of a
 * string is stored truncated, so the ones read here are of the types Iceberg stores in full. A count
 * is a sum over the files read, so it holds only while every one of them stated it. Nothing here
 * knows how many values were distinct: no manifest has ever held that.
 */
public final class IcebergManifestColStats {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergManifestColStats.class);

  /** Named so that a thread dump tells which planning a compile is spending its time in. */
  private static final String COL_STATS_PLAN_POOL = "iceberg-colstats-plan-pool";
  private static final long MICROS_PER_SECOND = 1_000_000L;

  private IcebergManifestColStats() {
  }

  /**
   * What the manifests state of one column over the files of one partition. A null bound is one
   * the files did not state, or one of a type whose bounds are not stored in full.
   */
  public record ColumnBounds(int fieldId, Object min, Object max, long numNulls,
      boolean numNullsStated) {
  }

  /**
   * What the manifests state of the asked columns, and whether it accounts for every row a scan
   * returns. A delete file removes rows the data files still count, and nothing here reads one, so
   * what is stated then describes more rows than the scan reads - and none of it is served, since
   * a null count read as measured would answer for rows the scan will not return.
   */
  public record Stated(Map<Integer, ColumnBounds> columns, boolean accountsForEveryRow) {
  }

  /**
   * The bounds of the asked columns over the data files of the admitted partitions, taken
   * together: what a planner asks of the scan as a whole rather than of any partition of it.
   * Asking about one partition is asking with a filter that admits only it.
   */
  public static Stated readBounds(Table table, Snapshot snapshot,
      Predicate<String> partitions, Set<Integer> fieldIds) {
    return readBounds(table, snapshot, partitions, Expressions.alwaysTrue(), fieldIds);
  }

  /**
   * The same, told what the store may skip outright. The scan plans every file the expression
   * admits and the predicate then keeps the partitions actually asked about, so the expression
   * only narrows what is planned - one that admits too much costs work, never correctness.
   */
  public static Stated readBounds(Table table, Snapshot snapshot,
      Predicate<String> partitions, Expression partitionsFilter, Set<Integer> fieldIds) {
    // what the asked ids are, resolved once rather than again for every file the scan plans
    Map<Integer, Type> types = Maps.newLinkedHashMapWithExpectedSize(fieldIds.size());
    for (int fieldId : fieldIds) {
      Types.NestedField field = table.schema().findField(fieldId);
      if (field != null) {
        types.put(fieldId, field.type());
      }
    }

    Bounds bounds = new Bounds(types);
    // planning this scan reads the manifests, and it happens while a query compiles: on its own
    // pool so that it does not contend with the one split generation plans with
    ExecutorService planPool = ThreadPools.newFixedThreadPool(COL_STATS_PLAN_POOL, ThreadPools.WORKER_THREAD_POOL_SIZE);
    TableScan scan = table.newScan().useSnapshot(snapshot.snapshotId())
        .filter(partitionsFilter).includeColumnStats().planWith(planPool);
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        PartitionSpec spec = task.spec();
        String partName = IcebergTableUtil.toPartitionName(spec,
            IcebergTableUtil.toPartitionData(task.partition(), spec.partitionType()));
        if (partitions.test(partName)) {
          bounds.accept(task);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      planPool.shutdown();
    }

    Stated stated = bounds.stated();
    LOG.debug("Read the bounds of {} columns of {}", types.size(), table.name());
    return stated;
  }

  /**
   * What the store may skip when the fold is asked about these partitions. Where the spec
   * partitions a column by identity the partition names its value, so the files of every other
   * value need never be planned. A field of any other transform names what its values map to
   * rather than the values, and constrains nothing here - it is left out, which costs the walk
   * those files and nothing else. Anything unparsed leaves the whole expression admitting
   * everything, since narrowing on a guess is the one thing that could lose a partition.
   */
  private static Expression buildPartitionsFilter(Table table, List<String> partNames) {
    Map<String, PartitionField> byName = Maps.newHashMap();
    for (PartitionField field : table.spec().fields()) {
      byName.put(field.name(), field);
    }
    if (byName.isEmpty() || partNames.isEmpty()) {
      return Expressions.alwaysTrue();
    }
    Expression any = Expressions.alwaysFalse();
    for (String partName : partNames) {
      Expression one = buildPartitionPredicate(table, byName, partName);
      if (one == null) {
        // this partition constrains nothing, so neither can the whole expression
        return Expressions.alwaysTrue();
      }
      any = Expressions.or(any, one);
    }
    return any;
  }

  /**
   * What one partition's name says of the columns it is a partition of, or null where it says
   * nothing this can express. A field names what its values map to rather than the values, so the
   * term compares the transform of the column against what the name carries - which for identity
   * is the value itself. A transform this cannot build leaves the field unconstrained.
   */
  private static Expression buildPartitionPredicate(Table table, Map<String, PartitionField> byName, String partName) {
    Map<String, String> values;
    try {
      values = Warehouse.makeSpecFromName(partName);
    } catch (Exception e) {
      return null;
    }
    Expression all = null;
    for (Map.Entry<String, String> named : values.entrySet()) {
      Expression one = buildFieldPredicate(table, byName.get(named.getKey()), named.getValue());
      if (one != null) {
        all = all == null ? one : Expressions.and(all, one);
      }
    }
    return all;
  }

  /** What one field of a partition name says, as a predicate over the column it partitions. */
  private static Expression buildFieldPredicate(Table table, PartitionField field, String stated) {
    if (field == null) {
      return null;
    }
    Types.NestedField source = table.schema().findField(field.sourceId());
    if (source == null) {
      return null;
    }
    try {
      UnboundTerm<Object> term = SchemaUtils.toTerm(
          TransformSpec.fromString(field.transform().toString(), source.name()));
      if (term == null) {
        return null;
      }
      // a transform names its partition the way it renders it - a month as 2020-01, an hour as
      // 2020-01-01-00 - which the type the transform results in does not read back
      Object value = IcebergTableUtil.parsePartitionValue(
          field.transform().getResultType(source.type()), stated);
      return value == null ? Expressions.isNull(source.name()) : Expressions.equal(term, value);
    } catch (Exception e) {
      // a partition this cannot state constrains nothing: the scan plans wider and the names are
      // filtered as the files are read, which is all that admitting nothing here costs
      return null;
    }
  }

  /**
   * The columns a scan asks about, as the manifests of the partitions it reads state them. A
   * manifest never held a distinct count, so only a column something already states one for can be
   * built at all; what the manifests add is the bounds and the null count of the files the scan
   * actually reads, where what is stored describes every partition instead.
   */
  public static List<ColumnStatisticsObj> computeColStats(Table table, Snapshot snapshot,
      List<String> colNames, List<String> partNames) {
    if (snapshot == null) {
      return Collections.emptyList();
    }
    Map<Integer, Long> ndvs = IcebergStoredStats.readStatedNdvs(table, snapshot.snapshotId());
    Map<Integer, Types.NestedField> asked = Maps.newHashMap();
    for (String colName : colNames) {
      Types.NestedField field = table.schema().caseInsensitiveFindField(colName);
      if (field != null && ndvs.containsKey(field.fieldId())) {
        asked.put(field.fieldId(), field);
      }
    }
    if (asked.isEmpty()) {
      return Collections.emptyList();
    }
    Set<String> partitions = Sets.newHashSet(partNames);
    Stated read = readBounds(table, snapshot, partitions::contains, buildPartitionsFilter(table, partNames),
        asked.keySet());
    if (!read.accountsForEveryRow()) {
      // a delete file removes rows the data files still count, so the null count states more of
      // them than the scan returns, and a planner reading it as measured would fold a predicate
      // on those rows away
      return Collections.emptyList();
    }
    Map<Integer, ColumnBounds> stated = read.columns();

    List<ColumnStatisticsObj> statsObjs = Lists.newArrayList();
    stated.forEach((fieldId, bounds) -> {
      ColumnStatisticsObj statsObj = toColumnStats(
          asked.get(fieldId), bounds, OptionalLong.of(ndvs.get(fieldId)));
      if (statsObj != null) {
        statsObjs.add(statsObj);
      }
    });
    LOG.debug("Estimated {} of the {} columns asked of {} from what its manifests state",
        statsObjs.size(), colNames.size(), table.name());
    return statsObjs;
  }

  /**
   * The entry these bounds and that distinct count describe together. Neither side can stand on
   * its own: a manifest never held a distinct count, and a distinct count says nothing of how many
   * rows held no value, which an entry cannot be built without. Null when the column is of a type
   * whose bounds are not read, or when nothing states one of the two counts.
   */
  public static ColumnStatisticsObj toColumnStats(Types.NestedField field, ColumnBounds bounds,
      OptionalLong ndv) {
    if (!bounds.numNullsStated() || ndv.isEmpty()) {
      return null;
    }
    ColumnStatisticsData data = new ColumnStatisticsData();
    long numNulls = bounds.numNulls();
    long numDVs = ndv.getAsLong();

    switch (field.type().typeId()) {
      case INTEGER:
      case LONG:
        LongColumnStatsData longStats = new LongColumnStatsData(numNulls, numDVs);
        asLong(bounds.min()).ifPresent(longStats::setLowValue);
        asLong(bounds.max()).ifPresent(longStats::setHighValue);
        data.setLongStats(longStats);
        break;
      case FLOAT:
      case DOUBLE:
        DoubleColumnStatsData doubleStats = new DoubleColumnStatsData(numNulls, numDVs);
        asDouble(bounds.min()).ifPresent(doubleStats::setLowValue);
        asDouble(bounds.max()).ifPresent(doubleStats::setHighValue);
        data.setDoubleStats(doubleStats);
        break;
      case DATE:
        DateColumnStatsData dateStats = new DateColumnStatsData(numNulls, numDVs);
        asLong(bounds.min()).ifPresent(days -> dateStats.setLowValue(new Date(days)));
        asLong(bounds.max()).ifPresent(days -> dateStats.setHighValue(new Date(days)));
        data.setDateStats(dateStats);
        break;
      case TIMESTAMP:
        // every other producer pairs a zoned timestamp with long statistics, and the consumer that
        // reads one states no range from it, so an entry of this shape would only mislead
        if (((Types.TimestampType) field.type()).shouldAdjustToUTC()) {
          return null;
        }
        TimestampColumnStatsData timestampStats = new TimestampColumnStatsData(numNulls, numDVs);
        // Iceberg counts microseconds where the entry counts seconds, so each bound is rounded the
        // way that keeps it one: the low down and the high up, or a value of the second they were
        // truncated from would fall outside what the entry states
        asLong(bounds.min()).ifPresent(micros ->
            timestampStats.setLowValue(new Timestamp(Math.floorDiv(micros, MICROS_PER_SECOND))));
        asLong(bounds.max()).ifPresent(micros ->
            timestampStats.setHighValue(new Timestamp(Math.ceilDiv(micros, MICROS_PER_SECOND))));
        data.setTimestampStats(timestampStats);
        break;
      case DECIMAL:
        DecimalColumnStatsData decimalStats = new DecimalColumnStatsData(numNulls, numDVs);
        asDecimal(bounds.min()).ifPresent(decimalStats::setLowValue);
        asDecimal(bounds.max()).ifPresent(decimalStats::setHighValue);
        data.setDecimalStats(decimalStats);
        break;
      default:
        // a type whose bounds are not read states nothing here
        return null;
    }
    return new ColumnStatisticsObj(field.name(), HiveSchemaUtil.convert(field.type()).getTypeName(), data);
  }

  private static OptionalLong asLong(Object bound) {
    return bound instanceof Number number ? OptionalLong.of(number.longValue()) : OptionalLong.empty();
  }

  private static OptionalDouble asDouble(Object bound) {
    return bound instanceof Number number ? OptionalDouble.of(number.doubleValue()) : OptionalDouble.empty();
  }

  private static Optional<Decimal> asDecimal(Object bound) {
    if (!(bound instanceof BigDecimal decimal)) {
      return Optional.empty();
    }
    return Optional.of(new Decimal((short) decimal.scale(),
        ByteBuffer.wrap(decimal.unscaledValue().toByteArray())));
  }

  /** Whether Iceberg stores this type's bounds in full, rather than truncated to a prefix. */
  public static boolean hasFullBounds(Type type) {
    switch (type.typeId()) {
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case DECIMAL:
        return true;
      default:
        // a string or a binary is stored truncated: the bound holds, the value it came from does not
        return false;
    }
  }

  /**
   * The bounds the files of a scan state, folded a file at a time. A file states its metrics once
   * for all of its columns, so what holds them is read here, where a file is in hand, rather than
   * again by each column of it.
   */
  private static final class Bounds {

    private final Map<Integer, Type> types;
    private final Map<Integer, Accumulator> columns = Maps.newHashMap();
    /** Resolved once per spec a scan meets, since a table may have evolved through several. */
    private final Map<Integer, Map<Integer, Integer>> identityBySpec = Maps.newHashMap();
    private boolean accountsForEveryRow = true;

    private Bounds(Map<Integer, Type> types) {
      this.types = types;
    }

    private void accept(FileScanTask task) {
      // a delete file leaves the manifests counting rows the scan will not read
      if (!task.deletes().isEmpty()) {
        accountsForEveryRow = false;
      }
      DataFile file = task.file();
      long records = file.recordCount();
      Map<Integer, Long> nullCounts = file.nullValueCounts();
      Map<Integer, Long> nanCounts = file.nanValueCounts();
      Map<Integer, ByteBuffer> lowerBounds = file.lowerBounds();
      Map<Integer, ByteBuffer> upperBounds = file.upperBounds();
      PartitionSpec spec = task.spec();
      Map<Integer, Integer> identities = identityBySpec.computeIfAbsent(spec.specId(), id -> identitiesOf(spec));

      types.forEach((fieldId, type) -> {
        Accumulator accumulator = columns.computeIfAbsent(fieldId, id -> new Accumulator(type));
        Integer position = identities.get(fieldId);
        if (position == null) {
          accumulator.add(records, stated(nullCounts, fieldId), stated(nanCounts, fieldId),
              stated(lowerBounds, fieldId), stated(upperBounds, fieldId));
        } else {
          accumulator.add(records, task.partition().get(position, type.typeId().javaClass()));
        }
      });
    }

    /**
     * Where a column is one the spec partitions by identity, the position its value sits at in a
     * file's partition tuple. A file of such a partition holds that one value for the column
     * throughout, so its own metrics need not be consulted - and need not have been written.
     */
    private static Map<Integer, Integer> identitiesOf(PartitionSpec spec) {
      Map<Integer, Integer> positions = Maps.newHashMap();
      List<PartitionField> fields = spec.fields();
      for (int position = 0; position < fields.size(); position++) {
        PartitionField field = fields.get(position);
        // only identity carries the column's own value; bucket, truncate and the date transforms
        // state something the values map to, which bounds nothing on its own
        if (field.transform().isIdentity()) {
          positions.putIfAbsent(field.sourceId(), position);
        }
      }
      return positions;
    }

    /** What a file states of one column, where it stated anything of that kind at all. */
    private static <V> V stated(Map<Integer, V> byField, int fieldId) {
      return byField == null ? null : byField.get(fieldId);
    }

    private Stated stated() {
      Map<Integer, ColumnBounds> bounds = Maps.newHashMapWithExpectedSize(columns.size());
      columns.forEach((fieldId, accumulator) -> bounds.put(fieldId, accumulator.stated(fieldId)));
      return new Stated(bounds, accountsForEveryRow);
    }
  }

  /** Folds one column's files into the least and the greatest of what they state. */
  private static final class Accumulator {
    private final Type type;
    private final Comparator<Object> comparator;
    private final boolean full;
    private Object min;
    private Object max;
    private boolean boundsHold = true;
    private long numNulls;
    private boolean numNullsStated = true;

    private Accumulator(Type type) {
      this.type = type;
      this.full = hasFullBounds(type);
      this.comparator = full ? Comparators.forType(type.asPrimitiveType()) : null;
    }

    /** What one file stated of this column, of the metrics a bound is folded from. */
    private void add(long fileRecords, Long nulls, Long nans, ByteBuffer lowerBound, ByteBuffer upperBound) {
      // a file of no rows holds no value of the column, so it states nothing of one either way
      if (fileRecords == 0) {
        return;
      }
      if (nulls == null) {
        // a file that stated no count leaves the sum describing fewer rows than the scan reads
        numNullsStated = false;
      } else {
        numNulls += nulls;
      }
      if (!full || !boundsHold) {
        return;
      }
      // a value that is not a number cannot bound one, and Iceberg never stores it as a bound
      if (nans != null && nans > 0) {
        boundsHold = false;
        return;
      }
      Object lower = bound(lowerBound);
      Object upper = bound(upperBound);
      if (lower == null || upper == null) {
        // a file that stated no bound holds values of its own, unless it holds no value at all:
        // what the others state would then bound fewer rows than the scan reads
        boundsHold = nulls != null && nulls == fileRecords;
        return;
      }
      min = least(min, lower);
      max = greatest(max, upper);
    }

    /**
     * What a file of a partition the column is identity-partitioned by holds: that one value, for
     * every row of it. The count of nulls is exact either way - a null partition value means the
     * column is null throughout, and any other means it is null nowhere - and a file that states
     * no metrics of its own takes nothing away from what the others bound.
     */
    private void add(long fileRecords, Object partitionValue) {
      // a file of no rows holds no value of the column, so the partition it sits in bounds nothing
      if (fileRecords == 0) {
        return;
      }
      if (partitionValue == null) {
        numNulls += fileRecords;
        return;
      }
      if (!full || !boundsHold) {
        return;
      }
      // a value that is not a number cannot bound one, and it would win every comparison
      if (partitionValue instanceof Double asDouble && asDouble.isNaN() ||
          partitionValue instanceof Float asFloat && asFloat.isNaN()) {
        boundsHold = false;
        return;
      }
      min = least(min, partitionValue);
      max = greatest(max, partitionValue);
    }

    private Object bound(ByteBuffer stored) {
      return stored == null ? null : Conversions.fromByteBuffer(type, stored);
    }

    private Object least(Object held, Object stated) {
      return stated == null || held != null && comparator.compare(held, stated) <= 0 ? held : stated;
    }

    private Object greatest(Object held, Object stated) {
      return stated == null || held != null && comparator.compare(held, stated) >= 0 ? held : stated;
    }

    private ColumnBounds stated(int fieldId) {
      return new ColumnBounds(fieldId, boundsHold ? min : null, boundsHold ? max : null,
          numNulls, numNullsStated);
    }
  }
}
