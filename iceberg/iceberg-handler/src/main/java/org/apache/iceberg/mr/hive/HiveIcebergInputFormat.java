/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.FileMetadataCache;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSupport;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.LlapCacheOnlyInputFormatInterface;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.hive.HiveVersion;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.variant.VariantFilterRewriter;
import org.apache.iceberg.mr.mapred.AbstractMapredIcebergRecordReader;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.mr.mapred.MapredIcebergInputFormat;
import org.apache.iceberg.mr.mapreduce.IcebergInputFormat;
import org.apache.iceberg.mr.mapreduce.IcebergMergeSplit;
import org.apache.iceberg.mr.mapreduce.IcebergSplit;
import org.apache.iceberg.mr.mapreduce.IcebergSplitContainer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergInputFormat extends MapredIcebergInputFormat<Record>
    implements CombineHiveInputFormat.MergeSplits, VectorizedInputFormatInterface,
    LlapCacheOnlyInputFormatInterface.VectorizedOnly {

  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergInputFormat.class);
  private static final String HIVE_VECTORIZED_RECORDREADER_CLASS =
      "org.apache.iceberg.mr.hive.vector.HiveIcebergVectorizedRecordReader";
  private static final DynConstructors.Ctor<AbstractMapredIcebergRecordReader> HIVE_VECTORIZED_RECORDREADER_CTOR;
  public static final String ICEBERG_DISABLE_VECTORIZATION_PREFIX = "iceberg.disable.vectorization.";

  static {
    if (HiveVersion.min(HiveVersion.HIVE_3)) {
      HIVE_VECTORIZED_RECORDREADER_CTOR = DynConstructors.builder(AbstractMapredIcebergRecordReader.class)
          .impl(HIVE_VECTORIZED_RECORDREADER_CLASS,
              IcebergInputFormat.class,
              IcebergSplit.class,
              JobConf.class,
              Reporter.class)
          .build();
    } else {
      HIVE_VECTORIZED_RECORDREADER_CTOR = null;
    }
  }

  /**
   * Encapsulates planning-time and reader-time Iceberg filter expressions derived from Hive predicates.
   */
  private static final class FilterExpressions {

    private static Expression planningFilter(Configuration conf) {
      // Planning-safe filter (extract removed) may already be serialized for reuse.
      Expression planningFilter = SerializationUtil
          .deserializeFromBase64(conf.get(InputFormatConfig.FILTER_EXPRESSION));
      if (planningFilter != null) {
        // in case we already have it prepared..
        return planningFilter;
      }
      // Reader filter should retain extract(...) for row-group pruning. Rebuild from Hive predicate to avoid losing
      // variant rewrites when planningFilter was stripped.
      Expression readerFilter = icebergDataFilterFromHiveConf(conf);
      if (readerFilter != null) {
        return VariantFilterRewriter.stripVariantExtractPredicates(readerFilter);
      }
      return null;
    }

    private static Expression icebergDataFilterFromHiveConf(Configuration conf) {
      // Build an Iceberg filter from Hive's serialized predicate so we can preserve extract(...) terms for
      // reader-level pruning (e.g. Parquet shredded VARIANT row-group pruning).
      //
      // This intentionally does NOT consult FILTER_EXPRESSION, because FILTER_EXPRESSION must remain safe for
      // Iceberg planning-time utilities (some of which cannot stringify extract(...) terms).
      String hiveFilter = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
      if (hiveFilter != null) {
        ExprNodeGenericFuncDesc exprNodeDesc =
            SerializationUtilities.deserializeObject(hiveFilter, ExprNodeGenericFuncDesc.class);
        return getFilterExpr(conf, exprNodeDesc);
      }
      return null;
    }

    private static Expression planningResidual(FileScanTask task, Configuration conf) {
      return residual(task, conf, planningFilter(conf));
    }

    private static Expression readerResidual(FileScanTask task, Configuration conf) {
      return residual(task, conf, icebergDataFilterFromHiveConf(conf));
    }

    private static Expression residual(FileScanTask task, Configuration conf, Expression filter) {
      if (filter == null) {
        return Expressions.alwaysTrue();
      }
      boolean caseSensitive = conf.getBoolean(
          InputFormatConfig.CASE_SENSITIVE, InputFormatConfig.CASE_SENSITIVE_DEFAULT);

      return ResidualEvaluator.of(task.spec(), filter, caseSensitive)
          .residualFor(task.file().partition());
    }
  }

  /**
   * Builds an Iceberg filter expression from a Hive predicate expression node.
   * @param conf - job conf
   * @param exprNodeDesc - Describes a GenericFunc node
   * @return Iceberg Filter Expression
   */
  static Expression getFilterExpr(Configuration conf, ExprNodeGenericFuncDesc exprNodeDesc) {
    if (exprNodeDesc == null) {
      return null;
    }

    ExprNodeGenericFuncDesc exprForSarg = exprNodeDesc;
    if (Boolean.parseBoolean(conf.get(InputFormatConfig.VARIANT_SHREDDING_ENABLED))) {
      ExprNodeGenericFuncDesc rewritten = VariantFilterRewriter.rewriteForShredding(exprNodeDesc);
      if (rewritten != null) {
        exprForSarg = rewritten;
      }
    }

    SearchArgument sarg = ConvertAstToSearchArg.create(conf, exprForSarg);
    if (sarg == null) {
      return null;
    }

    try {
      return HiveIcebergFilterFactory.generateFilterExpression(sarg);
    } catch (UnsupportedOperationException e) {
      LOG.warn(
          "Unable to create Iceberg filter, proceeding without it (will be applied by Hive later): ",
          e);
      return null;
    }
  }

  /**
   * Returns a residual expression that is safe to apply as a record-level filter.
   *
   * <p>This residual is derived from the task-level Iceberg planning filter (already extract-free) after
   * evaluating it against the task's partition value.
   * @param task - file scan task to evaluate the expression against
   * @param conf - job conf
   * @return - Iceberg residual filter expression
   */
  public static Expression residualForTask(FileScanTask task, Configuration conf) {
    return FilterExpressions.planningResidual(task, conf);
  }

  /**
   * Returns a residual expression intended only for reader-level pruning (best-effort).
   *
   * <p>This residual is derived from the task-level Iceberg filter after evaluating it against the task's
   * partition value. It may include {@code extract(...)} predicates and is suitable for formats/readers that
   * can leverage such terms for pruning (e.g. Parquet row-group pruning using shredded VARIANT columns).
   *
   * <p><strong>Do not</strong> use this for record-level residual filtering, as {@code extract} cannot be
   * evaluated at record level in Iceberg readers.
   */
  public static Expression residualForReaderPruning(FileScanTask task, Configuration conf) {
    return FilterExpressions.readerResidual(task, conf);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    Expression filter = FilterExpressions.planningFilter(job);
    if (filter != null) {
      // Iceberg planning-time utilities may attempt to stringify the filter. Ensure the planning filter never
      // contains extract(...) or shredded typed_value references.
      job.set(InputFormatConfig.FILTER_EXPRESSION, SerializationUtil.serializeToBase64(filter));
    }

    job.set(InputFormatConfig.SELECTED_COLUMNS, job.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, ""));
    job.set(InputFormatConfig.GROUPING_PARTITION_COLUMNS, job.get(TableScanDesc.GROUPING_PARTITION_COLUMNS, ""));
    job.setBoolean(InputFormatConfig.FETCH_VIRTUAL_COLUMNS,
        job.getBoolean(ColumnProjectionUtils.FETCH_VIRTUAL_COLUMNS_CONF_STR, false));
    job.set(InputFormatConfig.AS_OF_TIMESTAMP, job.get(TableScanDesc.AS_OF_TIMESTAMP, "-1"));
    job.set(InputFormatConfig.SNAPSHOT_ID, job.get(TableScanDesc.AS_OF_VERSION, "-1"));
    job.set(InputFormatConfig.SNAPSHOT_ID_INTERVAL_FROM, job.get(TableScanDesc.FROM_VERSION, "-1"));
    job.set(InputFormatConfig.OUTPUT_TABLE_SNAPSHOT_REF, job.get(TableScanDesc.SNAPSHOT_REF, ""));

    String location = job.get(InputFormatConfig.TABLE_LOCATION);
    int numBuckets = job.getInt(TableScanDesc.GROUPING_NUM_BUCKETS, -1);
    return Arrays.stream(super.getSplits(job, numSplits))
       .map(split -> new HiveIcebergSplit((IcebergSplit) split, location, numBuckets))
       .toArray(InputSplit[]::new);
  }

  @Override
  public RecordReader<Void, Container<Record>> getRecordReader(InputSplit split, JobConf job,
                                                               Reporter reporter) throws IOException {
    job.set(InputFormatConfig.SELECTED_COLUMNS, job.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, ""));
    job.setBoolean(InputFormatConfig.FETCH_VIRTUAL_COLUMNS,
        job.getBoolean(ColumnProjectionUtils.FETCH_VIRTUAL_COLUMNS_CONF_STR, false));

    if (HiveConf.getBoolVar(job, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED) && Utilities.getIsVectorized(job)) {
      Preconditions.checkArgument(HiveVersion.min(HiveVersion.HIVE_3), "Vectorization only supported for Hive 3+");

      job.setEnum(InputFormatConfig.IN_MEMORY_DATA_MODEL, InputFormatConfig.InMemoryDataModel.HIVE);
      job.setBoolean(InputFormatConfig.SKIP_RESIDUAL_FILTERING, true);

      IcebergSplit icebergSplit = ((IcebergSplitContainer) split).icebergSplit();
      // bogus cast for favouring code reuse over syntax
      return (RecordReader) HIVE_VECTORIZED_RECORDREADER_CTOR.newInstance(
          new IcebergInputFormat<>(),
          icebergSplit,
          job,
          reporter);
    } else {
      return super.getRecordReader(split, job, reporter);
    }
  }

  @Override
  public boolean shouldSkipCombine(Path path, Configuration conf) {
    return false;
  }

  @Override
  public VectorizedSupport.Support[] getSupportedFeatures() {
    throw new UnsupportedOperationException("This overload of getSupportedFeatures should never be called");
  }

  @Override
  public VectorizedSupport.Support[] getSupportedFeatures(HiveConf hiveConf, TableDesc tableDesc) {
    // disabling VectorizedSupport.Support.DECIMAL_64 for Parquet as it doesn't support it
    boolean isORCOnly =
        Boolean.parseBoolean(tableDesc.getProperties().getProperty(HiveIcebergMetaHook.DECIMAL64_VECTORIZATION)) &&
            Boolean.parseBoolean(tableDesc.getProperties().getProperty(HiveIcebergMetaHook.ORC_FILES_ONLY)) &&
            org.apache.iceberg.FileFormat.ORC.name()
                .equalsIgnoreCase(tableDesc.getProperties().getProperty(TableProperties.DEFAULT_FILE_FORMAT));
    if (!isORCOnly) {
      final String vectorizationConfName = getVectorizationConfName(tableDesc.getTableName());
      LOG.debug("Setting {} for table: {} to true", vectorizationConfName, tableDesc.getTableName());
      hiveConf.set(vectorizationConfName, "true");
      return new VectorizedSupport.Support[] {};
    }
    return new VectorizedSupport.Support[] { VectorizedSupport.Support.DECIMAL_64 };
  }

  @Override
  public void injectCaches(FileMetadataCache metadataCache, DataCache dataCache, Configuration cacheConf) {
    // no-op for Iceberg
  }

  public static String getVectorizationConfName(String tableName) {
    String dbAndTableName = TableName.fromString(tableName, null, null).getNotEmptyDbTable();
    return ICEBERG_DISABLE_VECTORIZATION_PREFIX + dbAndTableName;
  }

  @Override
  public FileSplit createMergeSplit(CombineHiveInputFormat.CombineHiveInputSplit split,
        Integer partition, Properties properties) throws IOException {
    return new IcebergMergeSplit(split, partition, properties);
  }
}
