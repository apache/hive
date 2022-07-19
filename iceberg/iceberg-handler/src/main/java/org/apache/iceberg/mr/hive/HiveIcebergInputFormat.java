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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.hive.MetastoreUtil;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.mapred.AbstractMapredIcebergRecordReader;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.mr.mapred.MapredIcebergInputFormat;
import org.apache.iceberg.mr.mapreduce.IcebergInputFormat;
import org.apache.iceberg.mr.mapreduce.IcebergSplit;
import org.apache.iceberg.mr.mapreduce.IcebergSplitContainer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergInputFormat extends MapredIcebergInputFormat<Record>
    implements CombineHiveInputFormat.AvoidSplitCombination, VectorizedInputFormatInterface,
    LlapCacheOnlyInputFormatInterface.VectorizedOnly {

  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergInputFormat.class);
  private static final String HIVE_VECTORIZED_RECORDREADER_CLASS =
      "org.apache.iceberg.mr.hive.vector.HiveIcebergVectorizedRecordReader";
  private static final DynConstructors.Ctor<AbstractMapredIcebergRecordReader> HIVE_VECTORIZED_RECORDREADER_CTOR;

  static {
    if (MetastoreUtil.hive3PresentOnClasspath()) {
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
   * Converts the Hive filter found in the job conf to an Iceberg filter expression.
   * @param conf - job conf
   * @return - Iceberg data filter expression
   */
  static Expression icebergDataFilterFromHiveConf(Configuration conf) {
    Expression icebergFilter = SerializationUtil.deserializeFromBase64(conf.get(InputFormatConfig.FILTER_EXPRESSION));
    if (icebergFilter != null) {
      // in case we already have it prepared..
      return icebergFilter;
    }
    String hiveFilter = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (hiveFilter != null) {
      ExprNodeGenericFuncDesc exprNodeDesc = SerializationUtilities
          .deserializeObject(hiveFilter, ExprNodeGenericFuncDesc.class);
      SearchArgument sarg = ConvertAstToSearchArg.create(conf, exprNodeDesc);
      try {
        return HiveIcebergFilterFactory.generateFilterExpression(sarg);
      } catch (UnsupportedOperationException e) {
        LOG.warn("Unable to create Iceberg filter, continuing without filter (will be applied by Hive later): ", e);
      }
    }
    return null;
  }

  /**
   * Converts Hive filter found in the passed job conf to an Iceberg filter expression. Then evaluates this
   * against the task's partition value producing a residual filter expression.
   * @param task - file scan task to evaluate the expression against
   * @param conf - job conf
   * @return - Iceberg residual filter expression
   */
  public static Expression residualForTask(FileScanTask task, Configuration conf) {
    Expression dataFilter = icebergDataFilterFromHiveConf(conf);
    if (dataFilter == null) {
      return Expressions.alwaysTrue();
    }
    return ResidualEvaluator.of(
        task.spec(), dataFilter,
        conf.getBoolean(InputFormatConfig.CASE_SENSITIVE, InputFormatConfig.CASE_SENSITIVE_DEFAULT)
    ).residualFor(task.file().partition());
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    Expression filter = icebergDataFilterFromHiveConf(job);
    if (filter != null) {
      job.set(InputFormatConfig.FILTER_EXPRESSION, SerializationUtil.serializeToBase64(filter));
    }

    job.set(InputFormatConfig.SELECTED_COLUMNS, job.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, ""));
    job.setBoolean(InputFormatConfig.FETCH_VIRTUAL_COLUMNS,
            job.getBoolean(ColumnProjectionUtils.FETCH_VIRTUAL_COLUMNS_CONF_STR, false));
    job.set(InputFormatConfig.AS_OF_TIMESTAMP, job.get(TableScanDesc.AS_OF_TIMESTAMP, "-1"));
    job.set(InputFormatConfig.SNAPSHOT_ID, job.get(TableScanDesc.AS_OF_VERSION, "-1"));

    String location = job.get(InputFormatConfig.TABLE_LOCATION);
    return Arrays.stream(super.getSplits(job, numSplits))
                 .map(split -> new HiveIcebergSplit((IcebergSplit) split, location))
                 .toArray(InputSplit[]::new);
  }

  @Override
  public RecordReader<Void, Container<Record>> getRecordReader(InputSplit split, JobConf job,
                                                               Reporter reporter) throws IOException {
    job.set(InputFormatConfig.SELECTED_COLUMNS, job.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, ""));
    job.setBoolean(InputFormatConfig.FETCH_VIRTUAL_COLUMNS,
            job.getBoolean(ColumnProjectionUtils.FETCH_VIRTUAL_COLUMNS_CONF_STR, false));

    if (HiveConf.getBoolVar(job, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED) && Utilities.getIsVectorized(job)) {
      Preconditions.checkArgument(MetastoreUtil.hive3PresentOnClasspath(), "Vectorization only supported for Hive 3+");

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
    return true;
  }

  @Override
  public VectorizedSupport.Support[] getSupportedFeatures() {
    // disabling VectorizedSupport.Support.DECIMAL_64 as Parquet doesn't support it, and we have no way of telling
    // beforehand what kind of file format we're going to hit later
    return new VectorizedSupport.Support[]{  };
  }

  @Override
  public void injectCaches(FileMetadataCache metadataCache, DataCache dataCache, Configuration cacheConf) {
    // no-op for Iceberg
  }

}
