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

package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

/**
 * HBaseStorageHandler provides a HiveStorageHandler implementation for
 * HBase.
 */
public class HBaseStorageHandler extends DefaultStorageHandler
  implements HiveStoragePredicateHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseStorageHandler.class);

  /** HBase-internal config by which input format receives snapshot name. */
  private static final String HBASE_SNAPSHOT_NAME_KEY = "hbase.TableSnapshotInputFormat.snapshot.name";
  /** HBase-internal config by which input format received restore dir before HBASE-11335. */
  private static final String HBASE_SNAPSHOT_TABLE_DIR_KEY = "hbase.TableSnapshotInputFormat.table.dir";
  /** HBase-internal config by which input format received restore dir after HBASE-11335. */
  private static final String HBASE_SNAPSHOT_RESTORE_DIR_KEY = "hbase.TableSnapshotInputFormat.restore.dir";
  private static final String[] HBASE_CACHE_KEYS = new String[] {
      /** HBase config by which a SlabCache is sized. From HBase [0.98.3, 1.0.0) */
      "hbase.offheapcache.percentage",
      /** HBase config by which a BucketCache is sized. */
      "hbase.bucketcache.size",
      /** HBase config by which the bucket cache implementation is chosen. From HBase 0.98.10+ */
      "hbase.bucketcache.ioengine",
      /** HBase config by which a BlockCache is sized. */
      "hfile.block.cache.size"
  };

  final static public String DEFAULT_PREFIX = "default.";

  //Check if the configure job properties is called from input
  // or output for setting asymmetric properties
  private boolean configureInputJobProps = true;

  private Configuration jobConf;
  private Configuration hbaseConf;

  @Override
  public Configuration getConf() {
    return hbaseConf;
  }

  public Configuration getJobConf() {
    return jobConf;
  }

  @Override
  public void setConf(Configuration conf) {
    jobConf = conf;
    hbaseConf = HBaseConfiguration.create(conf);
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    if (HiveConf.getVar(jobConf, HiveConf.ConfVars.HIVE_HBASE_SNAPSHOT_NAME) != null) {
      LOG.debug("Using TableSnapshotInputFormat");
      return HiveHBaseTableSnapshotInputFormat.class;
    }
    LOG.debug("Using HiveHBaseTableInputFormat");
    return HiveHBaseTableInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    if (isHBaseGenerateHFiles(jobConf)) {
      return HiveHFileOutputFormat.class;
    }
    return HiveHBaseTableOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return HBaseSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return new HBaseMetaHook(hbaseConf);
  }

  @Override
  public void configureInputJobProperties(
    TableDesc tableDesc,
    Map<String, String> jobProperties) {
    //Input
    this.configureInputJobProps = true;
    configureTableJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureOutputJobProperties(
    TableDesc tableDesc,
    Map<String, String> jobProperties) {
    //Output
    this.configureInputJobProps = false;
    configureTableJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureTableJobProperties(
    TableDesc tableDesc,
    Map<String, String> jobProperties) {

    Properties tableProperties = tableDesc.getProperties();

    jobProperties.put(
      HBaseSerDe.HBASE_COLUMNS_MAPPING,
      tableProperties.getProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING));
    jobProperties.put(HBaseSerDe.HBASE_COLUMNS_REGEX_MATCHING,
            tableProperties.getProperty(HBaseSerDe.HBASE_COLUMNS_REGEX_MATCHING, "true"));
    jobProperties.put(HBaseSerDe.HBASE_COLUMNS_PREFIX_HIDE,
            tableProperties.getProperty(HBaseSerDe.HBASE_COLUMNS_PREFIX_HIDE, "false"));
    jobProperties.put(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE,
      tableProperties.getProperty(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE,"string"));
    jobProperties.put(HBaseSerDe.HBASE_SCAN_CACHEBLOCKS, tableProperties
        .getProperty(HBaseSerDe.HBASE_SCAN_CACHEBLOCKS, "false"));
    String scanCache = tableProperties.getProperty(HBaseSerDe.HBASE_SCAN_CACHE);
    if (scanCache != null) {
      jobProperties.put(HBaseSerDe.HBASE_SCAN_CACHE, scanCache);
    }
    String scanBatch = tableProperties.getProperty(HBaseSerDe.HBASE_SCAN_BATCH);
    if (scanBatch != null) {
      jobProperties.put(HBaseSerDe.HBASE_SCAN_BATCH, scanBatch);
    }

    String tableName = tableProperties.getProperty(HBaseSerDe.HBASE_TABLE_NAME);
    if (tableName == null) {
      tableName = tableProperties.getProperty(hive_metastoreConstants.META_TABLE_NAME);
      tableName = tableName.toLowerCase();
      if (tableName.startsWith(DEFAULT_PREFIX)) {
        tableName = tableName.substring(DEFAULT_PREFIX.length());
      }
    }
    jobProperties.put(HBaseSerDe.HBASE_TABLE_NAME, tableName);

    Configuration jobConf = getJobConf();
    addHBaseResources(jobConf, jobProperties);

    // do this for reconciling HBaseStorageHandler for use in HCatalog
    // check to see if this an input job or an outputjob
    if (this.configureInputJobProps) {
      LOG.info("Configuring input job properties");
      String snapshotName = HiveConf.getVar(jobConf, HiveConf.ConfVars.HIVE_HBASE_SNAPSHOT_NAME);
      if (snapshotName != null) {
        HBaseTableSnapshotInputFormatUtil.assertSupportsTableSnapshots();

        try {
          String restoreDir =
            HiveConf.getVar(jobConf, HiveConf.ConfVars.HIVE_HBASE_SNAPSHOT_RESTORE_DIR);
          if (restoreDir == null) {
            throw new IllegalArgumentException(
              "Cannot process HBase snapshot without specifying " + HiveConf.ConfVars
                .HIVE_HBASE_SNAPSHOT_RESTORE_DIR);
          }

          HBaseTableSnapshotInputFormatUtil.configureJob(hbaseConf, snapshotName, new Path(restoreDir));
          // copy over configs touched by above method
          jobProperties.put(HBASE_SNAPSHOT_NAME_KEY, hbaseConf.get(HBASE_SNAPSHOT_NAME_KEY));
          if (hbaseConf.get(HBASE_SNAPSHOT_TABLE_DIR_KEY, null) != null) {
            jobProperties.put(HBASE_SNAPSHOT_TABLE_DIR_KEY, hbaseConf.get(HBASE_SNAPSHOT_TABLE_DIR_KEY));
          } else {
            jobProperties.put(HBASE_SNAPSHOT_RESTORE_DIR_KEY, hbaseConf.get(HBASE_SNAPSHOT_RESTORE_DIR_KEY));
          }

          TableMapReduceUtil.resetCacheConfig(hbaseConf);
          // copy over configs touched by above method
          for (String cacheKey : HBASE_CACHE_KEYS) {
            final String value = hbaseConf.get(cacheKey);
            if (value != null) {
              jobProperties.put(cacheKey, value);
            } else {
              jobProperties.remove(cacheKey);
            }
          }
        } catch (IOException e) {
          throw new IllegalArgumentException(e);
        }
      }

      for (String k : jobProperties.keySet()) {
        jobConf.set(k, jobProperties.get(k));
      }
      try {
        addHBaseDelegationToken(jobConf);
      } catch (IOException | MetaException e) {
        throw new IllegalStateException("Error while configuring input job properties", e);
      } //input job properties
    }
    else {
      LOG.info("Configuring output job properties");
      if (isHBaseGenerateHFiles(jobConf)) {
        // only support bulkload when a hfile.family.path has been specified.
        // TODO: support detecting cf's from column mapping
        // TODO: support loading into multiple CF's at a time
        String path = HiveHFileOutputFormat.getFamilyPath(jobConf, tableProperties);
        if (path == null || path.isEmpty()) {
          throw new RuntimeException("Please set " + HiveHFileOutputFormat.HFILE_FAMILY_PATH + " to target location for HFiles");
        }
        // TODO: should call HiveHFileOutputFormat#setOutputPath
        jobProperties.put("mapred.output.dir", path);
      } else {
        jobProperties.put(TableOutputFormat.OUTPUT_TABLE, tableName);
      }
    } // output job properties
  }

  /**
   * Return true when HBaseStorageHandler should generate hfiles instead of operate against the
   * online table. This mode is implicitly applied when "hive.hbase.generatehfiles" is true.
   */
  public static boolean isHBaseGenerateHFiles(Configuration conf) {
    return HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_HBASE_GENERATE_HFILES);
  }

  /**
   * Utility method to add hbase-default.xml and hbase-site.xml properties to a new map
   * if they are not already present in the jobConf.
   * @param jobConf Job configuration
   * @param newJobProperties  Map to which new properties should be added
   */
  private void addHBaseResources(Configuration jobConf,
      Map<String, String> newJobProperties) {
    Configuration conf = new Configuration(false);
    HBaseConfiguration.addHbaseResources(conf);
    for (Entry<String, String> entry : conf) {
      if (jobConf.get(entry.getKey()) == null) {
        newJobProperties.put(entry.getKey(), entry.getValue());
      }
    }
  }

  private void addHBaseDelegationToken(Configuration conf) throws IOException, MetaException {
    if (User.isHBaseSecurityEnabled(conf)) {
      Connection connection = ConnectionFactory.createConnection(hbaseConf);
      try {
        User curUser = User.getCurrent();
        Job job = new Job(conf);
        TokenUtil.addTokenForJob(connection, curUser, job);
      } catch (InterruptedException e) {
        throw new IOException("Error while obtaining hbase delegation token", e);
      } finally {
        if (connection != null) {
          connection.close();
        }
      }
    }
  }

  private static Class counterClass = null;
  static {
    try {
      counterClass = Class.forName("org.cliffc.high_scale_lib.Counter");
    } catch (ClassNotFoundException cnfe) {
      // this dependency is removed for HBase 1.0
    }
  }
  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    try {
      HBaseSerDe.configureJobConf(tableDesc, jobConf);
      /*
       * HIVE-6356
       * The following code change is only needed for hbase-0.96.0 due to HBASE-9165, and
       * will not be required once Hive bumps up its hbase version). At that time , we will
       * only need TableMapReduceUtil.addDependencyJars(jobConf) here.
       */
      if (counterClass != null) {
        TableMapReduceUtil.addDependencyJars(
          jobConf, HBaseStorageHandler.class, TableInputFormatBase.class, counterClass);
      } else {
        TableMapReduceUtil.addDependencyJars(
          jobConf, HBaseStorageHandler.class, TableInputFormatBase.class);
      }
      if (HiveConf.getVar(jobConf, HiveConf.ConfVars.HIVE_HBASE_SNAPSHOT_NAME) != null) {
        // There is an extra dependency on MetricsRegistry for snapshot IF.
        TableMapReduceUtil.addDependencyJars(jobConf, MetricRegistry.class);
      }

      Set<String> merged = new LinkedHashSet<String>(jobConf.getStringCollection("tmpjars"));

      Job copy = new Job(jobConf);
      TableMapReduceUtil.addDependencyJars(copy);
      merged.addAll(copy.getConfiguration().getStringCollection("tmpjars"));
      jobConf.set("tmpjars", StringUtils.arrayToString(merged.toArray(new String[0])));

      // Get credentials using the configuration instance which has HBase properties
      JobConf hbaseJobConf = new JobConf(getConf());
      org.apache.hadoop.hbase.mapred.TableMapReduceUtil.initCredentials(hbaseJobConf);
      ShimLoader.getHadoopShims().mergeCredentials(jobConf, hbaseJobConf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DecomposedPredicate decomposePredicate(
    JobConf jobConf,
    Deserializer deserializer,
    ExprNodeDesc predicate)
  {
    HBaseKeyFactory keyFactory = ((HBaseSerDe) deserializer).getKeyFactory();
    return keyFactory.decomposePredicate(jobConf, deserializer, predicate);
  }

  public static DecomposedPredicate decomposePredicate(
      JobConf jobConf,
      HBaseSerDe hBaseSerDe,
      ExprNodeDesc predicate) {
    ColumnMapping keyMapping = hBaseSerDe.getHBaseSerdeParam().getKeyColumnMapping();
    ColumnMapping tsMapping = hBaseSerDe.getHBaseSerdeParam().getTimestampColumnMapping();
    IndexPredicateAnalyzer analyzer = HiveHBaseTableInputFormat.newIndexPredicateAnalyzer(
        keyMapping.columnName, keyMapping.isComparable(),
        tsMapping == null ? null : tsMapping.columnName);
    List<IndexSearchCondition> conditions = new ArrayList<IndexSearchCondition>();
    ExprNodeGenericFuncDesc pushedPredicate = null;
    ExprNodeGenericFuncDesc residualPredicate =
        (ExprNodeGenericFuncDesc)analyzer.analyzePredicate(predicate, conditions);

    for (List<IndexSearchCondition> searchConditions:
        HiveHBaseInputFormatUtil.decompose(conditions).values()) {
      int scSize = searchConditions.size();
      if (scSize < 1 || 2 < scSize) {
        // Either there was nothing which could be pushed down (size = 0),
        // there were complex predicates which we don't support yet.
        // Currently supported are one of the form:
        // 1. key < 20                        (size = 1)
        // 2. key = 20                        (size = 1)
        // 3. key < 20 and key > 10           (size = 2)
        // Add to residual
        residualPredicate =
                extractResidualCondition(analyzer, searchConditions, residualPredicate);
        continue;
      }
      if (scSize == 2 &&
          (searchConditions.get(0).getComparisonOp().equals(GenericUDFOPEqual.class.getName()) ||
              searchConditions.get(1).getComparisonOp().equals(GenericUDFOPEqual.class.getName()))) {
        // If one of the predicates is =, then any other predicate with it is illegal.
        // Add to residual
        residualPredicate =
                extractResidualCondition(analyzer, searchConditions, residualPredicate);
        continue;
      }
      boolean sameType = sameTypeIndexSearchConditions(searchConditions);
      if (!sameType) {
        // If type for column and constant are different, we currently do not support pushing them
        residualPredicate =
                extractResidualCondition(analyzer, searchConditions, residualPredicate);
        continue;
      }
      TypeInfo typeInfo = searchConditions.get(0).getColumnDesc().getTypeInfo();
      if (typeInfo.getCategory() == Category.PRIMITIVE && PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
              ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) == PrimitiveGrouping.NUMERIC_GROUP) {
        // If the predicate is on a numeric column, and it specifies an
        // open range e.g. key < 20 , we do not support conversion, as negative
        // values are lexicographically stored after positive values and thus they
        // would be returned.
        if (scSize == 2) {
          boolean lowerBound = false;
          boolean upperBound = false;
          if (searchConditions.get(0).getComparisonOp().equals(GenericUDFOPEqualOrLessThan.class.getName()) ||
                searchConditions.get(0).getComparisonOp().equals(GenericUDFOPLessThan.class.getName())) {
            lowerBound = true;
          } else {
            upperBound = true;
          }
          if (searchConditions.get(1).getComparisonOp().equals(GenericUDFOPEqualOrGreaterThan.class.getName()) ||
                searchConditions.get(1).getComparisonOp().equals(GenericUDFOPGreaterThan.class.getName())) {
            upperBound = true;
          } else {
            lowerBound = true;
          }
          if (!upperBound || !lowerBound) {
            // Not valid range, add to residual
            residualPredicate =
                    extractResidualCondition(analyzer, searchConditions, residualPredicate);
            continue;
          }
        } else {
          // scSize == 1
          if (!searchConditions.get(0).getComparisonOp().equals(GenericUDFOPEqual.class.getName())) {
            // Not valid range, add to residual
            residualPredicate =
                    extractResidualCondition(analyzer, searchConditions, residualPredicate);
            continue;
          }
        }
      }

      // This one can be pushed
      pushedPredicate =
              extractStorageHandlerCondition(analyzer, searchConditions, pushedPredicate);
    }

    DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
    decomposedPredicate.pushedPredicate = pushedPredicate;
    decomposedPredicate.residualPredicate = residualPredicate;
    return decomposedPredicate;
  }

  private static ExprNodeGenericFuncDesc extractStorageHandlerCondition(IndexPredicateAnalyzer analyzer,
          List<IndexSearchCondition> searchConditions, ExprNodeGenericFuncDesc inputExpr) {
    if (inputExpr == null) {
      return analyzer.translateSearchConditions(searchConditions);
    }
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(analyzer.translateSearchConditions(searchConditions));
    children.add(inputExpr);
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
            FunctionRegistry.getGenericUDFForAnd(), children);
  }

  private static ExprNodeGenericFuncDesc extractResidualCondition(IndexPredicateAnalyzer analyzer,
          List<IndexSearchCondition> searchConditions, ExprNodeGenericFuncDesc inputExpr) {
    if (inputExpr == null) {
      return analyzer.translateOriginalConditions(searchConditions);
    }
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(analyzer.translateOriginalConditions(searchConditions));
    children.add(inputExpr);
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
            FunctionRegistry.getGenericUDFForAnd(), children);
  }

  private static boolean sameTypeIndexSearchConditions(List<IndexSearchCondition> searchConditions) {
    for (IndexSearchCondition isc : searchConditions) {
      if (!isc.getColumnDesc().getTypeInfo().equals(isc.getConstantDesc().getTypeInfo())) {
        return false;
      }
    }
    return true;
  }
}
