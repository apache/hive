/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.utils;

import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.columnstats.aggr.ColumnStatsAggregator;
import org.apache.hadoop.hive.metastore.columnstats.aggr.ColumnStatsAggregatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MetaStoreUtils {
  /** A fixed date format to be used for hive partition column values. */
  public static final ThreadLocal<DateFormat> PARTITION_DATE_FORMAT =
       new ThreadLocal<DateFormat>() {
    @Override
    protected DateFormat initialValue() {
      DateFormat val = new SimpleDateFormat("yyyy-MM-dd");
      val.setLenient(false); // Without this, 2020-20-20 becomes 2021-08-20.
      return val;
    }
  };
  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreUtils.class);

  /**
   * Catches exceptions that can't be handled and bundles them to MetaException
   *
   * @param e exception to wrap.
   * @throws MetaException wrapper for the exception
   */
  public static void logAndThrowMetaException(Exception e) throws MetaException {
    String exInfo = "Got exception: " + e.getClass().getName() + " "
        + e.getMessage();
    LOG.error(exInfo, e);
    LOG.error("Converting exception to MetaException");
    throw new MetaException(exInfo);
  }

  public static String encodeTableName(String name) {
    // The encoding method is simple, e.g., replace
    // all the special characters with the corresponding number in ASCII.
    // Note that unicode is not supported in table names. And we have explicit
    // checks for it.
    StringBuilder sb = new StringBuilder();
    for (char ch : name.toCharArray()) {
      if (Character.isLetterOrDigit(ch) || ch == '_') {
        sb.append(ch);
      } else {
        sb.append('-').append((int) ch).append('-');
      }
    }
    return sb.toString();
  }

  /**
   * convert Exception to MetaException, which sets the cause to such exception
   * @param e cause of the exception
   * @return  the MetaException with the specified exception as the cause
   */
  public static MetaException newMetaException(Exception e) {
    return newMetaException(e != null ? e.getMessage() : null, e);
  }

  /**
   * convert Exception to MetaException, which sets the cause to such exception
   * @param errorMessage  the error message for this MetaException
   * @param e             cause of the exception
   * @return  the MetaException with the specified exception as the cause
   */
  public static MetaException newMetaException(String errorMessage, Exception e) {
    MetaException metaException = new MetaException(errorMessage);
    if (e != null) {
      metaException.initCause(e);
    }
    return metaException;
  }

  /**
   * Helper function to transform Nulls to empty strings.
   */
  private static final com.google.common.base.Function<String,String> transFormNullsToEmptyString
      = new com.google.common.base.Function<String, String>() {
    @Override
    public java.lang.String apply(@Nullable java.lang.String string) {
      return org.apache.commons.lang.StringUtils.defaultString(string);
    }
  };
  /**
   * We have aneed to sanity-check the map before conversion from persisted objects to
   * metadata thrift objects because null values in maps will cause a NPE if we send
   * across thrift. Pruning is appropriate for most cases except for databases such as
   * Oracle where Empty strings are stored as nulls, in which case we need to handle that.
   * See HIVE-8485 for motivations for this.
   */
  public static Map<String,String> trimMapNulls(
      Map<String,String> dnMap, boolean retrieveMapNullsAsEmptyStrings){
    if (dnMap == null){
      return null;
    }
    // Must be deterministic order map - see HIVE-8707
    //   => we use Maps.newLinkedHashMap instead of Maps.newHashMap
    if (retrieveMapNullsAsEmptyStrings) {
      // convert any nulls present in map values to empty strings - this is done in the case
      // of backing dbs like oracle which persist empty strings as nulls.
      return Maps.newLinkedHashMap(Maps.transformValues(dnMap, transFormNullsToEmptyString));
    } else {
      // prune any nulls present in map values - this is the typical case.
      return Maps.newLinkedHashMap(Maps.filterValues(dnMap, Predicates.notNull()));
    }
  }


  // given a list of partStats, this function will give you an aggr stats
  public static List<ColumnStatisticsObj> aggrPartitionStats(List<ColumnStatistics> partStats,
                                                             String dbName, String tableName, List<String> partNames, List<String> colNames,
                                                             boolean useDensityFunctionForNDVEstimation, double ndvTuner)
      throws MetaException {
    // 1. group by the stats by colNames
    // map the colName to List<ColumnStatistics>
    Map<String, List<ColumnStatistics>> map = new HashMap<>();
    for (ColumnStatistics css : partStats) {
      List<ColumnStatisticsObj> objs = css.getStatsObj();
      for (ColumnStatisticsObj obj : objs) {
        List<ColumnStatisticsObj> singleObj = new ArrayList<>();
        singleObj.add(obj);
        ColumnStatistics singleCS = new ColumnStatistics(css.getStatsDesc(), singleObj);
        if (!map.containsKey(obj.getColName())) {
          map.put(obj.getColName(), new ArrayList<ColumnStatistics>());
        }
        map.get(obj.getColName()).add(singleCS);
      }
    }
    return MetaStoreUtils.aggrPartitionStats(map,dbName,tableName,partNames,colNames,useDensityFunctionForNDVEstimation, ndvTuner);
  }

  public static List<ColumnStatisticsObj> aggrPartitionStats(
      Map<String, List<ColumnStatistics>> map, String dbName, String tableName,
      final List<String> partNames, List<String> colNames,
      final boolean useDensityFunctionForNDVEstimation,final double ndvTuner) throws MetaException {
    List<ColumnStatisticsObj> colStats = new ArrayList<>();
    // 2. Aggregate stats for each column in a separate thread
    if (map.size()< 1) {
      //stats are absent in RDBMS
      LOG.debug("No stats data found for: dbName=" +dbName +" tblName=" + tableName +
          " partNames= " + partNames + " colNames=" + colNames );
      return colStats;
    }
    final ExecutorService pool = Executors.newFixedThreadPool(Math.min(map.size(), 16),
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("aggr-col-stats-%d").build());
    final List<Future<ColumnStatisticsObj>> futures = Lists.newLinkedList();

    long start = System.currentTimeMillis();
    for (final Map.Entry<String, List<ColumnStatistics>> entry : map.entrySet()) {
      futures.add(pool.submit(new Callable<ColumnStatisticsObj>() {
        @Override
        public ColumnStatisticsObj call() throws Exception {
          List<ColumnStatistics> css = entry.getValue();
          ColumnStatsAggregator aggregator = ColumnStatsAggregatorFactory.getColumnStatsAggregator(css
                  .iterator().next().getStatsObj().iterator().next().getStatsData().getSetField(),
              useDensityFunctionForNDVEstimation, ndvTuner);
          ColumnStatisticsObj statsObj = aggregator.aggregate(entry.getKey(), partNames, css);
          return statsObj;
        }}));
    }
    pool.shutdown();
    for (Future<ColumnStatisticsObj> future : futures) {
      try {
        colStats.add(future.get());
      } catch (InterruptedException | ExecutionException e) {
        pool.shutdownNow();
        LOG.debug(e.toString());
        throw new MetaException(e.toString());
      }
    }
    LOG.debug("Time for aggr col stats in seconds: {} Threads used: {}",
        ((System.currentTimeMillis() - (double)start))/1000, Math.min(map.size(), 16));
    return colStats;
  }

  public static double decimalToDouble(Decimal decimal) {
    return new BigDecimal(new BigInteger(decimal.getUnscaled()), decimal.getScale()).doubleValue();
  }
}
