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

package org.apache.hadoop.hive.metastore;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import java.math.BigDecimal;
import java.util.*;

import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.COLNAME;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.COLTYPE;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.LONG_LOW_VALUE;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.LONG_HIGH_VALUE;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.DOUBLE_LOW_VALUE;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.DOUBLE_HIGH_VALUE;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.BIG_DECIMAL_LOW_VALUE;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.BIG_DECIMAL_HIGH_VALUE;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.NUM_NULLS;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.NUM_DISTINCTS;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.AVG_COL_LEN;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.MAX_COL_LEN;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.NUM_TRUES;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.NUM_FALSES;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.SUM_NDV_LONG;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.SUM_NDV_DOUBLE;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.SUM_NDV_DECIMAL;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.COUNT_ROWS;
import static org.apache.hadoop.hive.metastore.IExtrapolatePartStatus.DBStatsAggrIndices.SUM_NUM_DISTINCTS;
import static org.apache.hadoop.hive.metastore.MetastoreDirectSqlUtils.getFullyQualifiedName;
import static org.apache.hadoop.hive.metastore.MetastoreDirectSqlUtils.makeParams;
import static org.apache.hadoop.hive.metastore.MetastoreDirectSqlUtils.executeWithArray;
import static org.apache.hadoop.hive.metastore.MetastoreDirectSqlUtils.prepareParams;

class DirectSqlAggrStats {
    private static final int NO_BATCHING = -1, DETECT_BATCHING = 0;

    private static final Logger LOG = LoggerFactory.getLogger(DirectSqlAggrStats.class);
    private final PersistenceManager pm;
    private final int batchSize;
    private final DatabaseProduct dbType;

    @java.lang.annotation.Target(java.lang.annotation.ElementType.FIELD)
    @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
    private @interface TableName {
    }

    @TableName
    private String DBS, TBLS, PARTITIONS, PART_COL_STATS, TAB_COL_STATS;

    public DirectSqlAggrStats(PersistenceManager pm, Configuration conf, String schema) {
        this.pm = pm;
        this.dbType = PersistenceManagerProvider.getDatabaseProduct();
        int batchSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.DIRECT_SQL_PARTITION_BATCH_SIZE);
        if (batchSize == DETECT_BATCHING) {
            batchSize = dbType.needsInBatching() ? 1000 : NO_BATCHING;
        }
        this.batchSize = batchSize;
        ImmutableMap.Builder<String, String> fieldNameToTableNameBuilder =
                new ImmutableMap.Builder<>();

        for (java.lang.reflect.Field f : this.getClass().getDeclaredFields()) {
            if (f.getAnnotation(DirectSqlAggrStats.TableName.class) == null) {
                continue;
            }
            try {
                String value = getFullyQualifiedName(schema, f.getName());
                f.set(this, value);
                fieldNameToTableNameBuilder.put(f.getName(), value);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                throw new RuntimeException("Internal error, cannot set " + f.getName());
            }
        }
    }

    /**
     * Retrieve the column statistics for the specified columns of the table. NULL
     * is returned if the columns are not provided.
     * @param catName     the catalog name of the table
     * @param dbName      the database name of the table
     * @param tableName   the table name
     * @param colNames    the list of the column names
     * @param engine      engine making the request
     * @return            the column statistics for the specified columns
     * @throws MetaException
     */
    public ColumnStatistics getTableStats(final String catName, final String dbName,
                                          final String tableName, List<String> colNames, String engine,
                                          boolean enableBitVector, boolean enableKll) throws MetaException {
        if (colNames == null || colNames.isEmpty()) {
            return null;
        }
        final boolean doTrace = LOG.isDebugEnabled();
        final String queryText0 = "select " + getStatsList(enableBitVector, enableKll) + " from " + TAB_COL_STATS
                + " inner join " + TBLS + " on " + TAB_COL_STATS + ".\"TBL_ID\" = " + TBLS + ".\"TBL_ID\" "
                + " inner join " + DBS + " on " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" "
                + " where " + DBS + ".\"CTLG_NAME\" = ? and " + DBS + ".\"NAME\" = ? and " + TBLS + ".\"TBL_NAME\" = ?"
                + " and \"ENGINE\" = ? and \"COLUMN_NAME\" in (";
        Batchable<String, Object[]> b = new Batchable<String, Object[]>() {
            @Override
            public List<Object[]> run(List<String> input) throws MetaException {
                String queryText = queryText0 + makeParams(input.size()) + ")";
                Object[] params = new Object[input.size() + 4];
                params[0] = catName;
                params[1] = dbName;
                params[2] = tableName;
                params[3] = engine;
                for (int i = 0; i < input.size(); ++i) {
                    params[i + 4] = input.get(i);
                }
                long start = doTrace ? System.nanoTime() : 0;
                Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
                try {
                    Object qResult = executeWithArray(query, params, queryText);
                    MetastoreDirectSqlUtils.timingTrace(doTrace, queryText0 + "...)", start, (doTrace ? System.nanoTime() : 0));
                    if (qResult == null) {
                        return null;
                    }
                    return MetastoreDirectSqlUtils.ensureList(qResult);
                } finally {
                    addQueryAfterUse(query);
                }
            }
        };
        List<Object[]> list;
        try {
            list = Batchable.runBatched(batchSize, colNames, b);
            if (list != null) {
                list = new ArrayList<>(list);
            }
        } finally {
            b.closeAllQueries();
        }

        if (list == null || list.isEmpty()) {
            return null;
        }
        ColumnStatisticsDesc csd = new ColumnStatisticsDesc(true, dbName, tableName);
        csd.setCatName(catName);
        return makeColumnStats(list, csd, 0, engine);
    }

    public List<ColumnStatisticsObj> columnStatisticsObjForPartitions(
            final String catName, final String dbName, final String tableName, final List<String> partNames,
            List<String> colNames, String engine, long partsFound, final boolean useDensityFunctionForNDVEstimation,
            final double ndvTuner, final boolean enableBitVector, boolean enableKll) throws MetaException {
        final boolean areAllPartsFound = (partsFound == partNames.size());
        return Batchable.runBatched(batchSize, colNames, new Batchable<String, ColumnStatisticsObj>() {
            @Override
            public List<ColumnStatisticsObj> run(final List<String> inputColNames) throws MetaException {
                return columnStatisticsObjForPartitionsBatch(catName, dbName, tableName, partNames, inputColNames, engine,
                        areAllPartsFound, useDensityFunctionForNDVEstimation, ndvTuner, enableBitVector, enableKll);
            }
        });
    }

    /** Should be called with the list short enough to not trip up Oracle/etc. */
    private List<ColumnStatisticsObj> columnStatisticsObjForPartitionsBatch(String catName, String dbName,
                                                                            String tableName, List<String> partNames, List<String> colNames, String engine,
                                                                            boolean areAllPartsFound, boolean useDensityFunctionForNDVEstimation, double ndvTuner,
                                                                            boolean enableBitVector, boolean enableKll)
            throws MetaException {
        if (enableBitVector || enableKll) {
            return aggrStatsUseJava(catName, dbName, tableName, partNames, colNames, engine, areAllPartsFound,
                    useDensityFunctionForNDVEstimation, ndvTuner, enableBitVector, enableKll);
        } else {
            return aggrStatsUseDB(catName, dbName, tableName, partNames, colNames, engine,
                    useDensityFunctionForNDVEstimation, ndvTuner);
        }
    }

    private List<ColumnStatisticsObj> aggrStatsUseJava(String catName, String dbName, String tableName,
                                                       List<String> partNames, List<String> colNames, String engine, boolean areAllPartsFound,
                                                       boolean useDensityFunctionForNDVEstimation, double ndvTuner, boolean enableBitVector,
                                                       boolean enableKll) throws MetaException {
        // 1. get all the stats for colNames in partNames;
        List<ColumnStatistics> partStats =
                getPartitionStats(catName, dbName, tableName, partNames, colNames, engine, enableBitVector, enableKll);
        // 2. use util function to aggr stats
        return MetaStoreServerUtils.aggrPartitionStats(partStats, catName, dbName, tableName, partNames, colNames,
                areAllPartsFound, useDensityFunctionForNDVEstimation, ndvTuner);
    }

    private List<ColumnStatisticsObj> aggrStatsUseDB(String catName, String dbName, String tableName,
                                                     List<String> partNames, List<String> colNames, String engine,
                                                     boolean useDensityFunctionForNDVEstimation, double ndvTuner)
            throws MetaException {
        // TODO: all the extrapolation logic should be moved out of this class,
        // only mechanical data retrieval should remain here.
        String queryText = "select \"COLUMN_NAME\", \"COLUMN_TYPE\", "
                + "min(\"LONG_LOW_VALUE\"), max(\"LONG_HIGH_VALUE\"), min(\"DOUBLE_LOW_VALUE\"), max(\"DOUBLE_HIGH_VALUE\"), "
                + "min(cast(\"BIG_DECIMAL_LOW_VALUE\" as decimal)), max(cast(\"BIG_DECIMAL_HIGH_VALUE\" as decimal)), "
                + "sum(\"NUM_NULLS\"), max(\"NUM_DISTINCTS\"), "
                + "max(\"AVG_COL_LEN\"), max(\"MAX_COL_LEN\"), sum(\"NUM_TRUES\"), sum(\"NUM_FALSES\"), "
                // The following data is used to compute a partitioned table's NDV based
                // on partitions' NDV when useDensityFunctionForNDVEstimation = true. Global NDVs cannot be
                // accurately derived from partition NDVs, because the domain of column value two partitions
                // can overlap. If there is no overlap then global NDV is just the sum
                // of partition NDVs (UpperBound). But if there is some overlay then
                // global NDV can be anywhere between sum of partition NDVs (no overlap)
                // and same as one of the partition NDV (domain of column value in all other
                // partitions is subset of the domain value in one of the partition)
                // (LowerBound).But under uniform distribution, we can roughly estimate the global
                // NDV by leveraging the min/max values.
                // And, we also guarantee that the estimation makes sense by comparing it to the
                // UpperBound (calculated by "sum(\"NUM_DISTINCTS\")")
                // and LowerBound (calculated by "max(\"NUM_DISTINCTS\")")
                + "sum((\"LONG_HIGH_VALUE\"-\"LONG_LOW_VALUE\")/cast(\"NUM_DISTINCTS\" as decimal)),"
                + "sum((\"DOUBLE_HIGH_VALUE\"-\"DOUBLE_LOW_VALUE\")/\"NUM_DISTINCTS\"),"
                + "sum((cast(\"BIG_DECIMAL_HIGH_VALUE\" as decimal)-cast(\"BIG_DECIMAL_LOW_VALUE\" as decimal))/\"NUM_DISTINCTS\"),"
                + "count(1),"
                + "sum(\"NUM_DISTINCTS\")" + " from " + PART_COL_STATS + ""
                + " inner join " + PARTITIONS + " on " + PART_COL_STATS + ".\"PART_ID\" = " + PARTITIONS + ".\"PART_ID\""
                + " inner join " + TBLS + " on " + PARTITIONS + ".\"TBL_ID\" = " + TBLS + ".\"TBL_ID\""
                + " inner join " + DBS + " on " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\""
                + " where " + DBS + ".\"CTLG_NAME\" = ? and " + DBS + ".\"NAME\" = ? and " + TBLS + ".\"TBL_NAME\" = ? "
                + " and \"COLUMN_NAME\" in (%1$s)" + " and " + PARTITIONS + ".\"PART_NAME\" in (%2$s)"
                + " and \"ENGINE\" = ? " + " group by \"COLUMN_NAME\", \"COLUMN_TYPE\"";

        boolean doTrace = LOG.isDebugEnabled();

        List<ColumnStatisticsObj> colStats = new ArrayList<>(colNames.size());
        List<Object[]> partialStatsRows = new ArrayList<>(colNames.size());
        columnWiseStatsMerger(queryText, catName, dbName, tableName,
                colNames, partNames, colStats, partialStatsRows,
                engine, useDensityFunctionForNDVEstimation, ndvTuner, doTrace);

        // Extrapolation is needed for partialStatsRows.
        if (partialStatsRows.size() != 0) {
            Map<String, Integer> indexMap = new HashMap<String, Integer>();
            for (int index = 0; index < partNames.size(); index++) {
                indexMap.put(partNames.get(index), index);
            }

            for (Object[] row : partialStatsRows) {
                String colName = row[COLNAME.idx()].toString();
                String colType = row[COLTYPE.idx()].toString();
                BigDecimal countVal = new BigDecimal(row[COUNT_ROWS.idx()].toString());

                // use linear extrapolation. more complicated one can be added in the
                // future.
                IExtrapolatePartStatus extrapolateMethod = new LinearExtrapolatePartStatus();
                // fill in colstatus
                Integer[] index;
                boolean decimal = false;
                if (colType.toLowerCase().startsWith("decimal")) {
                    index = IExtrapolatePartStatus.indexMaps.get("decimal");
                    decimal = true;
                } else {
                    index = IExtrapolatePartStatus.indexMaps.get(colType.toLowerCase());
                }
                // if the colType is not the known type, long, double, etc, then get
                // all index.
                if (index == null) {
                    index = IExtrapolatePartStatus.indexMaps.get("default");
                }

                for (int colStatIndex : index) {
                    String colStatName = IExtrapolatePartStatus.colStatNames[colStatIndex];
                    // if the aggregation type is sum, we do a scale-up
                    if (IExtrapolatePartStatus.aggrTypes[colStatIndex] == IExtrapolatePartStatus.AggrType.Sum) {
                        // +3 only for the case of SUM_NUM_DISTINCTS which is after count rows index
                        int rowIndex = (colStatIndex == 15) ? colStatIndex + 3 : colStatIndex + 2;
                        if (row[rowIndex] != null) {
                            Long val = MetastoreDirectSqlUtils.extractSqlLong(row[rowIndex]);
                            row[rowIndex] = val / countVal.longValue() * (partNames.size());
                        }
                    } else if (IExtrapolatePartStatus.aggrTypes[colStatIndex] == IExtrapolatePartStatus.AggrType.Min ||
                            IExtrapolatePartStatus.aggrTypes[colStatIndex] == IExtrapolatePartStatus.AggrType.Max) {
                        // if the aggregation type is min/max, we extrapolate from the
                        // left/right borders
                        String orderByExpr = decimal ? "cast(\"" + colStatName + "\" as decimal)" : "\"" + colStatName + "\"";

                        queryText = "select \"" + colStatName + "\",\"PART_NAME\" from " + PART_COL_STATS
                                + " inner join " + PARTITIONS + " on " + PART_COL_STATS + ".\"PART_ID\" = " + PARTITIONS + ".\"PART_ID\""
                                + " inner join " + TBLS + " on " + PARTITIONS + ".\"TBL_ID\" = " + TBLS + ".\"TBL_ID\""
                                + " inner join " + DBS + " on " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\""
                                + " where " + DBS + ".\"CTLG_NAME\" = ? and " + DBS + ".\"NAME\" = ? and " + TBLS + ".\"TBL_NAME\" = ? "
                                + " and " + PART_COL_STATS + ".\"COLUMN_NAME\" in (%1$s)"
                                + " and " + PARTITIONS + ".\"PART_NAME\" in (%2$s)"
                                + " and " + PART_COL_STATS + ".\"ENGINE\" = ? "
                                + " order by " + orderByExpr;

                        Batchable<String, Object[]> columnWisePartitionBatches =
                                columnWisePartitionBatcher(queryText, catName, dbName, tableName, partNames, engine, doTrace);
                        try {
                            List<Object[]> list = Batchable.runBatched(batchSize, Collections.singletonList(colName), columnWisePartitionBatches);
                            Object[] min = list.getFirst();
                            Object[] max = list.getLast();
                            if (batchSize > 0) {
                                for (int i = Math.min(batchSize - 1, list.size() - 1); i < list.size(); i += batchSize) {
                                    Object[] posMax = list.get(i);
                                    if (new BigDecimal(max[0].toString()).compareTo(new BigDecimal(posMax[0].toString())) < 0) {
                                        max = posMax;
                                    }
                                    int j = i + 1;
                                    if (j < list.size()) {
                                        Object[] posMin = list.get(j);
                                        if (new BigDecimal(min[0].toString()).compareTo(new BigDecimal(posMin[0].toString())) > 0) {
                                            min = posMin;
                                        }
                                    }
                                }
                            }
                            if (min[0] == null || max[0] == null) {
                                row[2 + colStatIndex] = null;
                            } else {
                                row[2 + colStatIndex] = extrapolateMethod.extrapolate(min, max, colStatIndex, indexMap);
                            }
                        } finally {
                            columnWisePartitionBatches.closeAllQueries();
                        }
                    }
                }
                colStats.add(prepareCSObjWithAdjustedNDV
                        (row, useDensityFunctionForNDVEstimation, ndvTuner));
                Deadline.checkTimeout();
            }
        }
        return colStats;
    }

    private void columnWiseStatsMerger(
            final String queryText, final String catName, final String dbName,
            final String tableName, final List<String> colNames, final List<String> partNames,
            final List<ColumnStatisticsObj> colStats, final List<Object[]> partialStatsRows, final String engine,
            final boolean useDensityFunctionForNDVEstimation, final double ndvTuner,
            final boolean doTrace
    ) throws MetaException {
        Batchable<String, Object[]> columnWisePartitionBatches =
                columnWisePartitionBatcher(queryText, catName, dbName, tableName, partNames, engine, doTrace);
        try {
            List<Object[]> unmergedColStatslist = Batchable.runBatched(batchSize, colNames, columnWisePartitionBatches);
            Map<String, Object[]> mergedColStatsMap = new HashMap<>();
            for (Object[] unmergedRow : unmergedColStatslist) {
                String colName = (String) unmergedRow[0];
                Object[] mergedRow = mergedColStatsMap.getOrDefault(colName, new Object[21]);
                mergeBackendDBStats(mergedRow, unmergedRow);
                mergedColStatsMap.put(colName, mergedRow);
            }

            for (Map.Entry<String, Object[]> entry : mergedColStatsMap.entrySet()) {
                BigDecimal partCount = new BigDecimal(entry.getValue()[COUNT_ROWS.idx()].toString());
                if (partCount.equals(new BigDecimal(partNames.size())) || partCount.longValue() < 2) {
                    colStats.add(
                            prepareCSObjWithAdjustedNDV(entry.getValue(), useDensityFunctionForNDVEstimation, ndvTuner));
                } else {
                    partialStatsRows.add(entry.getValue());
                }
                Deadline.checkTimeout();
            }
        } finally {
            columnWisePartitionBatches.closeAllQueries();
        }
    }

    private void mergeBackendDBStats(Object[] row1, Object[] row2) {
        if (row1[COLNAME.idx()] == null) {
            row1[COLNAME.idx()] = row2[COLNAME.idx()];
            row1[COLTYPE.idx()] = row2[COLTYPE.idx()];
        }
        row1[LONG_LOW_VALUE.idx()] = MetastoreDirectSqlUtils.min(row1[LONG_LOW_VALUE.idx()], row2[LONG_LOW_VALUE.idx()]);
        row1[LONG_HIGH_VALUE.idx()] = MetastoreDirectSqlUtils.max(row1[LONG_HIGH_VALUE.idx()], row2[LONG_HIGH_VALUE.idx()]);
        row1[DOUBLE_LOW_VALUE.idx()] = MetastoreDirectSqlUtils.min(row1[DOUBLE_LOW_VALUE.idx()], row2[DOUBLE_LOW_VALUE.idx()]);
        row1[DOUBLE_HIGH_VALUE.idx()] = MetastoreDirectSqlUtils.max(row1[DOUBLE_HIGH_VALUE.idx()], row2[DOUBLE_HIGH_VALUE.idx()]);
        row1[BIG_DECIMAL_LOW_VALUE.idx()] = MetastoreDirectSqlUtils.min(row1[BIG_DECIMAL_LOW_VALUE.idx()], row2[BIG_DECIMAL_LOW_VALUE.idx()]);
        row1[BIG_DECIMAL_HIGH_VALUE.idx()] = MetastoreDirectSqlUtils.max(row1[BIG_DECIMAL_HIGH_VALUE.idx()], row2[BIG_DECIMAL_HIGH_VALUE.idx()]);
        row1[NUM_NULLS.idx()] = MetastoreDirectSqlUtils.sum(row1[NUM_NULLS.idx()], row2[NUM_NULLS.idx()]);
        row1[NUM_DISTINCTS.idx()] = MetastoreDirectSqlUtils.max(row1[NUM_DISTINCTS.idx()], row2[NUM_DISTINCTS.idx()]);
        row1[AVG_COL_LEN.idx()] = MetastoreDirectSqlUtils.max(row1[AVG_COL_LEN.idx()], row2[AVG_COL_LEN.idx()]);
        row1[MAX_COL_LEN.idx()] = MetastoreDirectSqlUtils.max(row1[MAX_COL_LEN.idx()], row2[MAX_COL_LEN.idx()]);
        row1[NUM_TRUES.idx()] = MetastoreDirectSqlUtils.sum(row1[NUM_TRUES.idx()], row2[NUM_TRUES.idx()]);
        row1[NUM_FALSES.idx()] = MetastoreDirectSqlUtils.sum(row1[NUM_FALSES.idx()], row2[NUM_FALSES.idx()]);
        row1[SUM_NDV_LONG.idx()] = MetastoreDirectSqlUtils.sum(row1[SUM_NDV_LONG.idx()], row2[SUM_NDV_LONG.idx()]);
        row1[SUM_NDV_DOUBLE.idx()] = MetastoreDirectSqlUtils.sum(row1[SUM_NDV_DOUBLE.idx()], row2[SUM_NDV_DOUBLE.idx()]);
        row1[SUM_NDV_DECIMAL.idx()] = MetastoreDirectSqlUtils.sum(row1[SUM_NDV_DECIMAL.idx()], row2[SUM_NDV_DECIMAL.idx()]);
        row1[COUNT_ROWS.idx()] = MetastoreDirectSqlUtils.sum(row1[COUNT_ROWS.idx()], row2[COUNT_ROWS.idx()]);
        row1[SUM_NUM_DISTINCTS.idx()] = MetastoreDirectSqlUtils.sum(row1[SUM_NUM_DISTINCTS.idx()], row2[SUM_NUM_DISTINCTS.idx()]);
    }

    private ColumnStatisticsObj prepareCSObjWithAdjustedNDV(
            Object[] row,
            boolean useDensityFunctionForNDVEstimation, double ndvTuner)
            throws MetaException {
        if (row == null) {
            return null;
        }
        ColumnStatisticsData data = new ColumnStatisticsData();
        ColumnStatisticsObj cso = new ColumnStatisticsObj((String) row[COLNAME.idx()], (String) row[COLTYPE.idx()], data);
        Object avgLong = MetastoreDirectSqlUtils.divide(row[SUM_NDV_LONG.idx()], row[COUNT_ROWS.idx()]);
        Object avgDouble = MetastoreDirectSqlUtils.divide(row[SUM_NDV_DOUBLE.idx()], row[COUNT_ROWS.idx()]);
        Object avgDecimal = MetastoreDirectSqlUtils.divide(row[SUM_NDV_DECIMAL.idx()], row[COUNT_ROWS.idx()]);
        StatObjectConverter.fillColumnStatisticsData(cso.getColType(), data, row[LONG_LOW_VALUE.idx()],
                row[LONG_HIGH_VALUE.idx()], row[DOUBLE_LOW_VALUE.idx()], row[DOUBLE_HIGH_VALUE.idx()], row[BIG_DECIMAL_LOW_VALUE.idx()], row[BIG_DECIMAL_HIGH_VALUE.idx()],
                row[NUM_NULLS.idx()], row[NUM_DISTINCTS.idx()], row[AVG_COL_LEN.idx()], row[MAX_COL_LEN.idx()], row[NUM_TRUES.idx()], row[NUM_FALSES.idx()],
                avgLong, avgDouble, avgDecimal, row[SUM_NUM_DISTINCTS.idx()],
                useDensityFunctionForNDVEstimation, ndvTuner);
        return cso;
    }

    private ColumnStatisticsObj prepareCSObj(Object[] row, int i) throws MetaException {
        ColumnStatisticsData data = new ColumnStatisticsData();
        ColumnStatisticsObj cso = new ColumnStatisticsObj((String)row[i++], (String)row[i++], data);
        Object llow = row[i++], lhigh = row[i++], dlow = row[i++], dhigh = row[i++],
                declow = row[i++], dechigh = row[i++], nulls = row[i++], dist = row[i++], bitVector = row[i++],
                histogram = row[i++], avglen = row[i++], maxlen = row[i++], trues = row[i++], falses = row[i];
        StatObjectConverter.fillColumnStatisticsData(cso.getColType(), data,
                llow, lhigh, dlow, dhigh, declow, dechigh, nulls, dist, bitVector, histogram, avglen, maxlen, trues, falses);
        return cso;
    }

    private Batchable<String, Object[]> columnWisePartitionBatcher(
            final String queryText0, final String catName, final String dbName,
            final String tableName, final List<String> partNames, final String engine,
            final boolean doTrace) {
        return new Batchable<String, Object[]>() {
            @Override
            public List<Object[]> run(final List<String> inputColNames)
                    throws MetaException {
                Batchable<String, Object[]> partitionBatchesFetcher = new Batchable<String, Object[]>() {
                    @Override
                    public List<Object[]> run(List<String> inputPartNames)
                            throws MetaException {
                        String queryText =
                                String.format(queryText0, makeParams(inputColNames.size()), makeParams(inputPartNames.size()));
                        long start = doTrace ? System.nanoTime() : 0;
                        Query<?> query = pm.newQuery("javax.jdo.query.SQL", queryText);
                        try {
                            Object qResult = executeWithArray(query,
                                    prepareParams(catName, dbName, tableName, inputPartNames, inputColNames, engine), queryText);
                            long end = doTrace ? System.nanoTime() : 0;
                            MetastoreDirectSqlUtils.timingTrace(doTrace, queryText0, start, end);
                            if (qResult == null) {
                                return Collections.emptyList();
                            }
                            return MetastoreDirectSqlUtils.ensureList(qResult);
                        } finally {
                            addQueryAfterUse(query);
                        }
                    }
                };
                try {
                    return Batchable.runBatched(batchSize, partNames, partitionBatchesFetcher);
                } finally {
                    addQueryAfterUse(partitionBatchesFetcher);
                }
            }
        };
    }

    public List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> getColStatsForAllTablePartitions(String catName, String dbName,
                                                                                                 boolean enableBitVector, boolean enableKll) throws MetaException {
        String queryText = "select \"TBLS\".\"TBL_NAME\", \"PARTITIONS\".\"PART_NAME\", "
                + getStatsList(enableBitVector, enableKll)
                + " from " + PART_COL_STATS
                + " inner join " + PARTITIONS + " on " + PART_COL_STATS + ".\"PART_ID\" = " + PARTITIONS + ".\"PART_ID\""
                + " inner join " + TBLS + " on " + PARTITIONS + ".\"TBL_ID\" = " + TBLS + ".\"TBL_ID\""
                + " inner join " + DBS + " on " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\""
                + " where " + DBS + ".\"NAME\" = ? and " + DBS + ".\"CTLG_NAME\" = ?";
        long start = 0;
        long end = 0;
        boolean doTrace = LOG.isDebugEnabled();
        Object qResult = null;
        start = doTrace ? System.nanoTime() : 0;
        List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> colStatsForDB = new ArrayList<MetaStoreServerUtils.ColStatsObjWithSourceInfo>();
        try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
            qResult = executeWithArray(query.getInnerQuery(), new Object[] { dbName, catName }, queryText);
            if (qResult == null) {
                return colStatsForDB;
            }
            end = doTrace ? System.nanoTime() : 0;
            MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, end);
            List<Object[]> list = MetastoreDirectSqlUtils.ensureList(qResult);
            for (Object[] row : list) {
                String tblName = (String) row[0];
                String partName = (String) row[1];
                ColumnStatisticsObj colStatObj = prepareCSObj(row, 2);
                colStatsForDB.add(new MetaStoreServerUtils.ColStatsObjWithSourceInfo(colStatObj, catName, dbName, tblName, partName));
                Deadline.checkTimeout();
            }
        }
        return colStatsForDB;
    }

    public List<ColumnStatistics> getPartitionStats(
            final String catName, final String dbName, final String tableName, final List<String> partNames,
            List<String> colNames, String engine, boolean enableBitVector, boolean enableKll) throws MetaException {
        if (colNames.isEmpty() || partNames.isEmpty()) {
            return Collections.emptyList();
        }
        final boolean doTrace = LOG.isDebugEnabled();
        final String queryText0 = "select \"PARTITIONS\".\"PART_NAME\", " + getStatsList(enableBitVector, enableKll)
                + " from " + PART_COL_STATS
                + " inner join " + PARTITIONS + " on " + PART_COL_STATS + ".\"PART_ID\" = " + PARTITIONS + ".\"PART_ID\""
                + " inner join " + TBLS + " on " + PARTITIONS + ".\"TBL_ID\" = " + TBLS + ".\"TBL_ID\""
                + " inner join " + DBS + " on " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\""
                + " where " + DBS + ".\"CTLG_NAME\" = ? and " + DBS + ".\"NAME\" = ? and " + TBLS + ".\"TBL_NAME\" = ? "
                + " and " + PART_COL_STATS + ".\"COLUMN_NAME\" in (%1$s)"
                + " and " + PARTITIONS + ".\"PART_NAME\" in (%2$s)"
                + " and " + PART_COL_STATS + ".\"ENGINE\" = ? "
                + " order by " + PARTITIONS +  ".\"PART_NAME\"";
        Batchable<String, Object[]> b =
                columnWisePartitionBatcher(queryText0, catName, dbName, tableName, partNames, engine, doTrace);
        List<ColumnStatistics> result = new ArrayList<ColumnStatistics>(partNames.size());
        String lastPartName = null;
        int from = 0;
        try {
            List<Object[]> list = Batchable.runBatched(batchSize, colNames, b);
            for (int i = 0; i <= list.size(); ++i) {
                boolean isLast = i == list.size();
                String partName = isLast ? null : (String) list.get(i)[0];
                if (!isLast && partName.equals(lastPartName)) {
                    continue;
                } else if (from != i) {
                    ColumnStatisticsDesc csd =
                            new ColumnStatisticsDesc(false, dbName, tableName);
                    csd.setCatName(catName);
                    csd.setPartName(lastPartName);
                    result.add(makeColumnStats(list.subList(from, i), csd, 1, engine));
                }
                lastPartName = partName;
                from = i;
                Deadline.checkTimeout();
            }
        } finally {
            b.closeAllQueries();
        }
        return result;
    }

    private ColumnStatistics makeColumnStats(
            List<Object[]> list, ColumnStatisticsDesc csd, int offset, String engine) throws MetaException {
        ColumnStatistics result = new ColumnStatistics();
        result.setStatsDesc(csd);
        List<ColumnStatisticsObj> csos = new ArrayList<ColumnStatisticsObj>(list.size());
        for (Object[] row : list) {
            // LastAnalyzed is stored per column but thrift has it per several;
            // get the lowest for now as nobody actually uses this field.
            Object laObj = row[offset + 16]; // 16 is the offset of "last analyzed" field
            if (laObj != null && (!csd.isSetLastAnalyzed() || csd.getLastAnalyzed() > MetastoreDirectSqlUtils
                    .extractSqlLong(laObj))) {
                csd.setLastAnalyzed(MetastoreDirectSqlUtils.extractSqlLong(laObj));
            }
            csos.add(prepareCSObj(row, offset));
            Deadline.checkTimeout();
        }
        result.setStatsObj(csos);
        result.setEngine(engine);
        return result;
    }

    /**
     * The common query part for table and partition stats
     */
    private String getStatsList(boolean enableBitVector, boolean enableKll) {
        return "\"COLUMN_NAME\", \"COLUMN_TYPE\", \"LONG_LOW_VALUE\", \"LONG_HIGH_VALUE\", "
                + "\"DOUBLE_LOW_VALUE\", \"DOUBLE_HIGH_VALUE\", \"BIG_DECIMAL_LOW_VALUE\", "
                + "\"BIG_DECIMAL_HIGH_VALUE\", \"NUM_NULLS\", \"NUM_DISTINCTS\", "
                + (enableBitVector ? "\"BIT_VECTOR\", " : "\'\', ")
                + (enableKll ? "\"HISTOGRAM\", " : "\'\', ")
                + "\"AVG_COL_LEN\", \"MAX_COL_LEN\", \"NUM_TRUES\", \"NUM_FALSES\", \"LAST_ANALYZED\" ";
    }
}
