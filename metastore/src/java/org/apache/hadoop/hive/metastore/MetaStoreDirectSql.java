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

package org.apache.hadoop.hive.metastore;

import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.repeat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.parser.FilterParser;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LeafNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LogicalOperator;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.Operator;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeVisitor;

/**
 * This class contains the optimizations for MetaStore that rely on direct SQL access to
 * the underlying database. It should use ANSI SQL and be compatible with common databases
 * such as MySQL (note that MySQL doesn't use full ANSI mode by default), Postgres, etc.
 *
 * As of now, only the partition retrieval is done this way to improve job startup time;
 * JDOQL partition retrieval is still present so as not to limit the ORM solution we have
 * to SQL stores only. There's always a way to do without direct SQL.
 */
class MetaStoreDirectSql {
  private static final Log LOG = LogFactory.getLog(MetaStoreDirectSql.class);

  private final PersistenceManager pm;

  public MetaStoreDirectSql(PersistenceManager pm) {
    this.pm = pm;
  }

  /**
   * Gets partitions by using direct SQL queries.
   * @param dbName Metastore db name.
   * @param tblName Metastore table name.
   * @param partNames Partition names to get.
   * @param max The maximum number of partitions to return.
   * @return List of partitions.
   */
  public List<Partition> getPartitionsViaSqlFilter(
      String dbName, String tblName, List<String> partNames, Integer max) throws MetaException {
    String list = repeat(",?", partNames.size()).substring(1);
    return getPartitionsViaSqlFilterInternal(dbName, tblName, null,
        "and PARTITIONS.PART_NAME in (" + list + ")", partNames, new ArrayList<String>(), max);
  }

  /**
   * Gets partitions by using direct SQL queries.
   * @param table The table.
   * @param parser The parsed filter from which the SQL filter will be generated.
   * @param max The maximum number of partitions to return.
   * @return List of partitions.
   */
  public List<Partition> getPartitionsViaSqlFilter(
      Table table, FilterParser parser, Integer max) throws MetaException {
    List<String> params = new ArrayList<String>(), joins = new ArrayList<String>();
    String sqlFilter = (parser == null) ? null
        : PartitionFilterGenerator.generateSqlFilter(table, parser.tree, params, joins);
    return getPartitionsViaSqlFilterInternal(table.getDbName(), table.getTableName(),
        isViewTable(table), sqlFilter, params, joins, max);
  }

  /**
   * Gets all partitions of a table by using direct SQL queries.
   * @param dbName Metastore db name.
   * @param tblName Metastore table name.
   * @param max The maximum number of partitions to return.
   * @return List of partitions.
   */
  public List<Partition> getPartitions(
      String dbName, String tblName, Integer max) throws MetaException {
    return getPartitionsViaSqlFilterInternal(dbName, tblName, null,
        null, new ArrayList<String>(), new ArrayList<String>(), max);
  }

  private static Boolean isViewTable(Table t) {
    return t.isSetTableType() ?
        t.getTableType().equals(TableType.VIRTUAL_VIEW.toString()) : null;
  }

  private boolean isViewTable(String dbName, String tblName) throws MetaException {
    String queryText = "select TBL_TYPE from TBLS" +
        " inner join DBS on TBLS.DB_ID = DBS.DB_ID " +
        " where TBLS.TBL_NAME = ? and DBS.NAME = ?";
    Object[] params = new Object[] { tblName, dbName };
    Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
    query.setUnique(true);
    Object result = query.executeWithArray(params);
    return (result != null) && result.toString().equals(TableType.VIRTUAL_VIEW.toString());
  }

  /**
   * Get partition objects for the query using direct SQL queries, to avoid bazillion
   * queries created by DN retrieving stuff for each object individually.
   * @param dbName Metastore db name.
   * @param tblName Metastore table name.
   * @param isView Whether table is a view. Can be passed as null if not immediately
   *               known, then this method will get it only if necessary.
   * @param sqlFilter SQL filter to use. Better be SQL92-compliant. Can be null.
   * @param paramsForFilter params for ?-s in SQL filter text. Params must be in order.
   * @param joinsForFilter if the filter needs additional join statement, they must be in
   *                       this list. Better be SQL92-compliant.
   * @param max The maximum number of partitions to return.
   * @return List of partition objects. FieldSchema is currently not populated.
   */
  private List<Partition> getPartitionsViaSqlFilterInternal(String dbName, String tblName,
      Boolean isView, String sqlFilter, List<String> paramsForFilter,
      List<String> joinsForFilter, Integer max) throws MetaException {
    boolean doTrace = LOG.isDebugEnabled();
    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();
    // We have to be mindful of order during filtering if we are not returning all partitions.
    String orderForFilter = (max != null) ? " order by PART_NAME asc" : "";

    // Get all simple fields for partitions and related objects, which we can map one-on-one.
    // We will do this in 2 queries to use different existing indices for each one.
    // We do not get table and DB name, assuming they are the same as we are using to filter.
    // TODO: We might want to tune the indexes instead. With current ones MySQL performs
    // poorly, esp. with 'order by' w/o index on large tables, even if the number of actual
    // results is small (query that returns 8 out of 32k partitions can go 4sec. to 0sec. by
    // just adding a PART_ID IN (...) filter that doesn't alter the results to it, probably
    // causing it to not sort the entire table due to not knowing how selective the filter is.
    String queryText =
        "select PARTITIONS.PART_ID from PARTITIONS"
      + "  inner join TBLS on PARTITIONS.TBL_ID = TBLS.TBL_ID "
      + "  inner join DBS on TBLS.DB_ID = DBS.DB_ID "
      + join(joinsForFilter, ' ') + " where TBLS.TBL_NAME = ? and DBS.NAME = ?"
      + ((sqlFilter == null) ? "" : " " + sqlFilter) + orderForFilter;
    Object[] params = new Object[paramsForFilter.size() + 2];
    params[0] = tblName;
    params[1] = dbName;
    for (int i = 0; i < paramsForFilter.size(); ++i) {
      params[i + 2] = paramsForFilter.get(i);
    }

    long start = doTrace ? System.nanoTime() : 0;
    Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
    if (max != null) {
      query.setRange(0, max.shortValue());
    }
    @SuppressWarnings("unchecked")
    List<Object> sqlResult = (List<Object>)query.executeWithArray(params);
    long queryTime = doTrace ? System.nanoTime() : 0;
    if (sqlResult.isEmpty()) {
      timingTrace(doTrace, queryText, start, queryTime);
      return new ArrayList<Partition>(); // no partitions, bail early.
    }

    // Prepare StringBuilder for "PART_ID in (...)" to use in future queries.
    int sbCapacity = sqlResult.size() * 7; // if there are 100k things => 6 chars, plus comma
    StringBuilder partSb = new StringBuilder(sbCapacity);
    // Assume db and table names are the same for all partition, that's what we're selecting for.
    for (Object partitionId : sqlResult) {
      partSb.append((Long)partitionId).append(",");
    }
    String partIds = trimCommaList(partSb);
    timingTrace(doTrace, queryText, start, queryTime);

    // Now get most of the other fields.
    queryText =
      "select PARTITIONS.PART_ID, SDS.SD_ID, SDS.CD_ID, SERDES.SERDE_ID, "
    + "  PARTITIONS.CREATE_TIME, PARTITIONS.LAST_ACCESS_TIME, SDS.INPUT_FORMAT, "
    + "  SDS.IS_COMPRESSED, SDS.IS_STOREDASSUBDIRECTORIES, SDS.LOCATION,  SDS.NUM_BUCKETS, "
    + "  SDS.OUTPUT_FORMAT, SERDES.NAME, SERDES.SLIB "
    + "from PARTITIONS"
    + "  left outer join SDS on PARTITIONS.SD_ID = SDS.SD_ID "
    + "  left outer join SERDES on SDS.SERDE_ID = SERDES.SERDE_ID "
    + "where PART_ID in (" + partIds + ") order by PART_NAME asc";
    start = doTrace ? System.nanoTime() : 0;
    query = pm.newQuery("javax.jdo.query.SQL", queryText);
    @SuppressWarnings("unchecked")
    List<Object[]> sqlResult2 = (List<Object[]>)query.executeWithArray(params);
    queryTime = doTrace ? System.nanoTime() : 0;

    // Read all the fields and create partitions, SDs and serdes.
    TreeMap<Long, Partition> partitions = new TreeMap<Long, Partition>();
    TreeMap<Long, StorageDescriptor> sds = new TreeMap<Long, StorageDescriptor>();
    TreeMap<Long, SerDeInfo> serdes = new TreeMap<Long, SerDeInfo>();
    TreeMap<Long, List<FieldSchema>> colss = new TreeMap<Long, List<FieldSchema>>();
    // Keep order by name, consistent with JDO.
    ArrayList<Partition> orderedResult = new ArrayList<Partition>(sqlResult.size());

    // Prepare StringBuilder-s for "in (...)" lists to use in one-to-many queries.
    StringBuilder sdSb = new StringBuilder(sbCapacity), serdeSb = new StringBuilder(sbCapacity);
    StringBuilder colsSb = new StringBuilder(7); // We expect that there's only one field schema.
    tblName = tblName.toLowerCase();
    dbName = dbName.toLowerCase();
    for (Object[] fields : sqlResult2) {
      // Here comes the ugly part...
      long partitionId = (Long)fields[0];
      Long sdId = (Long)fields[1];
      Long colId = (Long)fields[2];
      Long serdeId = (Long)fields[3];
      // A partition must have either everything set, or nothing set if it's a view.
      if (sdId == null || colId == null || serdeId == null) {
        if (isView == null) {
          isView = isViewTable(dbName, tblName);
        }
        if ((sdId != null || colId != null || serdeId != null) || !isView) {
          throw new MetaException("Unexpected null for one of the IDs, SD " + sdId + ", column "
              + colId + ", serde " + serdeId + " for a " + (isView ? "" : "non-") + " view");
        }
      }

      Partition part = new Partition();
      orderedResult.add(part);
      // Set the collection fields; some code might not check presence before accessing them.
      part.setParameters(new HashMap<String, String>());
      part.setValues(new ArrayList<String>());
      part.setDbName(dbName);
      part.setTableName(tblName);
      if (fields[4] != null) part.setCreateTime((Integer)fields[4]);
      if (fields[5] != null) part.setLastAccessTime((Integer)fields[5]);
      partitions.put(partitionId, part);

      if (sdId == null) continue; // Probably a view.
      assert colId != null && serdeId != null;

      // We assume each partition has an unique SD.
      StorageDescriptor sd = new StorageDescriptor();
      StorageDescriptor oldSd = sds.put(sdId, sd);
      if (oldSd != null) {
        throw new MetaException("Partitions reuse SDs; we don't expect that");
      }
      // Set the collection fields; some code might not check presence before accessing them.
      sd.setSortCols(new ArrayList<Order>());
      sd.setBucketCols(new ArrayList<String>());
      sd.setParameters(new HashMap<String, String>());
      sd.setSkewedInfo(new SkewedInfo(new ArrayList<String>(),
          new ArrayList<List<String>>(), new HashMap<List<String>, String>()));
      sd.setInputFormat((String)fields[6]);
      Boolean tmpBoolean = extractSqlBoolean(fields[7]);
      if (tmpBoolean != null) sd.setCompressed(tmpBoolean);
      tmpBoolean = extractSqlBoolean(fields[8]);
      if (tmpBoolean != null) sd.setStoredAsSubDirectories(tmpBoolean);
      sd.setLocation((String)fields[9]);
      if (fields[10] != null) sd.setNumBuckets((Integer)fields[10]);
      sd.setOutputFormat((String)fields[11]);
      sdSb.append(sdId).append(",");
      part.setSd(sd);

      List<FieldSchema> cols = colss.get(colId);
      // We expect that colId will be the same for all (or many) SDs.
      if (cols == null) {
        cols = new ArrayList<FieldSchema>();
        colss.put(colId, cols);
        colsSb.append(colId).append(",");
      }
      sd.setCols(cols);

      // We assume each SD has an unique serde.
      SerDeInfo serde = new SerDeInfo();
      SerDeInfo oldSerde = serdes.put(serdeId, serde);
      if (oldSerde != null) {
        throw new MetaException("SDs reuse serdes; we don't expect that");
      }
      serde.setParameters(new HashMap<String, String>());
      serde.setName((String)fields[12]);
      serde.setSerializationLib((String)fields[13]);
      serdeSb.append(serdeId).append(",");
      sd.setSerdeInfo(serde);
    }
    query.closeAll();
    timingTrace(doTrace, queryText, start, queryTime);

    // Now get all the one-to-many things. Start with partitions.
    queryText = "select PART_ID, PARAM_KEY, PARAM_VALUE from PARTITION_PARAMS where PART_ID in ("
        + partIds + ") and PARAM_KEY is not null order by PART_ID asc";
    loopJoinOrderedResult(partitions, queryText, 0, new ApplyFunc<Partition>() {
      public void apply(Partition t, Object[] fields) {
        t.putToParameters((String)fields[1], (String)fields[2]);
      }});

    queryText = "select PART_ID, PART_KEY_VAL from PARTITION_KEY_VALS where PART_ID in ("
        + partIds + ") and INTEGER_IDX >= 0 order by PART_ID asc, INTEGER_IDX asc";
    loopJoinOrderedResult(partitions, queryText, 0, new ApplyFunc<Partition>() {
      public void apply(Partition t, Object[] fields) {
        t.addToValues((String)fields[1]);
      }});

    // Prepare IN (blah) lists for the following queries. Cut off the final ','s.
    if (sdSb.length() == 0) {
      assert serdeSb.length() == 0 && colsSb.length() == 0;
      return orderedResult; // No SDs, probably a view.
    }
    String sdIds = trimCommaList(sdSb), serdeIds = trimCommaList(serdeSb),
        colIds = trimCommaList(colsSb);

    // Get all the stuff for SD. Don't do empty-list check - we expect partitions do have SDs.
    queryText = "select SD_ID, PARAM_KEY, PARAM_VALUE from SD_PARAMS where SD_ID in ("
        + sdIds + ") and PARAM_KEY is not null order by SD_ID asc";
    loopJoinOrderedResult(sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
      public void apply(StorageDescriptor t, Object[] fields) {
        t.putToParameters((String)fields[1], (String)fields[2]);
      }});

    // Note that SORT_COLS has "ORDER" column, which is not SQL92-legal. We have two choices
    // here - drop SQL92, or get '*' and be broken on certain schema changes. We do the latter.
    queryText = "select SD_ID, COLUMN_NAME, SORT_COLS.* from SORT_COLS where SD_ID in ("
        + sdIds + ") and INTEGER_IDX >= 0 order by SD_ID asc, INTEGER_IDX asc";
    loopJoinOrderedResult(sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
      public void apply(StorageDescriptor t, Object[] fields) {
        if (fields[4] == null) return;
        t.addToSortCols(new Order((String)fields[1], (Integer)fields[4]));
      }});

    queryText = "select SD_ID, BUCKET_COL_NAME from BUCKETING_COLS where SD_ID in ("
        + sdIds + ") and INTEGER_IDX >= 0 order by SD_ID asc, INTEGER_IDX asc";
    loopJoinOrderedResult(sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
      public void apply(StorageDescriptor t, Object[] fields) {
        t.addToBucketCols((String)fields[1]);
      }});

    // Skewed columns stuff.
    queryText = "select SD_ID, SKEWED_COL_NAME from SKEWED_COL_NAMES where SD_ID in ("
        + sdIds + ") and INTEGER_IDX >= 0 order by SD_ID asc, INTEGER_IDX asc";
    boolean hasSkewedColumns =
      loopJoinOrderedResult(sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
        public void apply(StorageDescriptor t, Object[] fields) {
          if (!t.isSetSkewedInfo()) t.setSkewedInfo(new SkewedInfo());
          t.getSkewedInfo().addToSkewedColNames((String)fields[1]);
        }}) > 0;

    // Assume we don't need to fetch the rest of the skewed column data if we have no columns.
    if (hasSkewedColumns) {
      // We are skipping the SKEWED_STRING_LIST table here, as it seems to be totally useless.
      queryText =
            "select SKEWED_VALUES.SD_ID_OID, SKEWED_STRING_LIST_VALUES.STRING_LIST_ID, "
          + "  SKEWED_STRING_LIST_VALUES.STRING_LIST_VALUE "
          + "from SKEWED_VALUES "
          + "  left outer join SKEWED_STRING_LIST_VALUES on "
          + "    SKEWED_VALUES.STRING_LIST_ID_EID = SKEWED_STRING_LIST_VALUES.STRING_LIST_ID "
          + "where SKEWED_VALUES.SD_ID_OID in (" + sdIds + ") "
          + "  and SKEWED_VALUES.STRING_LIST_ID_EID is not null "
          + "  and SKEWED_VALUES.INTEGER_IDX >= 0 "
          + "order by SKEWED_VALUES.SD_ID_OID asc, SKEWED_VALUES.INTEGER_IDX asc, "
          + "  SKEWED_STRING_LIST_VALUES.INTEGER_IDX asc";
      loopJoinOrderedResult(sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
        private Long currentListId;
        private List<String> currentList;
        public void apply(StorageDescriptor t, Object[] fields) {
          if (!t.isSetSkewedInfo()) t.setSkewedInfo(new SkewedInfo());
          // Note that this is not a typical list accumulator - there's no call to finalize
          // the last list. Instead we add list to SD first, as well as locally to add elements.
          if (fields[1] == null) {
            currentList = null; // left outer join produced a list with no values
            currentListId = null;
            t.getSkewedInfo().addToSkewedColValues(new ArrayList<String>());
          } else {
            long fieldsListId = (Long)fields[1];
            if (currentListId == null || fieldsListId != currentListId) {
              currentList = new ArrayList<String>();
              currentListId = fieldsListId;
              t.getSkewedInfo().addToSkewedColValues(currentList);
            }
            currentList.add((String)fields[2]);
          }
        }});

      // We are skipping the SKEWED_STRING_LIST table here, as it seems to be totally useless.
      queryText =
            "select SKEWED_COL_VALUE_LOC_MAP.SD_ID, SKEWED_STRING_LIST_VALUES.STRING_LIST_ID,"
          + "  SKEWED_COL_VALUE_LOC_MAP.LOCATION, SKEWED_STRING_LIST_VALUES.STRING_LIST_VALUE "
          + "from SKEWED_COL_VALUE_LOC_MAP"
          + "  left outer join SKEWED_STRING_LIST_VALUES on SKEWED_COL_VALUE_LOC_MAP."
          + "STRING_LIST_ID_KID = SKEWED_STRING_LIST_VALUES.STRING_LIST_ID "
          + "where SKEWED_COL_VALUE_LOC_MAP.SD_ID in (" + sdIds + ")"
          + "  and SKEWED_COL_VALUE_LOC_MAP.STRING_LIST_ID_KID is not null "
          + "order by SKEWED_COL_VALUE_LOC_MAP.SD_ID asc,"
          + "  SKEWED_STRING_LIST_VALUES.STRING_LIST_ID asc,"
          + "  SKEWED_STRING_LIST_VALUES.INTEGER_IDX asc";

      loopJoinOrderedResult(sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
        private Long currentListId;
        private List<String> currentList;
        public void apply(StorageDescriptor t, Object[] fields) {
          if (!t.isSetSkewedInfo()) {
            SkewedInfo skewedInfo = new SkewedInfo();
            skewedInfo.setSkewedColValueLocationMaps(new HashMap<List<String>, String>());
            t.setSkewedInfo(skewedInfo);
          }
          Map<List<String>, String> skewMap = t.getSkewedInfo().getSkewedColValueLocationMaps();
          // Note that this is not a typical list accumulator - there's no call to finalize
          // the last list. Instead we add list to SD first, as well as locally to add elements.
          if (fields[1] == null) {
            currentList = new ArrayList<String>(); // left outer join produced a list with no values
            currentListId = null;
          } else {
            long fieldsListId = (Long)fields[1];
            if (currentListId == null || fieldsListId != currentListId) {
              currentList = new ArrayList<String>();
              currentListId = fieldsListId;
            } else {
              skewMap.remove(currentList); // value based compare.. remove first
            }
            currentList.add((String)fields[3]);
          }
          skewMap.put(currentList, (String)fields[2]);
        }});
    } // if (hasSkewedColumns)

    // Get FieldSchema stuff if any.
    if (!colss.isEmpty()) {
      // We are skipping the CDS table here, as it seems to be totally useless.
      queryText = "select CD_ID, COMMENT, COLUMN_NAME, TYPE_NAME from COLUMNS_V2 where CD_ID in ("
          + colIds + ") and INTEGER_IDX >= 0 order by CD_ID asc, INTEGER_IDX asc";
      loopJoinOrderedResult(colss, queryText, 0, new ApplyFunc<List<FieldSchema>>() {
        public void apply(List<FieldSchema> t, Object[] fields) {
          t.add(new FieldSchema((String)fields[2], (String)fields[3], (String)fields[1]));
        }});
    }

    // Finally, get all the stuff for serdes - just the params.
    queryText = "select SERDE_ID, PARAM_KEY, PARAM_VALUE from SERDE_PARAMS where SERDE_ID in ("
        + serdeIds + ") and PARAM_KEY is not null order by SERDE_ID asc";
    loopJoinOrderedResult(serdes, queryText, 0, new ApplyFunc<SerDeInfo>() {
      public void apply(SerDeInfo t, Object[] fields) {
        t.putToParameters((String)fields[1], (String)fields[2]);
      }});

    return orderedResult;
  }

  private void timingTrace(boolean doTrace, String queryText, long start, long queryTime) {
    if (!doTrace) return;
    LOG.debug("Direct SQL query in " + (queryTime - start) / 1000000.0 + "ms + " +
        (System.nanoTime() - queryTime) / 1000000.0 + "ms, the query is [ " + queryText + "]");
  }

  private static Boolean extractSqlBoolean(Object value) throws MetaException {
    // MySQL has booleans, but e.g. Derby uses 'Y'/'N' mapping. People using derby probably
    // don't care about performance anyway, but let's cover the common case.
    if (value == null) return null;
    if (value instanceof Boolean) return (Boolean)value;
    Character c = null;
    if (value instanceof String && ((String)value).length() == 1) {
      c = ((String)value).charAt(0);
    }
    if (c == 'Y') return true;
    if (c == 'N') return false;
    throw new MetaException("Cannot extrace boolean from column value " + value);
  }

  private static String trimCommaList(StringBuilder sb) {
    if (sb.length() > 0) {
      sb.setLength(sb.length() - 1);
    }
    return sb.toString();
  }

  private abstract class ApplyFunc<Target> {
    public abstract void apply(Target t, Object[] fields);
  }

  /**
   * Merges applies the result of a PM SQL query into a tree of object.
   * Essentially it's an object join. DN could do this for us, but it issues queries
   * separately for every object, which is suboptimal.
   * @param tree The object tree, by ID.
   * @param queryText The query text.
   * @param keyIndex Index of the Long column corresponding to the map ID in query result rows.
   * @param func The function that is called on each (object,row) pair with the same id.
   * @return the count of results returned from the query.
   */
  private <T> int loopJoinOrderedResult(TreeMap<Long, T> tree,
      String queryText, int keyIndex, ApplyFunc<T> func) throws MetaException {
    boolean doTrace = LOG.isDebugEnabled();
    long start = doTrace ? System.nanoTime() : 0;
    Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
    Object result = query.execute();
    long queryTime = doTrace ? System.nanoTime() : 0;
    if (result == null) {
      query.closeAll();
      return 0;
    }
    if (!(result instanceof List<?>)) {
      throw new MetaException("Wrong result type " + result.getClass());
    }
    @SuppressWarnings("unchecked")
    List<Object[]> list = (List<Object[]>)result;
    Iterator<Object[]> iter = list.iterator();
    Object[] fields = null;
    for (Map.Entry<Long, T> entry : tree.entrySet()) {
      if (fields == null && !iter.hasNext()) break;
      long id = entry.getKey();
      while (fields != null || iter.hasNext()) {
        if (fields == null) {
          fields = iter.next();
        }
        long nestedId = (Long)fields[keyIndex];
        if (nestedId < id) throw new MetaException("Found entries for unknown ID " + nestedId);
        if (nestedId > id) break; // fields belong to one of the next entries
        func.apply(entry.getValue(), fields);
        fields = null;
      }
    }
    int rv = list.size();
    query.closeAll();
    timingTrace(doTrace, queryText, start, queryTime);
    return rv;
  }

  private static class PartitionFilterGenerator implements TreeVisitor {
    private final Table table;
    private final StringBuilder filterBuffer;
    private final List<String> params;
    private final List<String> joins;

    private PartitionFilterGenerator(Table table, List<String> params, List<String> joins) {
      this.table = table;
      this.params = params;
      this.joins = joins;
      this.filterBuffer = new StringBuilder();
    }

    /**
     * Generate the ANSI SQL92 filter for the given expression tree
     * @param table the table being queried
     * @param params the ordered parameters for the resulting expression
     * @param joins the joins necessary for the resulting expression
     * @return the string representation of the expression tree
     */
    public static String generateSqlFilter(Table table,
        ExpressionTree tree, List<String> params, List<String> joins) throws MetaException {
      assert table != null;
      if (tree.getRoot() == null) {
        return "";
      }
      PartitionFilterGenerator visitor = new PartitionFilterGenerator(table, params, joins);
      tree.getRoot().accept(visitor);
      // Some joins might be null (see processNode for LeafNode), clean them up.
      for (int i = 0; i < joins.size(); ++i) {
        if (joins.get(i) != null) continue;
        joins.remove(i--);
      }
      return "and (" + visitor.filterBuffer.toString() + ")";
    }

    @Override
    public void visit(TreeNode node) throws MetaException {
      assert node != null && node.getLhs() != null && node.getRhs() != null;
      filterBuffer.append (" (");
      node.getLhs().accept(this);
      filterBuffer.append((node.getAndOr() == LogicalOperator.AND) ? " and " : " or ");
      node.getRhs().accept(this);
      filterBuffer.append (") ");
    }

    @Override
    public void visit(LeafNode node) throws MetaException {
      if (node.operator == Operator.LIKE) {
        // ANSI92 supports || for concatenation (we need to concat '%'-s to the parameter),
        // but it doesn't work on all RDBMSes, e.g. on MySQL by default. So don't use it for now.
        throw new MetaException("LIKE is not supported for SQL filter pushdown");
      }
      int partColCount = table.getPartitionKeys().size();
      int partColIndex = node.getPartColIndexForFilter(table);

      String valueAsString = node.getFilterPushdownParam(table, partColIndex);
      // Add parameters linearly; we are traversing leaf nodes LTR, so they would match correctly.
      params.add(valueAsString);

      if (joins.isEmpty()) {
        // There's a fixed number of partition cols that we might have filters on. To avoid
        // joining multiple times for one column (if there are several filters on it), we will
        // keep numCols elements in the list, one for each column; we will fill it with nulls,
        // put each join at a corresponding index when necessary, and remove nulls in the end.
        for (int i = 0; i < partColCount; ++i) {
          joins.add(null);
        }
      }
      if (joins.get(partColIndex) == null) {
        joins.set(partColIndex, "inner join PARTITION_KEY_VALS as FILTER" + partColIndex
            + " on FILTER"  + partColIndex + ".PART_ID = PARTITIONS.PART_ID and FILTER"
            + partColIndex + ".INTEGER_IDX = " + partColIndex);
      }

      String tableValue = "FILTER" + partColIndex + ".PART_KEY_VAL";
      // TODO: need casts here if #doesOperatorSupportIntegral is amended to include lt/gt/etc.
      filterBuffer.append(node.isReverseOrder
          ? "(? " + node.operator.getSqlOp() + " " + tableValue + ")"
          : "(" + tableValue + " " + node.operator.getSqlOp() + " ?)");
    }
  }
}
