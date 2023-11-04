/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Helper utilities used by DirectSQL code in HiveMetastore.
 */
class MetastoreDirectSqlUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreDirectSqlUtils.class);
  private MetastoreDirectSqlUtils() {

  }
  @SuppressWarnings("unchecked")
  static <T> T executeWithArray(Query query, Object[] params, String sql) throws MetaException {
    return (T)executeWithArray(query, params, sql, -1);
  }

  @SuppressWarnings("unchecked")
  static <T> T executeWithArray(Query query, Object[] params, String sql, int limit) throws MetaException {
    try {
      if (limit >= 0)
        query.setRange(0, limit);
      return (T)((params == null) ? query.execute() : query.executeWithArray(params));
    } catch (Exception ex) {
      StringBuilder errorBuilder = new StringBuilder("Failed to execute [" + sql + "] with parameters [");
      if (params != null) {
        boolean isFirst = true;
        for (Object param : params) {
          errorBuilder.append((isFirst ? "" : ", ") + param);
          isFirst = false;
        }
      }
      LOG.warn(errorBuilder.toString() + "]", ex);
      // We just logged an exception with (in case of JDO) a humongous callstack. Make a new one.
      throw new MetaException("See previous errors; " + ex.getMessage() + errorBuilder.toString() + "]");
    }
  }

  @SuppressWarnings("unchecked")
  static List<Object[]> ensureList(Object result) throws MetaException {
    if (!(result instanceof List<?>)) {
      throw new MetaException("Wrong result type " + result.getClass());
    }
    return (List<Object[]>)result;
  }

  static Long extractSqlLong(Object obj) throws MetaException {
    if (obj == null) return null;
    if (!(obj instanceof Number)) {
      throw new MetaException("Expected numeric type but got " + obj.getClass().getName());
    }
    return ((Number)obj).longValue();
  }

  static void timingTrace(boolean doTrace, String queryText, long start, long queryTime) {
    if (!doTrace) return;
    LOG.debug("Direct SQL query in " + (queryTime - start) / 1000000.0 + "ms + " +
        (System.nanoTime() - queryTime) / 1000000.0 + "ms, the query is [" + queryText + "]");
  }

  static <T> int loopJoinOrderedResult(PersistenceManager pm, TreeMap<Long, T> tree,
      String queryText, int keyIndex, ApplyFunc<T> func) throws MetaException {
    return loopJoinOrderedResult(pm, tree, queryText, null, keyIndex, func);
  }
  /**
   * Merges applies the result of a PM SQL query into a tree of object.
   * Essentially it's an object join. DN could do this for us, but it issues queries
   * separately for every object, which is suboptimal.
   * @param pm
   * @param tree The object tree, by ID.
   * @param queryText The query text.
   * @param keyIndex Index of the Long column corresponding to the map ID in query result rows.
   * @param func The function that is called on each (object,row) pair with the same id.
   * @return the count of results returned from the query.
   */
  static <T> int loopJoinOrderedResult(PersistenceManager pm, TreeMap<Long, T> tree,
      String queryText, Object[] parameters, int keyIndex, ApplyFunc<T> func) throws MetaException {
    boolean doTrace = LOG.isDebugEnabled();
    long start = doTrace ? System.nanoTime() : 0;
    int rv = 0;
    long queryTime = 0;
    try (Query query = pm.newQuery("javax.jdo.query.SQL", queryText)) {
      Object result = null;
      if (parameters == null || parameters.length == 0) {
        result = query.execute();
      } else {
        result = query.executeWithArray(parameters);
      }
      queryTime = doTrace ? System.nanoTime() : 0;
      if (result == null) {
        query.closeAll();
        return 0;
      }
      List<Object[]> list = ensureList(result);
      Iterator<Object[]> iter = list.iterator();
      Object[] fields = null;
      for (Map.Entry<Long, T> entry : tree.entrySet()) {
        if (fields == null && !iter.hasNext())
          break;
        long id = entry.getKey();
        while (fields != null || iter.hasNext()) {
          if (fields == null) {
            fields = iter.next();
          }
          long nestedId = extractSqlLong(fields[keyIndex]);
          if (nestedId < id) {
            throw new MetaException("Found entries for unknown ID " + nestedId);
          }
          if (nestedId > id)
            break; // fields belong to one of the next entries
          func.apply(entry.getValue(), fields);
          fields = null;
        }
        Deadline.checkTimeout();
      }
      rv = list.size();
    } catch (Exception e) {
      throwMetaOrRuntimeException(e);
    }
    timingTrace(doTrace, queryText, start, queryTime);
    return rv;
  }

  static void setPartitionParametersWithFilter(String PARTITION_PARAMS,
      boolean convertMapNullsToEmptyStrings, PersistenceManager pm, String partIds,
      TreeMap<Long, Partition> partitions, String includeParamKeyPattern, String excludeParamKeyPattern)
      throws MetaException {
    StringBuilder queryTextBuilder = new StringBuilder("select \"PART_ID\", \"PARAM_KEY\", \"PARAM_VALUE\" from ")
        .append(PARTITION_PARAMS)
        .append(" where \"PART_ID\" in (")
        .append(partIds)
        .append(") and \"PARAM_KEY\" is not null");
    List<Object> queryParams = new ArrayList<>(2);;
    if (includeParamKeyPattern != null && !includeParamKeyPattern.isEmpty()) {
      queryTextBuilder.append(" and \"PARAM_KEY\" LIKE (?)");
      queryParams.add(includeParamKeyPattern);
    }
    if (excludeParamKeyPattern != null && !excludeParamKeyPattern.isEmpty()) {
      queryTextBuilder.append(" and \"PARAM_KEY\" NOT LIKE (?)");
      queryParams.add(excludeParamKeyPattern);
    }

    queryTextBuilder.append(" order by \"PART_ID\" asc");
    String queryText = queryTextBuilder.toString();
    loopJoinOrderedResult(pm, partitions, queryText, queryParams.toArray(), 0, new ApplyFunc<Partition>() {
      @Override
      public void apply(Partition t, Object[] fields) {
        t.putToParameters((String) fields[1], extractSqlClob(fields[2]));
      }
    });
    // Perform conversion of null map values
    for (Partition t : partitions.values()) {
      t.setParameters(MetaStoreServerUtils.trimMapNulls(t.getParameters(), convertMapNullsToEmptyStrings));
    }
  }

  static void setPartitionValues(String PARTITION_KEY_VALS, PersistenceManager pm, String partIds,
      TreeMap<Long, Partition> partitions)
      throws MetaException {
    String queryText;
    queryText = "select \"PART_ID\", \"PART_KEY_VAL\" from " + PARTITION_KEY_VALS + ""
        + " where \"PART_ID\" in (" + partIds + ")"
        + " order by \"PART_ID\" asc, \"INTEGER_IDX\" asc";
    loopJoinOrderedResult(pm, partitions, queryText, 0, new ApplyFunc<Partition>() {
      @Override
      public void apply(Partition t, Object[] fields) {
        t.addToValues((String)fields[1]);
      }});
  }

  static String extractSqlClob(Object value) {
    if (value == null) return null;
    try {
      if (value instanceof Clob) {
        // we trim the Clob value to a max length an int can hold
        int maxLength = (((Clob)value).length() < Integer.MAX_VALUE - 2) ? (int)((Clob)value).length() : Integer.MAX_VALUE - 2;
        return ((Clob)value).getSubString(1L, maxLength);
      } else {
        return value.toString();
      }
    } catch (SQLException sqle) {
      return null;
    }
  }

  static void setSDParameters(String SD_PARAMS, boolean convertMapNullsToEmptyStrings,
      PersistenceManager pm, TreeMap<Long, StorageDescriptor> sds, String sdIds)
      throws MetaException {
    String queryText;
    queryText = "select \"SD_ID\", \"PARAM_KEY\", \"PARAM_VALUE\" from " + SD_PARAMS + ""
        + " where \"SD_ID\" in (" + sdIds + ") and \"PARAM_KEY\" is not null"
        + " order by \"SD_ID\" asc";
    loopJoinOrderedResult(pm, sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
      @Override
      public void apply(StorageDescriptor t, Object[] fields) {
        t.putToParameters((String)fields[1], extractSqlClob(fields[2]));
      }});
    // Perform conversion of null map values
    for (StorageDescriptor t : sds.values()) {
      t.setParameters(MetaStoreServerUtils.trimMapNulls(t.getParameters(), convertMapNullsToEmptyStrings));
    }
  }

  static int extractSqlInt(Object field) {
    return ((Number)field).intValue();
  }

  static void setSDSortCols(String SORT_COLS, List<String> columnNames, PersistenceManager pm,
      TreeMap<Long, StorageDescriptor> sds, String sdIds)
      throws MetaException {
    StringBuilder queryTextBuilder = new StringBuilder("select \"SD_ID\"");
    int counter = 0;
    if (columnNames.contains("col")) {
      counter++;
      queryTextBuilder.append(", \"COLUMN_NAME\"");
    }
    if (columnNames.contains("order")) {
      counter++;
      queryTextBuilder.append(", \"ORDER\"");
    }
    queryTextBuilder
        .append(" from ")
        .append(SORT_COLS)
        .append(" where \"SD_ID\" in (")
        .append(sdIds)
        .append(") order by \"SD_ID\" asc, \"INTEGER_IDX\" asc");
    String queryText = queryTextBuilder.toString();
    final int finalCounter = counter;
    loopJoinOrderedResult(pm, sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
      @Override
      public void apply(StorageDescriptor t, Object[] fields) {
        if (finalCounter > 1 && fields[2] == null) {
          return;
        }
        Order order = new Order();
        if (finalCounter > 0) {
          order.setCol((String) fields[1]);
        }
        if (finalCounter > 1) {
          order.setOrder(extractSqlInt(fields[2]));
        }
        t.addToSortCols(order);
      }});
  }

  static void setSDSortCols(String SORT_COLS, PersistenceManager pm,
      TreeMap<Long, StorageDescriptor> sds, String sdIds)
      throws MetaException {
    String queryText;
    queryText = "select \"SD_ID\", \"COLUMN_NAME\", " + SORT_COLS + ".\"ORDER\""
        + " from " + SORT_COLS + ""
        + " where \"SD_ID\" in (" + sdIds + ")"
        + " order by \"SD_ID\" asc, \"INTEGER_IDX\" asc";
    loopJoinOrderedResult(pm, sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
      @Override
      public void apply(StorageDescriptor t, Object[] fields) {
        if (fields[2] == null) return;
        t.addToSortCols(new Order((String)fields[1], extractSqlInt(fields[2])));
      }});
  }

  static void setSDBucketCols(String BUCKETING_COLS, PersistenceManager pm,
      TreeMap<Long, StorageDescriptor> sds, String sdIds)
      throws MetaException {
    String queryText;
    queryText = "select \"SD_ID\", \"BUCKET_COL_NAME\" from " + BUCKETING_COLS + ""
        + " where \"SD_ID\" in (" + sdIds + ")"
        + " order by \"SD_ID\" asc, \"INTEGER_IDX\" asc";
    loopJoinOrderedResult(pm, sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
      @Override
      public void apply(StorageDescriptor t, Object[] fields) {
        t.addToBucketCols((String)fields[1]);
      }});
  }

  static boolean setSkewedColNames(String SKEWED_COL_NAMES, PersistenceManager pm,
      TreeMap<Long, StorageDescriptor> sds, String sdIds)
      throws MetaException {
    String queryText;
    queryText = "select \"SD_ID\", \"SKEWED_COL_NAME\" from " + SKEWED_COL_NAMES + ""
        + " where \"SD_ID\" in (" + sdIds + ")"
        + " order by \"SD_ID\" asc, \"INTEGER_IDX\" asc";
    return loopJoinOrderedResult(pm, sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
      @Override
      public void apply(StorageDescriptor t, Object[] fields) {
        if (!t.isSetSkewedInfo()) t.setSkewedInfo(new SkewedInfo());
        t.getSkewedInfo().addToSkewedColNames((String)fields[1]);
      }}) > 0;
  }

  static void setSkewedColValues(String SKEWED_STRING_LIST_VALUES, String SKEWED_VALUES,
      PersistenceManager pm, TreeMap<Long, StorageDescriptor> sds, String sdIds)
      throws MetaException {
    String queryText;
    queryText =
          "select " + SKEWED_VALUES + ".\"SD_ID_OID\","
        + "  " + SKEWED_STRING_LIST_VALUES + ".\"STRING_LIST_ID\","
        + "  " + SKEWED_STRING_LIST_VALUES + ".\"STRING_LIST_VALUE\" "
        + "from " + SKEWED_VALUES + " "
        + "  left outer join " + SKEWED_STRING_LIST_VALUES + " on " + SKEWED_VALUES + "."
        + "\"STRING_LIST_ID_EID\" = " + SKEWED_STRING_LIST_VALUES + ".\"STRING_LIST_ID\" "
        + "where " + SKEWED_VALUES + ".\"SD_ID_OID\" in (" + sdIds + ") "
        + "  and " + SKEWED_VALUES + ".\"STRING_LIST_ID_EID\" is not null "
        + "  and " + SKEWED_VALUES + ".\"INTEGER_IDX\" >= 0 "
        + "order by " + SKEWED_VALUES + ".\"SD_ID_OID\" asc, " + SKEWED_VALUES + ".\"INTEGER_IDX\" asc,"
        + "  " + SKEWED_STRING_LIST_VALUES + ".\"INTEGER_IDX\" asc";
    loopJoinOrderedResult(pm, sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
      private Long currentListId;
      private List<String> currentList;
      @Override
      public void apply(StorageDescriptor t, Object[] fields) throws MetaException {
        if (!t.isSetSkewedInfo()) t.setSkewedInfo(new SkewedInfo());
        // Note that this is not a typical list accumulator - there's no call to finalize
        // the last list. Instead we add list to SD first, as well as locally to add elements.
        if (fields[1] == null) {
          currentList = null; // left outer join produced a list with no values
          currentListId = null;
          t.getSkewedInfo().addToSkewedColValues(Collections.<String>emptyList());
        } else {
          long fieldsListId = extractSqlLong(fields[1]);
          if (currentListId == null || fieldsListId != currentListId) {
            currentList = new ArrayList<String>();
            currentListId = fieldsListId;
            t.getSkewedInfo().addToSkewedColValues(currentList);
          }
          currentList.add((String)fields[2]);
        }
      }});
  }

  static void setSkewedColLocationMaps(String SKEWED_COL_VALUE_LOC_MAP,
      String SKEWED_STRING_LIST_VALUES, PersistenceManager pm, TreeMap<Long, StorageDescriptor> sds,
      String sdIds)
      throws MetaException {
    String queryText;
    queryText =
          "select " + SKEWED_COL_VALUE_LOC_MAP + ".\"SD_ID\","
        + " " + SKEWED_STRING_LIST_VALUES + ".\"STRING_LIST_ID\","
        + " " + SKEWED_COL_VALUE_LOC_MAP + ".\"LOCATION\","
        + " " + SKEWED_STRING_LIST_VALUES + ".\"STRING_LIST_VALUE\" "
        + "from " + SKEWED_COL_VALUE_LOC_MAP + ""
        + "  left outer join " + SKEWED_STRING_LIST_VALUES + " on " + SKEWED_COL_VALUE_LOC_MAP + "."
        + "\"STRING_LIST_ID_KID\" = " + SKEWED_STRING_LIST_VALUES + ".\"STRING_LIST_ID\" "
        + "where " + SKEWED_COL_VALUE_LOC_MAP + ".\"SD_ID\" in (" + sdIds + ")"
        + "  and " + SKEWED_COL_VALUE_LOC_MAP + ".\"STRING_LIST_ID_KID\" is not null "
        + "order by " + SKEWED_COL_VALUE_LOC_MAP + ".\"SD_ID\" asc,"
        + "  " + SKEWED_STRING_LIST_VALUES + ".\"STRING_LIST_ID\" asc,"
        + "  " + SKEWED_STRING_LIST_VALUES + ".\"INTEGER_IDX\" asc";

    loopJoinOrderedResult(pm, sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
      private Long currentListId;
      private List<String> currentList;
      @Override
      public void apply(StorageDescriptor t, Object[] fields) throws MetaException {
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
          long fieldsListId = extractSqlLong(fields[1]);
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
  }

  static void setSDCols(String COLUMNS_V2, List<String> columnNames, PersistenceManager pm,
      TreeMap<Long, List<FieldSchema>> colss, String colIds)
      throws MetaException {
    StringBuilder queryTextBuilder = new StringBuilder("select \"CD_ID\"");
    int counter = 0;
    if (columnNames.contains("name")) {
      counter++;
      queryTextBuilder.append(", \"COLUMN_NAME\"");
    }
    if (columnNames.contains("type")) {
      counter++;
      queryTextBuilder.append(", \"TYPE_NAME\"");
    }
    if (columnNames.contains("comment")) {
      counter++;
      queryTextBuilder.append(", \"COMMENT\"");
    }
    queryTextBuilder
        .append(" from ")
        .append(COLUMNS_V2)
        .append(" where \"CD_ID\" in (")
        .append(colIds)
        .append(") order by \"CD_ID\" asc, \"INTEGER_IDX\" asc");
    String queryText = queryTextBuilder.toString();
    int finalCounter = counter;
    loopJoinOrderedResult(pm, colss, queryText, 0, new ApplyFunc<List<FieldSchema>>() {
      @Override
      public void apply(List<FieldSchema> t, Object[] fields) {
        FieldSchema fieldSchema = new FieldSchema();
        if (finalCounter > 0) {
          fieldSchema.setName((String) fields[1]);
        }
        if (finalCounter > 1) {
          fieldSchema.setType(extractSqlClob(fields[2]));
        }
        if (finalCounter > 2) {
          fieldSchema.setComment((String) fields[3]);
        }
        t.add(fieldSchema);
      }});
  }

  static void setSDCols(String COLUMNS_V2, PersistenceManager pm,
      TreeMap<Long, List<FieldSchema>> colss, String colIds)
      throws MetaException {
    String queryText;
    queryText = "select \"CD_ID\", \"COMMENT\", \"COLUMN_NAME\", \"TYPE_NAME\""
        + " from " + COLUMNS_V2 + " where \"CD_ID\" in (" + colIds + ")"
        + " order by \"CD_ID\" asc, \"INTEGER_IDX\" asc";
    loopJoinOrderedResult(pm, colss, queryText, 0, new ApplyFunc<List<FieldSchema>>() {
      @Override
      public void apply(List<FieldSchema> t, Object[] fields) {
        t.add(new FieldSchema((String)fields[2], extractSqlClob(fields[3]), (String)fields[1]));
      }});
  }

  static void setSerdeParams(String SERDE_PARAMS, boolean convertMapNullsToEmptyStrings,
      PersistenceManager pm, TreeMap<Long, SerDeInfo> serdes, String serdeIds) throws MetaException {
    String queryText;
    queryText = "select \"SERDE_ID\", \"PARAM_KEY\", \"PARAM_VALUE\" from " + SERDE_PARAMS + ""
        + " where \"SERDE_ID\" in (" + serdeIds + ") and \"PARAM_KEY\" is not null"
        + " order by \"SERDE_ID\" asc";
    loopJoinOrderedResult(pm, serdes, queryText, 0, new ApplyFunc<SerDeInfo>() {
      @Override
      public void apply(SerDeInfo t, Object[] fields) {
        t.putToParameters((String)fields[1], extractSqlClob(fields[2]));
      }});
    // Perform conversion of null map values
    for (SerDeInfo t : serdes.values()) {
      t.setParameters(MetaStoreServerUtils.trimMapNulls(t.getParameters(), convertMapNullsToEmptyStrings));
    }
  }

  static void setFunctionResourceUris(String FUNC_RU, PersistenceManager pm, String funcIds,
      TreeMap<Long, Function> functions)
      throws MetaException {
    String queryText;
    queryText = "select \"FUNC_ID\", \"RESOURCE_TYPE\", \"RESOURCE_URI\" from " +  FUNC_RU
        + " where \"FUNC_ID\" in (" + funcIds + ")"
        + " order by \"FUNC_ID\" asc, \"INTEGER_IDX\" asc";
    loopJoinOrderedResult(pm, functions, queryText, 0, (t, fields) -> {
      ResourceUri resourceUri = new ResourceUri(ResourceType.findByValue((int)fields[1]), (String) fields[2]);
      t.getResourceUris().add(resourceUri);
    });
  }

  /**
   * Convert a boolean value returned from the RDBMS to a Java Boolean object.
   * MySQL has booleans, but e.g. Derby uses 'Y'/'N' mapping and Oracle DB
   * doesn't supports boolean, so we have used Number to store the value,
   * which maps to BigDecimal.
   *
   * @param value
   *          column value from the database
   * @return The Boolean value of the database column value, null if the column
   *         value is null
   * @throws MetaException
   *           if the column value cannot be converted into a Boolean object
   */
  static Boolean extractSqlBoolean(Object value) throws MetaException {
    if (value == null) {
      return null;
    }
    if (value instanceof Boolean) {
      return (Boolean)value;
    }

    // check if oracle db returned 0 or 1 for boolean value
    if (value instanceof Number) {
      try {
        return BooleanUtils.toBooleanObject(Integer.valueOf(((Number) value).intValue()), 1, 0, null);
      } catch (IllegalArgumentException iae) {
        // NOOP
      }
    }
    if (value instanceof String) {
      try {
        return BooleanUtils.toBooleanObject((String) value, "Y", "N", null);
      } catch (IllegalArgumentException iae) {
        // NOOP
      }
    }

    if (value instanceof BigDecimal) {
      BigDecimal bigDecimal = (BigDecimal) value;
      if (bigDecimal.intValue() == 0) {
        return false;
      } else {
        return true;
      }
    }
    LOG.debug("Value is of type {}", value.getClass());
    throw new MetaException("Cannot extract boolean from column value " + value);
  }

  static String extractSqlString(Object value) {
    if (value == null) return null;
    return value.toString();
  }

  static Double extractSqlDouble(Object obj) throws MetaException {
    if (obj == null)
      return null;
    if (!(obj instanceof Number)) {
      throw new MetaException("Expected numeric type but got " + obj.getClass().getName());
    }
    return ((Number) obj).doubleValue();
  }

  static byte[] extractSqlBlob(Object value) throws MetaException {
    if (value == null)
      return null;
    if (value instanceof Blob) {
      //derby, oracle
      try {
        // getBytes function says: pos the ordinal position of the first byte in
        // the BLOB value to be extracted; the first byte is at position 1
        return ((Blob) value).getBytes(1, (int) ((Blob) value).length());
      } catch (SQLException e) {
        throw new MetaException("Encounter error while processing blob.");
      }
    }
    else if (value instanceof byte[]) {
      // mysql, postgres, sql server
      return (byte[]) value;
    }
	else {
      // this may happen when enablebitvector is false
      LOG.debug("Expected blob type but got " + value.getClass().getName());
      return null;
    }
  }

  @FunctionalInterface
  static interface ApplyFunc<Target> {
    void apply(Target t, Object[] fields) throws MetaException;
  }

  public static void throwMetaOrRuntimeException(Exception e) throws MetaException {
    if (e instanceof MetaException) {
      throw (MetaException) e;
    } else if (e instanceof RuntimeException) {
      throw (RuntimeException) e;
    } else {
      throw new RuntimeException(e);
    }
  }
}
