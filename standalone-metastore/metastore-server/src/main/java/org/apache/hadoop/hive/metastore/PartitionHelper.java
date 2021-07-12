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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.PersistenceManager;
import javax.jdo.datastore.JDOConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.metastore.DirectSqlUpdateStat.quoteString;
import static org.apache.hadoop.hive.metastore.Warehouse.makePartName;

/**
 * This class contains the optimizations for MetaStore that rely on direct SQL access to
 * the underlying database. It should use ANSI SQL and be compatible with common databases
 * such as MySQL (note that MySQL doesn't use full ANSI mode by default), Postgres, etc.
 *
 * This class implements some helper method for operations related to partition.
 */
class PartitionHelper {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionHelper.class);

  private static void close(ResultSet rs) {
    try {
      if (rs != null && !rs.isClosed()) {
        rs.close();
      }
    } catch(SQLException ex) {
      LOG.warn("Failed to close statement ", ex);
    }
  }

  // If next value is already inserted, then increment it by numValues. Else return -1.
  private static long updateAndGetNextValueFromSequence(Connection dbConn, String seqName, long numValues)
          throws SQLException {
    ResultSet rs = null;
    long currValue = -1;

    try (Statement statement = dbConn.createStatement()) {
      String query = "SELECT \"NEXT_VAL\" FROM \"SEQUENCE_TABLE\" WHERE \"SEQUENCE_NAME\"= "
              + quoteString(seqName)
              + " FOR UPDATE";
      LOG.debug("Going to execute query " + query);
      rs = statement.executeQuery(query);
      if (rs.next()) {
        currValue = rs.getLong(1);
      }
    } finally {
      close(rs);
    }

    // If its already there, update.
    if (currValue != -1) {
      long nextValue = currValue + numValues;
      try (Statement statement = dbConn.createStatement()) {
        String query = "UPDATE \"SEQUENCE_TABLE\" SET \"NEXT_VAL\" = "
                + nextValue
                + " WHERE \"SEQUENCE_NAME\" = "
                + quoteString(seqName);
        statement.executeUpdate(query);
      }
    }
    return currValue;
  }

  /**
   * Gets the next value from SEQUENCE_TABLE table for given sequence. The sequence value is updated by numValues and
   * the current value is returned.
   * @return The sequence value before update.
   */
  public static long getNextValueFromSequenceTable(Connection dbConn, String seqName, long numValues,
                                            DatabaseProduct dbType) throws MetaException {
    try {
      long currValue = updateAndGetNextValueFromSequence(dbConn, seqName, numValues);
      if (currValue != -1) {
        return currValue;
      }

      // If next value for the sequence is not present insert the updated value and return 1.
      currValue = 1;
      try (Statement statement = dbConn.createStatement()) {
        long nextValue = currValue + numValues;
        String query = "INSERT INTO \"SEQUENCE_TABLE\" (\"SEQUENCE_NAME\", \"NEXT_VAL\")  VALUES ( "
                + quoteString(seqName) + "," + nextValue
                + ")";
        statement.executeUpdate(query);
      } catch (SQLException e) {
        // If the record is already inserted by some other thread then update it.
        if (dbType.isDuplicateKeyError(e)) {
          currValue = updateAndGetNextValueFromSequence(dbConn, seqName, numValues);
        } else {
          LOG.error("Unable to insert into SEQUENCE_TABLE for MPartitionColumnStatistics.", e);
          throw e;
        }
      }
      return currValue;
    } catch (Exception e) {
      LOG.error("Unable to getNextCSIdForMPartitionColumnStatistics", e);
      throw new MetaException("Unable to getNextCSIdForMPartitionColumnStatistics  "
              + " due to: " + e.getMessage());
    }
  }

  // Check if partition privilege tables needs to be updated. This is done by checking the table privilege table. If
  // table privilege info is present for table, then the same info is added for partitions also.
  public static boolean needToAddPrivilegeInfo(Connection dbConn, long tblId) throws SQLException {
    ResultSet rs = null;
    try (Statement statement = dbConn.createStatement()) {
      String query = "SELECT \"PARAM_VALUE\" FROM \"TABLE_PARAMS\" WHERE \"PARAM_KEY\"= "
              + quoteString("PARTITION_LEVEL_PRIVILEGE")
              + " AND  \"TBL_ID\" = " + tblId;
      LOG.debug("Going to execute query " + query);
      rs = statement.executeQuery(query);
      if (rs.next()) {
        String grantInfo = rs.getString(1);
        if ("TRUE".equalsIgnoreCase(grantInfo)) {
          return true;
        }
      }
    } finally {
      close(rs);
    }
    return false;
  }
  
  private static long addBatch(PreparedStatement pst, long numRecords, long maxBatchSize) throws SQLException {
    pst.addBatch();
    numRecords++;
    if (numRecords == maxBatchSize) {
      executeBatch(pst, numRecords);
      numRecords = 0;
    }
    return numRecords;
  }
  
  private static void executeBatch(PreparedStatement pst, long numRecords) throws SQLException {
    if (numRecords != 0) {
      pst.executeBatch();
    }
  }

  public static void addPartitionPrivilegeInfo(Connection dbConn, List<Partition> parts, long tblId, long partIdx,
                                               DatabaseProduct dbType, long maxBatchSize)
          throws SQLException, MetaException {
    List<Integer> grantOptionList = new ArrayList<>();
    List<String> grantorList = new ArrayList<>();
    List<String> grantTypeList = new ArrayList<>();
    List<String> principalNameList = new ArrayList<>();
    List<String> principalTypeList = new ArrayList<>();
    List<String> tblPrivList = new ArrayList<>();
    List<String> authorizerList = new ArrayList<>();
    ResultSet rs = null;
    try (Statement statement = dbConn.createStatement()) {
      String query = "SELECT \"GRANT_OPTION\", \"GRANTOR\", \"GRANTOR_TYPE\", \"PRINCIPAL_NAME\", "
              + " \"PRINCIPAL_TYPE\", \"TBL_PRIV\", \"AUTHORIZER\" "
              + " FROM \"TBL_PRIVS\""
              + " WHERE  \"TBL_ID\" = " + tblId;
      LOG.debug("Going to execute query " + query);
      rs = statement.executeQuery(query);
      while (rs.next()) {
        grantOptionList.add(rs.getInt(1));
        grantorList.add(rs.getString(2));
        grantTypeList.add(rs.getString(3));
        principalNameList.add(rs.getString(4));
        principalTypeList.add(rs.getString(5));
        tblPrivList.add(rs.getString(6));
        authorizerList.add(rs.getString(7));
      }
    } finally {
      close(rs);
    }

    long grantIdIdx = PartitionHelper.getNextValueFromSequenceTable(dbConn,
            "org.apache.hadoop.hive.metastore.model.MTablePrivilege", parts.size(), dbType);
    int now = (int) (System.currentTimeMillis() / 1000);
    String insertPartPrev = "INSERT INTO \"PART_PRIVS\" "
            + " (\"PART_GRANT_ID\", \"CREATE_TIME\", \"GRANT_OPTION\", \"GRANTOR\", \"GRANTOR_TYPE\", \"PART_ID\","
            + " \"PRINCIPAL_NAME\", \"PRINCIPAL_TYPE\", \"PART_PRIV\", \"AUTHORIZER\") "
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";

    try (PreparedStatement pst = dbConn.prepareStatement(insertPartPrev)) {
      long numRecords = 0;
      for (int j = 0; j < parts.size(); j++) {
        for (int i = 0; i < grantOptionList.size(); i++) {
          pst.setLong(1, grantIdIdx++);
          pst.setInt(2, now);
          pst.setInt(3, grantOptionList.get(i));
          pst.setString(4, grantorList.get(i));
          pst.setString(5, grantTypeList.get(i));
          pst.setLong(6, partIdx);
          pst.setString(7, principalNameList.get(i));
          pst.setString(8, principalTypeList.get(i));
          pst.setString(9, tblPrivList.get(i));
          pst.setString(10, authorizerList.get(i));
          numRecords = addBatch(pst, numRecords, maxBatchSize);
        }
        partIdx++;
      }
      executeBatch(pst, numRecords);
    }
  }

  public static void addPartitionColPrivilegeInfo(Connection dbConn, List<Partition> parts, long tblId,
                                                  long partIdx, DatabaseProduct dbType, long maxBatchSize)
          throws SQLException, MetaException {
    ResultSet rs = null;
    List<String> colNameList = new ArrayList<>();
    List<Integer> grantOptionList = new ArrayList<>();
    List<String> grantorList = new ArrayList<>();
    List<String> grantTypeList = new ArrayList<>();
    List<String> principalNameList = new ArrayList<>();
    List<String> principalTypeList = new ArrayList<>();
    List<String> tblColPrivList = new ArrayList<>();
    List<String> authorizerList = new ArrayList<>();
    try (Statement statement = dbConn.createStatement()) {
      String query = "SELECT \"COLUMN_NAME\", \"GRANT_OPTION\", \"GRANTOR\", \"GRANTOR_TYPE\", \"PRINCIPAL_NAME\","
              + " \"PRINCIPAL_TYPE\", \"TBL_COL_PRIV\", \"AUTHORIZER\" "
              + " FROM \"TBL_COL_PRIVS\""
              + " WHERE  \"TBL_ID\" = " + tblId;
      LOG.debug("Going to execute query " + query);
      rs = statement.executeQuery(query);
      while (rs.next()) {
        colNameList.add(rs.getString(1));
        grantOptionList.add(rs.getInt(2));
        grantorList.add(rs.getString(3));
        grantTypeList.add(rs.getString(4));
        principalNameList.add(rs.getString(5));
        principalTypeList.add(rs.getString(6));
        tblColPrivList.add(rs.getString(7));
        authorizerList.add(rs.getString(8));
      }
    } finally {
      close(rs);
    }

    long grantIdIdx = PartitionHelper.getNextValueFromSequenceTable(dbConn,
            "org.apache.hadoop.hive.metastore.model.MTablePrivilege", parts.size(), dbType);
    int now = (int) (System.currentTimeMillis() / 1000);
    String insertPartPrev = "INSERT INTO \"PART_COL_PRIVS\" "
            + " (\"PART_COLUMN_GRANT_ID\", \"COLUMN_NAME\", \"CREATE_TIME\", \"GRANT_OPTION\", \"GRANTOR\","
            + " \"GRANTOR_TYPE\", \"PART_ID\","
            + " \"PRINCIPAL_NAME\", \"PRINCIPAL_TYPE\", \"PART_COL_PRIV\", \"AUTHORIZER\") "
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";
    try (PreparedStatement pst = dbConn.prepareStatement(insertPartPrev)) {
      long numRecords = 0;
      for (int j = 0; j < parts.size(); j++) {
        for (int i = 0; i < grantOptionList.size(); i++) {
          pst.setLong(1, grantIdIdx++);
          pst.setString(2, colNameList.get(i));
          pst.setInt(3, now);
          pst.setInt(4, grantOptionList.get(i));
          pst.setString(5, grantorList.get(i));
          pst.setString(6, grantTypeList.get(i));
          pst.setLong(7, partIdx);
          pst.setString(8, principalNameList.get(i));
          pst.setString(9, principalTypeList.get(i));
          pst.setString(10, tblColPrivList.get(i));
          pst.setString(11, authorizerList.get(i));
          numRecords = addBatch(pst, numRecords, maxBatchSize);
        }
        partIdx++;
      }
      executeBatch(pst, numRecords);
    }
  }

  public static void addSerdeInfo(Connection dbConn, List<Partition> parts, long serdeIdIdx, long maxBatchSize)
          throws SQLException {
    String insertSerdeInfo = "INSERT INTO \"SERDES\" (\"SERDE_ID\", \"NAME\", \"SLIB\", \"DESCRIPTION\","
            + " \"SERIALIZER_CLASS\", \"DESERIALIZER_CLASS\", \"SERDE_TYPE\") VALUES (?, ?, ?, ?, ?, ?, ?) ";
    try (PreparedStatement pst = dbConn.prepareStatement(insertSerdeInfo)) {
      long numRecords = 0;
      for (Partition part : parts) {
        if (part.getSd() == null) {
          continue;
        }
        SerDeInfo serDeInfo = part.getSd().getSerdeInfo();
        pst.setLong(1, serdeIdIdx++);
        pst.setObject(2, serDeInfo.getName());
        pst.setObject(3, serDeInfo.getSerializationLib());
        pst.setObject(4, serDeInfo.getDescription());
        pst.setObject(5, serDeInfo.getSerializerClass());
        pst.setObject(6, serDeInfo.getDeserializerClass());
        pst.setLong(7, serDeInfo.getSerdeType() != null ?
                serDeInfo.getSerdeType().getValue() : 0);
        numRecords = addBatch(pst, numRecords, maxBatchSize);
      }
      executeBatch(pst, numRecords);
    }
  }

  public static void addColDescInfo(Connection dbConn, long numPart, long cdIdIdx, long maxBatchSize)
          throws SQLException {
    String insertCDInfo = "INSERT INTO \"CDS\" (\"CD_ID\") VALUES (?) ";
    try (PreparedStatement pst = dbConn.prepareStatement(insertCDInfo)) {
      long numRecords = 0;
      for (int i = 0; i < numPart; i++) {
        pst.setLong(1, cdIdIdx++);
        numRecords = addBatch(pst, numRecords, maxBatchSize);
      }
      executeBatch(pst, numRecords);
    }
  }

  public static void addSDInfo(Connection dbConn, List<Partition> parts, long sdIdIdx,
                               long serdeIdIdx, long cdIdIdx, DatabaseProduct dbType, long maxBatchSize)
          throws SQLException {
    String insertSDInfo = "INSERT INTO \"SDS\" (\"SD_ID\", \"INPUT_FORMAT\", \"IS_COMPRESSED\", \"LOCATION\","
            + " \"NUM_BUCKETS\", \"OUTPUT_FORMAT\", \"SERDE_ID\", \"CD_ID\", \"IS_STOREDASSUBDIRECTORIES\")"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ";
    try (PreparedStatement pst = dbConn.prepareStatement(insertSDInfo)) {
      long numRecords = 0;
      for (Partition part : parts) {
        StorageDescriptor sd = part.getSd();
        if (sd == null) {
          continue;
        }
        pst.setLong(1, sdIdIdx++);
        pst.setObject(2, sd.getInputFormat());
        pst.setObject(4, sd.getLocation());
        pst.setObject(5, sd.getNumBuckets());
        pst.setObject(6, sd.getOutputFormat());
        pst.setLong(7, serdeIdIdx++);
        pst.setObject(8, sd.getCols() == null ? null : cdIdIdx);
        cdIdIdx++;

        if (dbType.isDERBY()) {
          // In Derby schema file, constraint is added for the value to be either Y or N.
          pst.setObject(3, sd.isCompressed() ? "Y" : "N");
          pst.setObject(9, sd.isStoredAsSubDirectories() ? "Y" : "N");
        } else if (dbType.isORACLE()) {
          // In Oracle schema file, constraint is added for the value to be either 1 or 0.
          pst.setObject(3, sd.isCompressed() ? 1 : 0);
          pst.setObject(9, sd.isStoredAsSubDirectories() ? 1 : 0);
        } else if (dbType.isMYSQL() || dbType.isPOSTGRES() || dbType.isSQLSERVER()) {
          // For postgres the column is of type is boolean. For mysql its bit(1) and for sql server its bit.
          // For both, the conversion is done automatically from boolean to bit.
          pst.setBoolean(3, sd.isCompressed());
          pst.setBoolean(9, sd.isStoredAsSubDirectories());
        } else {
          throw new IllegalArgumentException("Unsupported DB type: " + dbType);
        }

        numRecords = addBatch(pst, numRecords, maxBatchSize);
      }
      executeBatch(pst, numRecords);
    }
  }

  public static void addColV2Info(Connection dbConn, List<Partition> parts, long cdIdIdx, long maxBatchSize)
          throws SQLException {
    String insertColInfo = "INSERT INTO \"COLUMNS_V2\" (\"CD_ID\", \"COMMENT\", \"COLUMN_NAME\", \"TYPE_NAME\","
            + " \"INTEGER_IDX\") VALUES (?, ?, ?, ?, ?) ";
    try (PreparedStatement pst = dbConn.prepareStatement(insertColInfo)) {
      long numRecords = 0;
      for (Partition part : parts) {
        StorageDescriptor sd = part.getSd();
        if (sd == null) {
          continue;
        }
        List<FieldSchema> cols = sd.getCols();
        if (cols == null) {
          continue;
        }
        int idx = 0;
        for (FieldSchema col : cols) {
          pst.setLong(1, cdIdIdx);
          pst.setString(2, col.getComment());
          pst.setString(3, col.getName());
          pst.setString(4, col.getType());
          pst.setLong(5, idx++);
          LOG.debug("Executing insert numRecords " + numRecords  + " maxBatchSize " + maxBatchSize +
                          insertColInfo.replaceAll("\\?", "{}"),
                  cdIdIdx, col.getComment(), col.getName(), col.getType(), idx);
          numRecords = addBatch(pst, numRecords, maxBatchSize);
        }
        cdIdIdx++;
      }
      executeBatch(pst, numRecords);
    }
  }

  public static void addToSortColsTable(Connection dbConn, List<Partition> parts, long sdId, long maxBatchSize)
          throws SQLException {
    String insertColInfo = "INSERT INTO \"SORT_COLS\" (\"SD_ID\", \"COLUMN_NAME\", \"ORDER\","
            + " \"INTEGER_IDX\") VALUES (?, ?, ?, ?) ";
    try (PreparedStatement pst = dbConn.prepareStatement(insertColInfo)) {
      long numRecords = 0;
      for (Partition part : parts) {
        StorageDescriptor sd = part.getSd();
        if (sd == null) {
          continue;
        }
        List<Order> cols = sd.getSortCols();
        if (cols == null) {
          continue;
        }
        int idx = 0;
        for (Order col : cols) {
          pst.setLong(1, sdId);
          pst.setString(2, col.getCol());
          pst.setLong(3, col.getOrder());
          pst.setLong(4, idx++);
          LOG.debug("Executing insert numRecords " + numRecords  + " maxBatchSize " + maxBatchSize +
                          insertColInfo.replaceAll("\\?", "{}"),
                  sdId, col.getCol(), col.getOrder(), idx);
          numRecords = addBatch(pst, numRecords, maxBatchSize);
        }
        sdId++;
      }
      executeBatch(pst, numRecords);
    }
  }

  public static void addToBucketColsTable(Connection dbConn, List<Partition> parts, long sdId, long maxBatchSize)
          throws SQLException {
    String insertColInfo = "INSERT INTO \"BUCKETING_COLS\" (\"SD_ID\", \"BUCKET_COL_NAME\","
            + " \"INTEGER_IDX\") VALUES (?, ?, ?) ";
    try (PreparedStatement pst = dbConn.prepareStatement(insertColInfo)) {
      long numRecords = 0;
      for (Partition part : parts) {
        StorageDescriptor sd = part.getSd();
        if (sd == null) {
          continue;
        }
        List<String> cols = sd.getBucketCols();
        if (cols == null) {
          continue;
        }
        int idx = 0;
        for (String col : cols) {
          pst.setLong(1, sdId);
          pst.setString(2, col);
          pst.setLong(3, idx++);
          LOG.debug("Executing insert numRecords " + numRecords  + " maxBatchSize " + maxBatchSize +
                          insertColInfo.replaceAll("\\?", "{}"), sdId, col, idx);
          numRecords = addBatch(pst, numRecords, maxBatchSize);
        }
        sdId++;
      }
      executeBatch(pst, numRecords);
    }
  }

  public static long addSkewedColsName(Connection dbConn, List<Partition> parts, long sdId, long maxBatchSize)
          throws SQLException {
    String insertColInfo = "INSERT INTO \"SKEWED_COL_NAMES\" (\"SD_ID\", \"SKEWED_COL_NAME\","
            + " \"INTEGER_IDX\") VALUES (?, ?, ?) ";
    long numSkewedString = 0;
    try (PreparedStatement pst = dbConn.prepareStatement(insertColInfo)) {
      long numRecords = 0;
      for (Partition part : parts) {
        StorageDescriptor sd = part.getSd();
        if (sd == null) {
          continue;
        }
        SkewedInfo skewedInfo = sd.getSkewedInfo();
        if (skewedInfo == null) {
          continue;
        }
        int idx = 0;
        for (String col : skewedInfo.getSkewedColNames()) {
          pst.setLong(1, sdId);
          pst.setString(2, col);
          pst.setLong(3, idx++);
          LOG.debug("Executing insert numRecords " + numRecords  + " maxBatchSize " + maxBatchSize +
                          insertColInfo.replaceAll("\\?", "{}"), sdId, col, idx);
          numRecords = addBatch(pst, numRecords, maxBatchSize);
        }
        numSkewedString += skewedInfo.getSkewedColValues().size();
        sdId++;
      }
      executeBatch(pst, numRecords);
    }
    return numSkewedString;
  }

  public static long addSkewedStringListId(Connection dbConn, long maxBatchSize, long numSkewedString)
          throws SQLException {
    long maxListId = 1;
    ResultSet rs = null;
    try (Statement statement = dbConn.createStatement()) {
      rs = statement.executeQuery("SELECT MAX(\"STRING_LIST_ID\") FROM \"SKEWED_STRING_LIST\"");
      if (rs.next()) {
        maxListId = rs.getLong(1) + 1;
      }
    } finally {
      close(rs);
    }

    String insertSDParamInfo = "INSERT INTO \"SKEWED_STRING_LIST\" (\"STRING_LIST_ID\") VALUES (?)";
    try (PreparedStatement pst = dbConn.prepareStatement(insertSDParamInfo)) {
      long numRecords = 0;
      for (long idx = 0; idx < numSkewedString; idx++) {
        pst.setLong(1, maxListId + idx);
        numRecords = addBatch(pst, numRecords, maxBatchSize);
      }
      executeBatch(pst, numRecords);
    }
    return maxListId;
  }

  public static void addSkewedStringListValues(Connection dbConn, List<Partition> parts,
                                               long listId, long sdId, long maxBatchSize) throws SQLException {
    String insertSkewedListVal = "INSERT INTO \"SKEWED_STRING_LIST_VALUES\" (\"STRING_LIST_ID\"," +
            " \"STRING_LIST_VALUE\", \"INTEGER_IDX\") VALUES (?, ?, ?)";
    String insertSkewedVal = "INSERT INTO \"SKEWED_VALUES\" (\"SD_ID_OID\", \"STRING_LIST_ID_EID\"," +
            " \"INTEGER_IDX\") VALUES (?, ?, ?)";
    String insertSkewedMap = "INSERT INTO \"SKEWED_COL_VALUE_LOC_MAP\" (\"SD_ID\", \"STRING_LIST_ID_KID\"," +
            " \"LOCATION\") VALUES (?, ?, ?)";
    PreparedStatement pstValues = null;
    PreparedStatement pstMap = null;
    try (PreparedStatement pst = dbConn.prepareStatement(insertSkewedListVal)) {
      pstValues = dbConn.prepareStatement(insertSkewedVal);
      pstMap = dbConn.prepareStatement(insertSkewedMap);
      long numRecords = 0;
      long numRecordsVals = 0;
      long numRecordsMap = 0;
      for (Partition part : parts) {
        StorageDescriptor sd = part.getSd();
        if (sd == null) {
          continue;
        }
        SkewedInfo skewedInfo = sd.getSkewedInfo();
        if (skewedInfo == null) {
          continue;
        }
        for (List<String> values : skewedInfo.getSkewedColValues()) {
          long idx = 0;
          long idx1 = 0;
          for (String value : values) {
            pst.setLong(1, listId);
            pst.setString(2, value);
            pst.setLong(3, idx++);
            numRecords = addBatch(pst, numRecords, maxBatchSize);

            pstValues.setLong(1, sdId);
            pstValues.setLong(2, listId);
            pstValues.setLong(3, idx1++);
            numRecordsVals = addBatch(pstValues, numRecordsVals, maxBatchSize);
          }

          pstMap.setLong(1, sdId);
          pstMap.setLong(2, listId);
          pstMap.setString(3, skewedInfo.getSkewedColValueLocationMaps().get(values));
          numRecordsMap = addBatch(pstMap, numRecordsMap, maxBatchSize);

          listId++;
        }
        sdId++;
      }
      executeBatch(pst, numRecords);
      executeBatch(pstValues, numRecordsVals);
      executeBatch(pstMap, numRecordsMap);
    } finally {
      if (pstValues != null) {
        pstValues.close();
      }

      if (pstMap != null) {
        pstMap.close();
      }
    }
  }

  public static void addSDParaInfo(Connection dbConn, List<Partition> parts, long sdIdIdx, long maxBatchSize)
          throws SQLException {
    String insertSDParamInfo = "INSERT INTO \"SD_PARAMS\" (\"SD_ID\", \"PARAM_KEY\", \"PARAM_VALUE\")" +
            " VALUES (?, ?, ?) ";
    try (PreparedStatement pst = dbConn.prepareStatement(insertSDParamInfo)) {
      long numRecords = 0;
      for (Partition part : parts) {
        StorageDescriptor sd = part.getSd();
        if (sd == null) {
          continue;
        }
        for (Map.Entry entry : sd.getParameters().entrySet()) {
          String key = (String) entry.getKey();
          String value = (String) entry.getValue();
          pst.setLong(1, sdIdIdx);
          pst.setString(2, key);
          pst.setString(3, value);
          numRecords = addBatch(pst, numRecords, maxBatchSize);
        }
        sdIdIdx++;
      }
      executeBatch(pst, numRecords);
    }
  }

  public static void addSerdeParaInfo(Connection dbConn, List<Partition> parts, long serdeIdIdx, long maxBatchSize)
          throws SQLException {
    String insertSerdePara
            = "INSERT INTO \"SERDE_PARAMS\" (\"SERDE_ID\", \"PARAM_KEY\", \"PARAM_VALUE\") VALUES (?, ?, ?) ";
    try (PreparedStatement pst = dbConn.prepareStatement(insertSerdePara)) {
      long numRecords = 0;
      for (Partition part : parts) {
        if (part.getSd() == null) {
          continue;
        }
        SerDeInfo serDeInfo = part.getSd().getSerdeInfo();
        for (Map.Entry entry : serDeInfo.getParameters().entrySet()) {
          String key = (String) entry.getKey();
          String value = (String) entry.getValue();
          pst.setLong(1, serdeIdIdx);
          pst.setString(2, key);
          pst.setString(3, value);
          numRecords = addBatch(pst, numRecords, maxBatchSize);
        }
        serdeIdIdx++;
      }
      executeBatch(pst, numRecords);
    }
  }

  public static void addPartitionInfo(Connection dbConn, List<Partition> parts, List<FieldSchema> partKeys,
                                       long tblId, long partIdIdx, long sdIdIdx, long maxBatchSize)
          throws MetaException, SQLException {
    String insertPartition = "INSERT INTO \"PARTITIONS\" (\"PART_ID\", \"CREATE_TIME\", \"LAST_ACCESS_TIME\", "
            + "\"PART_NAME\", \"SD_ID\", \"TBL_ID\", \"WRITE_ID\") VALUES (?, ?, ?, ?, ?, ?, ?) ";
    try (PreparedStatement pst = dbConn.prepareStatement(insertPartition)) {
      long numRecords = 0;
      for (Partition part : parts) {
        pst.setLong(1, partIdIdx++);
        pst.setObject(2, part.getCreateTime());
        pst.setObject(3, part.getLastAccessTime());
        pst.setString(4,
                makePartName(partKeys, part.getValues(), null));
        pst.setObject(5, part.getSd() == null ? null : sdIdIdx);
        pst.setLong(6, tblId);
        pst.setLong(7, part.getWriteId());
        sdIdIdx++;
        numRecords = addBatch(pst, numRecords, maxBatchSize);
      }
      executeBatch(pst, numRecords);
    }
  }

  public static void addPartitionParaInfo(Connection dbConn, List<Partition> parts,
                                          long partIdIdx, long maxBatchSize) throws SQLException {
    String insertParamKeys =
            "INSERT INTO \"PARTITION_PARAMS\" (\"PART_ID\", \"PARAM_KEY\", \"PARAM_VALUE\") VALUES (?, ?, ?)";
    try (PreparedStatement pst = dbConn.prepareStatement(insertParamKeys)) {
      long numRecords = 0;
      for (Partition part : parts) {
        for (Map.Entry entry : part.getParameters().entrySet()) {
          String key = (String) entry.getKey();
          String value = (String) entry.getValue();
          pst.setLong(1, partIdIdx);
          pst.setString(2, key);
          pst.setString(3, value);
          numRecords = addBatch(pst, numRecords, maxBatchSize);
        }
        partIdIdx++;
      }
      executeBatch(pst, numRecords);
    }
  }

  public static void addPartitionKeyValInfo(Connection dbConn, List<Partition> parts,
                                            long partIdIdx, long maxBatchSize) throws SQLException {
    String insertPartKeyVals
            = "INSERT INTO \"PARTITION_KEY_VALS\" (\"PART_ID\", \"PART_KEY_VAL\", \"INTEGER_IDX\") VALUES (?, ?, ?)";
    try (PreparedStatement pst = dbConn.prepareStatement(insertPartKeyVals)) {
      long numRecords = 0;
      for (Partition part : parts) {
        int idx = 0;
        for (String value : part.getValues()) {
          pst.setLong(1, partIdIdx);
          pst.setString(2, value);
          pst.setLong(3, idx++);
          numRecords = addBatch(pst, numRecords, maxBatchSize);
        }
        partIdIdx++;
      }
      executeBatch(pst, numRecords);
    }
  }
}
