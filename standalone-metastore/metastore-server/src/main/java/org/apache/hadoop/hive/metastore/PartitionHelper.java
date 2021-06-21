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
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
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

  /**
   * Gets the next value from SEQUENCE_TABLE table for given sequence. The sequence value is updated by numValues and
   * the current value is returned. write lock is taken on the SEQUENCE_TABLE to avoid race condition of multiple
   * thread trying to insert the initial value.
   * @return The sequence value before update.
   */
  public static long getNextValueFromSequenceTable(Connection dbConn, String seqName, long numValues,
                                            DatabaseProduct dbType) throws MetaException {
    try {
      try (Statement stmt = dbConn.createStatement()) {
        String lockTableSql = dbType.lockTable("SEQUENCE_TABLE", false);
        stmt.executeUpdate(lockTableSql);
      }

      String updateQuery;
      ResultSet rs = null;
      long currValue;
      try (Statement stmt = dbConn.createStatement()) {
        String query = "SELECT \"NEXT_VAL\" FROM \"SEQUENCE_TABLE\" WHERE \"SEQUENCE_NAME\"= " + quoteString(seqName);
        rs = stmt.executeQuery(query);
        if (rs.next()) {
          currValue = rs.getLong(1);
          long nextValue = currValue + numValues;
          updateQuery = "UPDATE \"SEQUENCE_TABLE\" SET \"NEXT_VAL\" = " + nextValue + " WHERE \"SEQUENCE_NAME\" = "
                  + quoteString(seqName);
        } else {
          currValue = 1;
          long nextValue = currValue + numValues ;
          updateQuery = "INSERT INTO \"SEQUENCE_TABLE\" (\"SEQUENCE_NAME\", \"NEXT_VAL\")  VALUES ( "
                  + quoteString(seqName) + "," + nextValue
                  + ")";
        }
      } finally {
        close(rs);
      }

      try (Statement stmt = dbConn.createStatement()) {
        stmt.executeUpdate(updateQuery);
      }
      return currValue;
    } catch (SQLException e){
      LOG.error("Failed to get next value for sequence " + seqName + " with numValues " + numValues, e);
      throw new MetaException(e.getMessage());
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

  public static void addPartitionPrivilegeInfo(Connection dbConn, List<Partition> parts,
                                                long tblId, long partId, long grantId ) throws SQLException {
    ResultSet rs = null;
    long numPartGrant = 0;
    try (Statement statement = dbConn.createStatement()) {
      String query = "SELECT COUNT(*) FROM \"TBL_PRIVS\" WHERE  \"TBL_ID\" = " + tblId;
      rs = statement.executeQuery(query);
      if (rs.next()) {
        numPartGrant = rs.getLong(1);
      }
    } finally {
      close(rs);
    }

    if (numPartGrant == 0) {
      return;
    }

    long grantIdIdx = grantId;
    int now = (int) (System.currentTimeMillis() / 1000);
    String insertPartPrev = "INSERT INTO \"PART_PRIVS\" "
            + " (\"PART_GRANT_ID\", \"CREATE_TIME\", \"GRANT_OPTION\", \"GRANTOR\", \"GRANTOR_TYPE\", \"PART_ID\","
            + " \"PRINCIPAL_NAME\", \"PRINCIPAL_TYPE\", \"PART_PRIV\", \"AUTHORIZER\") "
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";
    List<Integer> grantOptionList = new ArrayList<>();
    List<String> grantorList = new ArrayList<>();
    List<String> grantTypeList = new ArrayList<>();
    List<String> principalNameList = new ArrayList<>();
    List<String> principalTypeList = new ArrayList<>();
    List<String> tblPrivList = new ArrayList<>();
    List<String> authorizerList = new ArrayList<>();

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

    try (PreparedStatement pst = dbConn.prepareStatement(insertPartPrev)) {
      long partIdx = partId;
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
          pst.addBatch();
        }
        partIdx++;
      }
    }
  }

  public static void addPartitionColPrivilegeInfo(Connection dbConn, List<Partition> parts,
                                               long tblId, long startPartId, long startGrantId ) throws SQLException {
    ResultSet rs = null;
    long numPartColGrant = 0;
    try (Statement statement = dbConn.createStatement()) {
      String query = "SELECT COUNT(*) FROM \"TBL_COL_PRIVS\" WHERE  \"TBL_ID\" = " + tblId;
      rs = statement.executeQuery(query);
      if (rs.next()) {
        numPartColGrant = rs.getLong(1);
      }
    } finally {
      close(rs);
    }

    if (numPartColGrant == 0) {
      return;
    }

    List<String> colNameList = new ArrayList<>();
    List<Integer> grantOptionList = new ArrayList<>();
    List<String> grantorList = new ArrayList<>();
    List<String> grantTypeList = new ArrayList<>();
    List<String> principalNameList = new ArrayList<>();
    List<String> principalTypeList = new ArrayList<>();
    List<String> tblColPrivList = new ArrayList<>();
    List<String> authorizerList = new ArrayList<>();
    rs = null;
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

    String insertPartPrev = "INSERT INTO \"PART_COL_PRIVS\" "
            + " (\"PART_COLUMN_GRANT_ID\", \"COLUMN_NAME\", \"CREATE_TIME\", \"GRANT_OPTION\", \"GRANTOR\","
            + " \"GRANTOR_TYPE\", \"PART_ID\","
            + " \"PRINCIPAL_NAME\", \"PRINCIPAL_TYPE\", \"PART_COL_PRIV\", \"AUTHORIZER\") "
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";
    try (PreparedStatement pst = dbConn.prepareStatement(insertPartPrev)) {
      long partIdx = startPartId;
      long grantIdIdx = startGrantId;
      int now = (int) (System.currentTimeMillis() / 1000);
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
          pst.addBatch();
        }
        partIdx++;
      }
    }
  }

  public static void addSerdeInfo(Connection dbConn, List<Partition> parts, long serdeIdIdx) throws SQLException {
    String insertSerdeInfo = "INSERT INTO \"SERDES\" (\"SERDE_ID\", \"NAME\", \"SLIB\", \"DESCRIPTION\","
            + " \"SERIALIZER_CLASS\", \"DESERIALIZER_CLASS\", \"SERDE_TYPE\") VALUES (?, ?, ?, ?, ?, ?, ?) ";
    try (PreparedStatement preparedStatement = dbConn.prepareStatement(insertSerdeInfo)) {
      for (Partition part : parts) {
        if (part.getSd() == null) {
          continue;
        }
        SerDeInfo serDeInfo = part.getSd().getSerdeInfo();
        preparedStatement.setLong(1, serdeIdIdx++);
        preparedStatement.setObject(2, serDeInfo.getName());
        preparedStatement.setObject(3, serDeInfo.getSerializationLib());
        preparedStatement.setObject(4, serDeInfo.getDescription());
        preparedStatement.setObject(5, serDeInfo.getSerializerClass());
        preparedStatement.setObject(6, serDeInfo.getDeserializerClass());
        preparedStatement.setLong(7, serDeInfo.getSerdeType() != null ?
                serDeInfo.getSerdeType().getValue() : 0);
        preparedStatement.addBatch();
      }
      preparedStatement.executeBatch();
    }
  }

  public static void addColDescInfo(Connection dbConn, List<Partition> parts, long cdIdIdx) throws SQLException {
    String insertCDInfo = "INSERT INTO \"CDS\" (\"CD_ID\") VALUES (?) ";
    try (PreparedStatement preparedStatement = dbConn.prepareStatement(insertCDInfo)) {
      for (Partition ignored : parts) {
        preparedStatement.setLong(1, cdIdIdx++);
        preparedStatement.addBatch();
      }
      preparedStatement.executeBatch();
    }
  }

  public static void addSDInfo(Connection dbConn, List<Partition> parts, long sdIdIdx,
                               long serdeIdIdx, long cdIdIdx, DatabaseProduct dbType) throws SQLException {
    String insertSDInfo = "INSERT INTO \"SDS\" (\"SD_ID\", \"INPUT_FORMAT\", \"IS_COMPRESSED\", \"LOCATION\","
            + " \"NUM_BUCKETS\", \"OUTPUT_FORMAT\", \"SERDE_ID\", \"CD_ID\", \"IS_STOREDASSUBDIRECTORIES\")"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ";
    try (PreparedStatement preparedStatement = dbConn.prepareStatement(insertSDInfo)) {
      for (Partition part : parts) {
        StorageDescriptor sd = part.getSd();
        if (sd == null) {
          continue;
        }
        preparedStatement.setLong(1, sdIdIdx++);
        preparedStatement.setObject(2, sd.getInputFormat());
        preparedStatement.setObject(4, sd.getLocation());
        preparedStatement.setObject(5, sd.getNumBuckets());
        preparedStatement.setObject(6, sd.getOutputFormat());
        preparedStatement.setLong(7, serdeIdIdx++);
        preparedStatement.setObject(8, sd.getCols() == null ? null : cdIdIdx);
        cdIdIdx++;

        if (dbType.isDERBY()) {
          // In Derby schema file, constraint is added for the value to be either Y or N.
          preparedStatement.setObject(3, sd.isCompressed() ? "Y" : "N");
          preparedStatement.setObject(9, sd.isStoredAsSubDirectories() ? "Y" : "N");
        } else if (dbType.isORACLE()) {
          // In Oracle schema file, constraint is added for the value to be either 1 or 0.
          preparedStatement.setObject(3, sd.isCompressed() ? 1 : 0);
          preparedStatement.setObject(9, sd.isStoredAsSubDirectories() ? 1 : 0);
        } else if (dbType.isMYSQL() || dbType.isPOSTGRES() || dbType.isSQLSERVER()) {
          // For postgres the column is of type is boolean. For mysql its bit(1) and for sql server its bit.
          // For both, the conversion is done automatically from boolean to bit.
          preparedStatement.setBoolean(3, sd.isCompressed());
          preparedStatement.setBoolean(9, sd.isStoredAsSubDirectories());
        } else {
          throw new IllegalArgumentException("Unsupported DB type: " + dbType);
        }

        preparedStatement.addBatch();
      }
      preparedStatement.executeBatch();
    }
  }

  public static void addColV2Info(Connection dbConn, List<Partition> parts, long cdIdIdx) throws SQLException {
    String insertColInfo = "INSERT INTO \"COLUMNS_V2\" (\"CD_ID\", \"COMMENT\", \"COLUMN_NAME\", \"TYPE_NAME\","
            + " \"INTEGER_IDX\") VALUES (?, ?, ?, ?, ?) ";
    try (PreparedStatement preparedStatement = dbConn.prepareStatement(insertColInfo)) {
      for (Partition part : parts) {
        StorageDescriptor sd = part.getSd();
        if (sd == null) {
          continue;
        }
        List<FieldSchema> cols = sd.getCols();
        if (cols == null) {
          continue;
        }
        int colIdx = 0;
        for (FieldSchema col : cols) {
          preparedStatement.setLong(1, cdIdIdx);
          preparedStatement.setString(2, col.getComment());
          preparedStatement.setString(3, col.getName());
          preparedStatement.setString(4, col.getType());
          preparedStatement.setLong(5, colIdx++);
          preparedStatement.addBatch();
        }
        cdIdIdx++;
      }
      preparedStatement.executeBatch();
    }
  }

  public static void addSDParaInfo(Connection dbConn, List<Partition> parts, long sdIdIdx) throws SQLException {
    String insertSDParamInfo = "INSERT INTO \"SD_PARAMS\" (\"SD_ID\", \"PARAM_KEY\", \"PARAM_VALUE\")" +
            " VALUES (?, ?, ?) ";
    try (PreparedStatement preparedStatement = dbConn.prepareStatement(insertSDParamInfo)) {
      for (Partition part : parts) {
        StorageDescriptor sd = part.getSd();
        if (sd == null) {
          continue;
        }
        for (Map.Entry entry : sd.getParameters().entrySet()) {
          String key = (String) entry.getKey();
          String value = (String) entry.getValue();
          preparedStatement.setLong(1, sdIdIdx);
          preparedStatement.setString(2, key);
          preparedStatement.setString(3, value);
          preparedStatement.addBatch();
        }
        sdIdIdx++;
      }
      preparedStatement.executeBatch();
    }
  }

  public static void addSerdeParaInfo(Connection dbConn, List<Partition> parts, long serdeIdIdx) throws SQLException {
    String insertSerdePara
            = "INSERT INTO \"SERDE_PARAMS\" (\"SERDE_ID\", \"PARAM_KEY\", \"PARAM_VALUE\") VALUES (?, ?, ?) ";
    try (PreparedStatement preparedStatement = dbConn.prepareStatement(insertSerdePara)) {
      for (Partition part : parts) {
        if (part.getSd() == null) {
          continue;
        }
        SerDeInfo serDeInfo = part.getSd().getSerdeInfo();
        for (Map.Entry entry : serDeInfo.getParameters().entrySet()) {
          String key = (String) entry.getKey();
          String value = (String) entry.getValue();
          preparedStatement.setLong(1, serdeIdIdx);
          preparedStatement.setString(2, key);
          preparedStatement.setString(3, value);
          preparedStatement.addBatch();
        }
        serdeIdIdx++;
      }
      preparedStatement.executeBatch();
    }
  }

  public static void addPartitionInfo(Connection dbConn, List<Partition> parts, List<FieldSchema> partKeys,
                                       long tblId, long partIdIdx, long sdIdIdx) throws MetaException, SQLException {
    String insertPartition = "INSERT INTO \"PARTITIONS\" (\"PART_ID\", \"CREATE_TIME\", \"LAST_ACCESS_TIME\", "
            + "\"PART_NAME\", \"SD_ID\", \"TBL_ID\", \"WRITE_ID\") VALUES (?, ?, ?, ?, ?, ?, ?) ";
    try (PreparedStatement preparedStatement = dbConn.prepareStatement(insertPartition)) {
      for (Partition part : parts) {
        preparedStatement.setLong(1, partIdIdx++);
        preparedStatement.setObject(2, part.getCreateTime());
        preparedStatement.setObject(3, part.getLastAccessTime());
        preparedStatement.setString(4,
                makePartName(partKeys, part.getValues(), null));
        preparedStatement.setObject(5, part.getSd() == null ? null : sdIdIdx);
        preparedStatement.setLong(6, tblId);
        preparedStatement.setLong(7, part.getWriteId());
        sdIdIdx++;
        preparedStatement.addBatch();
      }
      preparedStatement.executeBatch();
    }
  }

  public static void addPartitionParaInfo(Connection dbConn, List<Partition> parts,
                                          long partIdIdx) throws SQLException {
    String insertParamKeys =
            "INSERT INTO \"PARTITION_PARAMS\" (\"PART_ID\", \"PARAM_KEY\", \"PARAM_VALUE\") VALUES (?, ?, ?) ";
    try (PreparedStatement preparedStatement = dbConn.prepareStatement(insertParamKeys)) {
      for (Partition part : parts) {
        for (Map.Entry entry : part.getParameters().entrySet()) {
          String key = (String) entry.getKey();
          String value = (String) entry.getValue();
          preparedStatement.setLong(1, partIdIdx);
          preparedStatement.setString(2, key);
          preparedStatement.setString(3, value);
          preparedStatement.addBatch();
        }
        partIdIdx++;
      }
      preparedStatement.executeBatch();
    }
  }

  public static void addPartitionKeyValInfo(Connection dbConn, List<Partition> parts,
                                            long partIdIdx) throws SQLException {
    String insertPartKeyVals
            = "INSERT INTO \"PARTITION_KEY_VALS\" (\"PART_ID\", \"PART_KEY_VAL\", \"INTEGER_IDX\") VALUES (?, ?, ?) ";
    try (PreparedStatement preparedStatement = dbConn.prepareStatement(insertPartKeyVals)) {
      for (Partition part : parts) {
        int idx = 0;
        for (String value : part.getValues()) {
          preparedStatement.setLong(1, partIdIdx);
          preparedStatement.setString(2, value);
          preparedStatement.setLong(3, idx++);
          preparedStatement.addBatch();
        }
        partIdIdx++;
      }
      preparedStatement.executeBatch();
    }
  }
}
