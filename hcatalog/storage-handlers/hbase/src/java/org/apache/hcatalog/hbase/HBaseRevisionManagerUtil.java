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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.hbase.HBaseSerDe;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.hbase.snapshot.RevisionManager;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerFactory;
import org.apache.hcatalog.hbase.snapshot.TableSnapshot;
import org.apache.hcatalog.hbase.snapshot.Transaction;
import org.apache.hcatalog.mapreduce.HCatTableInfo;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.apache.hcatalog.mapreduce.OutputJobInfo;
import org.apache.hcatalog.mapreduce.StorerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Class HBaseRevisionManagerUtil has utility methods to interact with Revision Manager
 *
 */
class HBaseRevisionManagerUtil {

  private final static Logger LOG = LoggerFactory.getLogger(HBaseRevisionManagerUtil.class);

  private HBaseRevisionManagerUtil() {
  }

  /**
   * Creates the latest snapshot of the table.
   *
   * @param jobConf The job configuration.
   * @param hbaseTableName The fully qualified name of the HBase table.
   * @param tableInfo HCat table information
   * @return An instance of HCatTableSnapshot
   * @throws IOException Signals that an I/O exception has occurred.
   */
  static HCatTableSnapshot createSnapshot(Configuration jobConf,
                      String hbaseTableName, HCatTableInfo tableInfo) throws IOException {

    RevisionManager rm = null;
    TableSnapshot snpt;
    try {
      rm = getOpenedRevisionManager(jobConf);
      snpt = rm.createSnapshot(hbaseTableName);
    } finally {
      closeRevisionManagerQuietly(rm);
    }

    HCatTableSnapshot hcatSnapshot = HBaseRevisionManagerUtil.convertSnapshot(snpt, tableInfo);
    return hcatSnapshot;
  }

  /**
   * Creates the snapshot using the revision specified by the user.
   *
   * @param jobConf The job configuration.
   * @param tableName The fully qualified name of the table whose snapshot is being taken.
   * @param revision The revision number to use for the snapshot.
   * @return An instance of HCatTableSnapshot.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  static HCatTableSnapshot createSnapshot(Configuration jobConf,
                      String tableName, long revision)
    throws IOException {

    TableSnapshot snpt;
    RevisionManager rm = null;
    try {
      rm = getOpenedRevisionManager(jobConf);
      snpt = rm.createSnapshot(tableName, revision);
    } finally {
      closeRevisionManagerQuietly(rm);
    }

    String inputJobString = jobConf.get(HCatConstants.HCAT_KEY_JOB_INFO);
    if (inputJobString == null) {
      throw new IOException(
        "InputJobInfo information not found in JobContext. "
          + "HCatInputFormat.setInput() not called?");
    }
    InputJobInfo inputInfo = (InputJobInfo) HCatUtil.deserialize(inputJobString);
    HCatTableSnapshot hcatSnapshot = HBaseRevisionManagerUtil
      .convertSnapshot(snpt, inputInfo.getTableInfo());

    return hcatSnapshot;
  }

  /**
   * Gets an instance of revision manager which is opened.
   *
   * @param jobConf The job configuration.
   * @return RevisionManager An instance of revision manager.
   * @throws IOException
   */
  static RevisionManager getOpenedRevisionManager(Configuration jobConf) throws IOException {
    return RevisionManagerFactory.getOpenedRevisionManager(jobConf);
  }

  static void closeRevisionManagerQuietly(RevisionManager rm) {
    if (rm != null) {
      try {
        rm.close();
      } catch (IOException e) {
        LOG.warn("Error while trying to close revision manager", e);
      }
    }
  }


  static HCatTableSnapshot convertSnapshot(TableSnapshot hbaseSnapshot,
                       HCatTableInfo hcatTableInfo) throws IOException {

    HCatSchema hcatTableSchema = hcatTableInfo.getDataColumns();
    Map<String, String> hcatHbaseColMap = getHCatHBaseColumnMapping(hcatTableInfo);
    HashMap<String, Long> revisionMap = new HashMap<String, Long>();

    for (HCatFieldSchema fSchema : hcatTableSchema.getFields()) {
      if (hcatHbaseColMap.containsKey(fSchema.getName())) {
        String colFamily = hcatHbaseColMap.get(fSchema.getName());
        long revisionID = hbaseSnapshot.getRevision(colFamily);
        revisionMap.put(fSchema.getName(), revisionID);
      }
    }

    HCatTableSnapshot hcatSnapshot = new HCatTableSnapshot(
      hcatTableInfo.getDatabaseName(), hcatTableInfo.getTableName(), revisionMap, hbaseSnapshot.getLatestRevision());
    return hcatSnapshot;
  }

  static TableSnapshot convertSnapshot(HCatTableSnapshot hcatSnapshot,
                     HCatTableInfo hcatTableInfo) throws IOException {

    HCatSchema hcatTableSchema = hcatTableInfo.getDataColumns();
    Map<String, Long> revisionMap = new HashMap<String, Long>();
    Map<String, String> hcatHbaseColMap = getHCatHBaseColumnMapping(hcatTableInfo);
    for (HCatFieldSchema fSchema : hcatTableSchema.getFields()) {
      String colFamily = hcatHbaseColMap.get(fSchema.getName());
      if (hcatSnapshot.containsColumn(fSchema.getName())) {
        long revision = hcatSnapshot.getRevision(fSchema.getName());
        revisionMap.put(colFamily, revision);
      }
    }

    String fullyQualifiedName = hcatSnapshot.getDatabaseName() + "."
      + hcatSnapshot.getTableName();
    return new TableSnapshot(fullyQualifiedName, revisionMap, hcatSnapshot.getLatestRevision());

  }

  /**
   * Begins a transaction in the revision manager for the given table.
   * @param qualifiedTableName Name of the table
   * @param tableInfo HCat Table information
   * @param jobConf Job Configuration
   * @return The new transaction in revision manager
   * @throws IOException
   */
  static Transaction beginWriteTransaction(String qualifiedTableName,
                       HCatTableInfo tableInfo, Configuration jobConf) throws IOException {
    Transaction txn;
    RevisionManager rm = null;
    try {
      rm = HBaseRevisionManagerUtil.getOpenedRevisionManager(jobConf);
      String hBaseColumns = tableInfo.getStorerInfo().getProperties()
        .getProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING);
      String[] splits = hBaseColumns.split("[,:]");
      Set<String> families = new HashSet<String>();
      for (int i = 0; i < splits.length; i += 2) {
        if (!splits[i].isEmpty())
          families.add(splits[i]);
      }
      txn = rm.beginWriteTransaction(qualifiedTableName, new ArrayList<String>(families));
    } finally {
      HBaseRevisionManagerUtil.closeRevisionManagerQuietly(rm);
    }
    return txn;
  }

  static Transaction getWriteTransaction(Configuration conf) throws IOException {
    OutputJobInfo outputJobInfo = (OutputJobInfo) HCatUtil.deserialize(conf.get(HCatConstants.HCAT_KEY_OUTPUT_INFO));
    return (Transaction) HCatUtil.deserialize(outputJobInfo.getProperties()
      .getProperty(HBaseConstants.PROPERTY_WRITE_TXN_KEY));
  }

  static void setWriteTransaction(Configuration conf, Transaction txn) throws IOException {
    OutputJobInfo outputJobInfo = (OutputJobInfo) HCatUtil.deserialize(conf.get(HCatConstants.HCAT_KEY_OUTPUT_INFO));
    outputJobInfo.getProperties().setProperty(HBaseConstants.PROPERTY_WRITE_TXN_KEY, HCatUtil.serialize(txn));
    conf.set(HCatConstants.HCAT_KEY_OUTPUT_INFO, HCatUtil.serialize(outputJobInfo));
  }

  /**
   * Get the Revision number that will be assigned to this job's output data
   * @param conf configuration of the job
   * @return the revision number used
   * @throws IOException
   */
  static long getOutputRevision(Configuration conf) throws IOException {
    return getWriteTransaction(conf).getRevisionNumber();
  }

  private static Map<String, String> getHCatHBaseColumnMapping(HCatTableInfo hcatTableInfo)
    throws IOException {

    HCatSchema hcatTableSchema = hcatTableInfo.getDataColumns();
    StorerInfo storeInfo = hcatTableInfo.getStorerInfo();
    String hbaseColumnMapping = storeInfo.getProperties().getProperty(
      HBaseSerDe.HBASE_COLUMNS_MAPPING);

    Map<String, String> hcatHbaseColMap = new HashMap<String, String>();
    List<String> columnFamilies = new ArrayList<String>();
    List<String> columnQualifiers = new ArrayList<String>();
    HBaseUtil.parseColumnMapping(hbaseColumnMapping, columnFamilies,
      null, columnQualifiers, null);

    for (HCatFieldSchema column : hcatTableSchema.getFields()) {
      int fieldPos = hcatTableSchema.getPosition(column.getName());
      String colFamily = columnFamilies.get(fieldPos);
      if (colFamily.equals(HBaseSerDe.HBASE_KEY_COL) == false) {
        hcatHbaseColMap.put(column.getName(), colFamily);
      }
    }

    return hcatHbaseColMap;
  }

}
