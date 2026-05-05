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

package org.apache.hadoop.hive.metastore.metastore.impl;

import com.google.common.base.Joiner;

import javax.jdo.Query;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.Batchable;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.directsql.MetaStoreDirectSql;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.PartitionProjectionEvaluator;
import org.apache.hadoop.hive.metastore.PersistenceManagerProvider;
import org.apache.hadoop.hive.metastore.QueryWrapper;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableFields;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionFilterMode;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRow;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metastore.RawStoreAware;
import org.apache.hadoop.hive.metastore.model.FetchGroups;
import org.apache.hadoop.hive.metastore.model.MColumnDescriptor;
import org.apache.hadoop.hive.metastore.model.MConstraint;
import org.apache.hadoop.hive.metastore.model.MCreationMetadata;
import org.apache.hadoop.hive.metastore.model.MMVSource;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MPartitionEvent;
import org.apache.hadoop.hive.metastore.model.MPartitionPrivilege;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MTableColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MTablePrivilege;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.metastore.GetHelper;
import org.apache.hadoop.hive.metastore.metastore.GetListHelper;
import org.apache.hadoop.hive.metastore.metastore.iface.TableStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.hadoop.hive.metastore.Batchable.NO_BATCHING;
import static org.apache.hadoop.hive.metastore.ObjectStore.appendPatternCondition;
import static org.apache.hadoop.hive.metastore.ObjectStore.appendSimpleCondition;
import static org.apache.hadoop.hive.metastore.ObjectStore.convertToCreationMetadata;
import static org.apache.hadoop.hive.metastore.ObjectStore.convertToFieldSchemas;
import static org.apache.hadoop.hive.metastore.ObjectStore.convertToMCreationMetadata;
import static org.apache.hadoop.hive.metastore.ObjectStore.convertToMPart;
import static org.apache.hadoop.hive.metastore.ObjectStore.convertToMTable;
import static org.apache.hadoop.hive.metastore.ObjectStore.convertToPart;
import static org.apache.hadoop.hive.metastore.ObjectStore.convertToParts;
import static org.apache.hadoop.hive.metastore.ObjectStore.convertToTable;
import static org.apache.hadoop.hive.metastore.ObjectStore.getJDOFilterStrForPartitionNames;
import static org.apache.hadoop.hive.metastore.ObjectStore.getPartQueryWithParams;
import static org.apache.hadoop.hive.metastore.ObjectStore.makeParameterDeclarationString;
import static org.apache.hadoop.hive.metastore.ObjectStore.putPersistentPrivObjects;
import static org.apache.hadoop.hive.metastore.ObjectStore.verifyStatsChangeCtx;
import static org.apache.hadoop.hive.metastore.metastore.impl.PrivilegeStoreImpl.getPrincipalTypeFromStr;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.newMetaException;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

@SuppressWarnings("unchecked")
public class TableStoreImpl extends RawStoreAware implements TableStore {
  private final static Logger LOG = LoggerFactory.getLogger(TableStoreImpl.class);
  private DatabaseProduct dbType;
  protected int batchSize = NO_BATCHING;
  private boolean areTxnStatsSupported = false;
  private PartitionExpressionProxy expressionProxy = null;
  private Configuration conf;

  @Override
  public void setBaseStore(RawStore store) {
    super.setBaseStore(store);
    this.dbType = PersistenceManagerProvider.getDatabaseProduct();
    this.batchSize = MetastoreConf.getIntVar(store.getConf(),
        MetastoreConf.ConfVars.RAWSTORE_PARTITION_BATCH_SIZE);
    this.areTxnStatsSupported = MetastoreConf.getBoolVar(baseStore.getConf(),
        MetastoreConf.ConfVars.HIVE_TXN_STATS_ENABLED);
    this.expressionProxy = PartFilterExprUtil.createExpressionProxy(store.getConf());
    this.conf = store.getConf();
  }

  @Override
  public boolean dropTable(TableName table)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    String catName = normalizeIdentifier(table.getCat());
    String dbName = normalizeIdentifier(table.getDb());
    String tableName = normalizeIdentifier(table.getTable());
    boolean materializedView = false;
    MTable tbl = getMTable(catName, dbName, tableName);
    pm.retrieve(tbl);
    if (tbl != null) {
      materializedView = TableType.MATERIALIZED_VIEW.toString().equals(tbl.getTableType());
      // first remove all the grants
      List<MTablePrivilege> tabGrants = listAllTableGrants(catName, dbName, tableName);
      if (CollectionUtils.isNotEmpty(tabGrants)) {
        pm.deletePersistentAll(tabGrants);
      }
      List<MTableColumnPrivilege> tblColGrants = listTableAllColumnGrants(catName, dbName,
          tableName, null);
      if (CollectionUtils.isNotEmpty(tblColGrants)) {
        pm.deletePersistentAll(tblColGrants);
      }

      List<MPartitionPrivilege> partGrants = listTableAllPartitionGrants(catName, dbName, tableName);
      if (CollectionUtils.isNotEmpty(partGrants)) {
        pm.deletePersistentAll(partGrants);
      }

      List<MPartitionColumnPrivilege> partColGrants = listTableAllPartitionColumnGrants(catName, dbName,
          tableName);
      if (CollectionUtils.isNotEmpty(partColGrants)) {
        pm.deletePersistentAll(partColGrants);
      }

      // delete column statistics if present
      baseStore.deleteTableColumnStatistics(catName, dbName, tableName, null, null);

      List<MConstraint> tabConstraints = listAllTableConstraintsWithOptionalConstraintName(
          catName, dbName, tableName, null);
      if (CollectionUtils.isNotEmpty(tabConstraints)) {
        pm.deletePersistentAll(tabConstraints);
      }

      preDropStorageDescriptor(tbl.getSd());

      if (materializedView) {
        dropCreationMetadata(tbl.getDatabase().getCatalogName(),
            tbl.getDatabase().getName(), tbl.getTableName());
      }

      // then remove the table
      pm.deletePersistentAll(tbl);
    }
    return true;
  }

  private List<MConstraint> listAllTableConstraintsWithOptionalConstraintName(
      String catName, String dbName, String tableName, String constraintname) {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tableName = normalizeIdentifier(tableName);
    constraintname = constraintname!=null?normalizeIdentifier(constraintname):null;
    List<MConstraint> mConstraints = null;
    List<String> constraintNames = new ArrayList<>();

    Query queryForConstraintName = pm.newQuery("select constraintName from org.apache.hadoop.hive.metastore.model.MConstraint  where "
        + "((parentTable.tableName == ptblname && parentTable.database.name == pdbname && " +
        "parentTable.database.catalogName == pcatname) || "
        + "(childTable != null && childTable.tableName == ctblname &&" +
        "childTable.database.name == cdbname && childTable.database.catalogName == ccatname)) " +
        (constraintname != null ? " && constraintName == constraintname" : ""));
    Query queryForMConstraint = pm.newQuery(MConstraint.class);
    queryForConstraintName.declareParameters("java.lang.String ptblname, java.lang.String pdbname,"
        + "java.lang.String pcatname, java.lang.String ctblname, java.lang.String cdbname," +
        "java.lang.String ccatname" +
        (constraintname != null ? ", java.lang.String constraintname" : ""));
    Collection<?> constraintNamesColl =
        constraintname != null ?
            ((Collection<?>) queryForConstraintName.
                executeWithArray(tableName, dbName, catName, tableName, dbName, catName, constraintname)):
            ((Collection<?>) queryForConstraintName.
                executeWithArray(tableName, dbName, catName, tableName, dbName, catName));
    for (Iterator<?> i = constraintNamesColl.iterator(); i.hasNext();) {
      String currName = (String) i.next();
      constraintNames.add(currName);
    }

    queryForMConstraint.setFilter("param.contains(constraintName)");
    queryForMConstraint.declareParameters("java.util.Collection param");
    Collection<?> constraints = (Collection<?>)queryForMConstraint.execute(constraintNames);
    mConstraints = new ArrayList<>();
    for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
      MConstraint currConstraint = (MConstraint) i.next();
      mConstraints.add(currConstraint);
    }
    return mConstraints;
  }

  private List<MPartitionColumnPrivilege> listTableAllPartitionColumnGrants(
      String catName, String dbName, String tableName) {
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);
    List<MPartitionColumnPrivilege> mSecurityColList = new ArrayList<>();
    LOG.debug("Executing listTableAllPartitionColumnGrants");

    String queryStr = "partition.table.tableName == t1 && partition.table.database.name == t2 " +
        "&& partition.table.database.catalogName == t3";
    Query query = pm.newQuery(MPartitionColumnPrivilege.class, queryStr);
    query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
    List<MPartitionColumnPrivilege> mPrivs =
        (List<MPartitionColumnPrivilege>) query.executeWithArray(tableName, dbName, catName);
    pm.retrieveAll(mPrivs);
    mSecurityColList.addAll(mPrivs);

    LOG.debug("Done retrieving all objects for listTableAllPartitionColumnGrants");
    return mSecurityColList;
  }


  private List<MPartitionPrivilege> listTableAllPartitionGrants(String catName, String dbName, String tableName) {
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);
    List<MPartitionPrivilege> mSecurityTabPartList = new ArrayList<>();
    LOG.debug("Executing listTableAllPartitionGrants");

    String queryStr = "partition.table.tableName == t1 && partition.table.database.name == t2 " +
        "&& partition.table.database.catalogName == t3";
    Query query = pm.newQuery(MPartitionPrivilege.class, queryStr);
    query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
    List<MPartitionPrivilege> mPrivs =
        (List<MPartitionPrivilege>) query.executeWithArray(tableName, dbName, catName);
    pm.retrieveAll(mPrivs);
    mSecurityTabPartList.addAll(mPrivs);

    LOG.debug("Done retrieving all objects for listTableAllPartitionGrants");
    return mSecurityTabPartList;
  }

  private void dropCreationMetadata(String catName, String dbName, String tableName) {
    MCreationMetadata mcm = getCreationMetadata(catName, dbName, tableName);
    pm.retrieve(mcm);
    if (mcm != null) {
      pm.deletePersistentAll(mcm);
    }
  }

  /**
   * Called right before an action that would drop a storage descriptor.
   * This function makes the SD's reference to a CD null, and then deletes the CD
   * if it no longer is referenced in the table.
   * @param msd the storage descriptor to drop
   */
  private void preDropStorageDescriptor(MStorageDescriptor msd) {
    if (msd == null || msd.getCD() == null) {
      return;
    }

    MColumnDescriptor mcd = msd.getCD();
    // Because there is a 1-N relationship between CDs and SDs,
    // we must set the SD's CD to null first before dropping the storage descriptor
    // to satisfy foreign key constraints.
    msd.setCD(null);
    removeUnusedColumnDescriptor(mcd);
  }


  @Override
  public List<String> dropAllPartitionsAndGetLocations(TableName table, String baseLocationToNotShow,
      AtomicReference<String> message)
      throws MetaException, InvalidInputException, NoSuchObjectException, InvalidObjectException {
    String catName = normalizeIdentifier(table.getCat());
    String dbName = normalizeIdentifier(table.getDb());
    String tblName = normalizeIdentifier(table.getTable());
    return new GetHelper<TableName, List<String>>(this, new TableName(catName, dbName, tblName)) {
      @Override
      protected String describeResult() {
        return "delete all partitions from " + table;
      }

      @Override
      protected List<String> getSqlResult() throws MetaException {
        return getDirectSql()
            .dropAllPartitionsAndGetLocations(getTable().getId(), baseLocationToNotShow, message);
      }

      @Override
      protected List<String> getJdoResult()
          throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        Map<String, String> partitionLocations =
            getPartitionLocations(table, baseLocationToNotShow, -1);
        dropPartitionsViaJdo(catName, dbName, tblName, new ArrayList<>(partitionLocations.keySet()), message);
        return partitionLocations.values().stream().filter(Objects::nonNull).toList();
      }
    }.run(true);
  }

  @Override
  public Map<String, String> getPartitionLocations(TableName tableName,
      String baseLocationToNotShow, int max) {
    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());

    Map<String, String> partLocations = new HashMap<>();
    LOG.debug("Executing getPartitionLocations");

    Query query = pm.newQuery(MPartition.class);
    query.setFilter(
        "this.table.database.catalogName == t1 && this.table.database.name == t2 "
            + "&& this.table.tableName == t3");
    query.declareParameters("String t1, String t2, String t3");
    query.setResult("this.partitionName, this.sd.location");
    if (max >= 0) {
      //Row limit specified, set it on the Query
      query.setRange(0, max);
    }

    List<Object[]> result = (List<Object[]>) query.execute(catName, dbName, tblName);
    for (Object[] row : result) {
      String location = (String) row[1];
      if (baseLocationToNotShow != null && location != null &&
          FileUtils.isSubdirectory(baseLocationToNotShow, location)) {
        location = null;
      }
      partLocations.put((String) row[0], location);
    }
    LOG.debug("Done executing query for getPartitionLocations");
    return partLocations;
  }

  private void dropPartitionsViaJdo(String catName, String dbName, String tblName, List<String> partNames,
      AtomicReference<String> message) throws MetaException {
    if (partNames.isEmpty()) {
      return;
    }
    int batch = batchSize == NO_BATCHING ? 1 : (partNames.size() + batchSize) / batchSize;
    AtomicLong batchIdx = new AtomicLong(1);
    AtomicLong timeSpent = new AtomicLong(0);
    Batchable.runBatched(batchSize, partNames, new Batchable<String, Void>() {
      @Override
      public List<Void> run(List<String> input) throws MetaException {
        StringBuilder progress = new StringBuilder("Dropping partitions, batch: ");
        long start = System.currentTimeMillis();
        progress.append(batchIdx.get()).append("/").append(batch);
        if (batchIdx.get() > 1) {
          long leftTime = (batch - batchIdx.get()) * timeSpent.get() / batchIdx.get();
          progress.append(", time left: ").append(leftTime).append("ms");
        }
        message.set(progress.toString());
        // Delete all things.
        dropPartitionGrantsNoTxn(catName, dbName, tblName, input);
        dropPartitionAllColumnGrantsNoTxn(catName, dbName, tblName, input);
        dropPartitionColumnStatisticsNoTxn(catName, dbName, tblName, input);

        // CDs are reused; go try partition SDs, detach all CDs from SDs, then remove unused CDs.
        for (MColumnDescriptor mcd : detachCdsFromSdsNoTxn(catName, dbName, tblName, input)) {
          removeUnusedColumnDescriptor(mcd);
        }
        dropPartitionsNoTxn(catName, dbName, tblName, input);
        timeSpent.addAndGet(System.currentTimeMillis() - start);
        batchIdx.incrementAndGet();
        return Collections.emptyList();
      }
    });
  }

  /**
   * Checks if a column descriptor has any remaining references by storage descriptors
   * in the db.  If it does not, then delete the CD.  If it does, then do nothing.
   *
   * @param oldCD the column descriptor to delete if it is no longer referenced anywhere
   */
  private void removeUnusedColumnDescriptor(MColumnDescriptor oldCD) {
    if (oldCD == null) {
      return;
    }
    LOG.debug("execute removeUnusedColumnDescriptor");
    if (!hasRemainingCDReference(oldCD)) {
      // First remove any constraints that may be associated with this CD
      Query query = pm.newQuery(MConstraint.class, "parentColumn == inCD || childColumn == inCD");
      query.declareParameters("MColumnDescriptor inCD");
      List<MConstraint> mConstraintsList = (List<MConstraint>) query.execute(oldCD);
      if (CollectionUtils.isNotEmpty(mConstraintsList)) {
        pm.deletePersistentAll(mConstraintsList);
      }
      // Finally remove CD
      pm.retrieve(oldCD);
      pm.deletePersistent(oldCD);
      LOG.debug("successfully deleted a CD in removeUnusedColumnDescriptor");
    }
  }

  /**
   * Checks if a column descriptor has any remaining references by storage descriptors
   * in the db.
   *
   * @param oldCD the column descriptor to check if it has references or not
   * @return true if has references
   */
  private boolean hasRemainingCDReference(MColumnDescriptor oldCD) {
    assert oldCD != null;
    Query query;
    /**
     * In order to workaround oracle not supporting limit statement caused performance issue, HIVE-9447 makes
     * all the backend DB run select count(1) from SDS where SDS.CD_ID=? to check if the specific CD_ID is
     * referenced in SDS table before drop a partition. This select count(1) statement does not scale well in
     * Postgres, and there is no index for CD_ID column in SDS table.
     * For a SDS table with with 1.5 million rows, select count(1) has average 700ms without index, while in
     * 10-20ms with index. But the statement before
     * HIVE-9447( SELECT * FROM "SDS" "A0" WHERE "A0"."CD_ID" = $1 limit 1) uses less than 10ms .
     */
    // HIVE-21075: Fix Postgres performance regression caused by HIVE-9447
    LOG.debug("The dbType is {} ", dbType.getHiveSchemaPostfix());
    if (dbType.isPOSTGRES() || dbType.isMYSQL()) {
      query = pm.newQuery(MStorageDescriptor.class, "this.cd == inCD");
      query.declareParameters("MColumnDescriptor inCD");
      List<MStorageDescriptor> referencedSDs = null;
      LOG.debug("Executing listStorageDescriptorsWithCD");
      // User specified a row limit, set it on the Query
      query.setRange(0L, 1L);
      referencedSDs = (List<MStorageDescriptor>) query.execute(oldCD);
      LOG.debug("Done executing query for listStorageDescriptorsWithCD");
      pm.retrieveAll(referencedSDs);
      LOG.debug("Done retrieving all objects for listStorageDescriptorsWithCD");
      //if no other SD references this CD, we can throw it out.
      return referencedSDs != null && !referencedSDs.isEmpty();
    } else {
      query = pm.newQuery(
          "select count(1) from org.apache.hadoop.hive.metastore.model.MStorageDescriptor where (this.cd == inCD)");
      query.declareParameters("MColumnDescriptor inCD");
      long count = (Long) query.execute(oldCD);
      //if no other SD references this CD, we can throw it out.
      return count != 0;
    }
  }

  /**
   * Detaches column descriptors from storage descriptors; returns the set of unique CDs
   * thus detached. This is done before dropping partitions because CDs are reused between
   * SDs; so, we remove the links to delete SDs and then check the returned CDs to see if
   * they are referenced by other SDs.
   */
  private Set<MColumnDescriptor> detachCdsFromSdsNoTxn(String catName, String dbName, String tblName,
      List<String> partNames) {
    Pair<Query, Map<String, String>> queryWithParams = getPartQueryWithParams(pm, catName, dbName, tblName, partNames);
    Query query = queryWithParams.getLeft();
    query.setClass(MPartition.class);
    query.setResult("sd");
    List<MStorageDescriptor> sds =
        (List<MStorageDescriptor>) query.executeWithMap(queryWithParams.getRight());
    HashSet<MColumnDescriptor> candidateCds = new HashSet<>();
    for (MStorageDescriptor sd : sds) {
      if (sd != null && sd.getCD() != null) {
        candidateCds.add(sd.getCD());
        sd.setCD(null);
      }
    }
    return candidateCds;
  }

  private void dropPartitionsNoTxn(String catName, String dbName, String tblName, List<String> partNames) {
    Pair<Query, Map<String, String>> queryWithParams = getPartQueryWithParams(pm, catName, dbName, tblName, partNames);
    Query query = queryWithParams.getLeft();
    query.setClass(MPartition.class);
    long deleted = query.deletePersistentAll(queryWithParams.getRight());
    LOG.debug("Deleted {} partition from store", deleted);
  }

  private void dropPartitionGrantsNoTxn(String catName, String dbName, String tableName, List<String> partNames) {
    Pair<Query, Object[]> queryWithParams = makeQueryByPartitionNames(catName, dbName, tableName, partNames,
        MPartitionPrivilege.class, "partition.table.tableName", "partition.table.database.name",
        "partition.partitionName", "partition.table.database.catalogName");
    Query query = queryWithParams.getLeft();
    query.deletePersistentAll(queryWithParams.getRight());
  }

  private Pair<Query, Object[]> makeQueryByPartitionNames(String catName, String dbName, String tableName,
      List<String> partNames, Class<?> clazz, String tbCol, String dbCol, String partCol, String catCol) {
    StringBuilder queryStr = new StringBuilder(tbCol + " == t1 && " + dbCol + " == t2 && " + catCol + " == t3");
    StringBuilder paramStr = new StringBuilder("java.lang.String t1, java.lang.String t2, java.lang.String t3");
    Object[] params = new Object[3 + partNames.size()];
    params[0] = normalizeIdentifier(tableName);
    params[1] = normalizeIdentifier(dbName);
    params[2] = normalizeIdentifier(catName);
    int index = 0;
    for (String partName : partNames) {
      params[index + 3] = partName;
      queryStr.append(((index == 0) ? " && (" : " || ") + partCol + " == p" + index);
      paramStr.append(", java.lang.String p" + index);
      ++index;
    }
    queryStr.append(")");
    Query query = pm.newQuery(clazz, queryStr.toString());
    query.declareParameters(paramStr.toString());
    return Pair.of(query, params);
  }

  private void dropPartitionAllColumnGrantsNoTxn(String catName, String dbName, String tableName,
      List<String> partNames) {
    Pair<Query, Object[]> queryWithParams = makeQueryByPartitionNames(catName, dbName, tableName, partNames,
        MPartitionColumnPrivilege.class, "partition.table.tableName", "partition.table.database.name",
        "partition.partitionName", "partition.table.database.catalogName");
    Query query = queryWithParams.getLeft();
    query.deletePersistentAll(queryWithParams.getRight());
  }

  private void dropPartitionColumnStatisticsNoTxn(String catName, String dbName, String tableName,
      List<String> partNames) {
    Pair<Query, Object[]> queryWithParams = makeQueryByPartitionNames(catName, dbName, tableName, partNames,
        MPartitionColumnStatistics.class, "partition.table.tableName", "partition.table.database.name",
        "partition.partitionName", "partition.table.database.catalogName");
    Query query = queryWithParams.getLeft();
    query.deletePersistentAll(queryWithParams.getRight());
  }

  class AttachedMTableInfo {
    MTable mtbl;
    MColumnDescriptor mcd;

    public AttachedMTableInfo() {}

    public AttachedMTableInfo(MTable mtbl, MColumnDescriptor mcd) {
      this.mtbl = mtbl;
      this.mcd = mcd;
    }
  }

  private MTable getMTable(String catName, String db, String table) {
    AttachedMTableInfo nmtbl = getMTable(catName, db, table, false);
    return nmtbl.mtbl;
  }

  private AttachedMTableInfo getMTable(String catName, String db, String table,
      boolean retrieveCD) {
    AttachedMTableInfo nmtbl = new AttachedMTableInfo();
    catName = normalizeIdentifier(Optional.ofNullable(catName).orElse(getDefaultCatalog(baseStore.getConf())));
    db = normalizeIdentifier(db);
    table = normalizeIdentifier(table);
    Query query = pm.newQuery(MTable.class,
        "tableName == table && database.name == db && database.catalogName == catname");
    query.declareParameters(
        "java.lang.String table, java.lang.String db, java.lang.String catname");
    query.setUnique(true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Executing getMTable for {}",
          TableName.getQualified(catName, db, table));
    }
    MTable mtbl = (MTable) query.execute(table, db, catName);
    pm.retrieve(mtbl);
    // Retrieving CD can be expensive and unnecessary, so do it only when required.
    if (mtbl != null && retrieveCD) {
      pm.retrieve(mtbl.getSd());
      pm.retrieveAll(mtbl.getSd().getCD());
      nmtbl.mcd = mtbl.getSd().getCD();
    }
    nmtbl.mtbl = mtbl;
    return nmtbl;
  }

  @Override
  public Table getTable(TableName table, String writeIdList, long tableId)
      throws MetaException {
    Table tbl;
    String catName = normalizeIdentifier(table.getCat());
    String dbName = normalizeIdentifier(table.getDb());
    String tableName = normalizeIdentifier(table.getTable());
    MTable mtable = getMTable(catName, dbName, tableName);
    tbl = convertToTable(mtable, conf);
    // Retrieve creation metadata if needed
    if (tbl != null && TableType.MATERIALIZED_VIEW.toString().equals(tbl.getTableType())) {
      tbl.setCreationMetadata(
          convertToCreationMetadata(getCreationMetadata(catName, dbName, tableName), baseStore));
    }

    // If transactional non partitioned table,
    // check whether the current version table statistics
    // in the metastore comply with the client query's snapshot isolation.
    // Note: a partitioned table has table stats and table snapshot in MPartiiton.
    if (writeIdList != null) {
      boolean isTxn = TxnUtils.isTransactionalTable(tbl);
      if (isTxn && !areTxnStatsSupported) {
        StatsSetupConst.setBasicStatsState(tbl.getParameters(), StatsSetupConst.FALSE);
        LOG.info("Removed COLUMN_STATS_ACCURATE from Table's parameters.");
      } else if (isTxn && tbl.getPartitionKeysSize() == 0) {
        if (isCurrentStatsValidForTheQuery(mtable.getParameters(),
            mtable.getWriteId(), writeIdList, false)) {
          tbl.setIsStatsCompliant(true);
        } else {
          tbl.setIsStatsCompliant(false);
          // Do not make persistent the following state since it is the query specific (not global).
          StatsSetupConst.setBasicStatsState(tbl.getParameters(), StatsSetupConst.FALSE);
          LOG.info("Removed COLUMN_STATS_ACCURATE from Table's parameters.");
        }
      }
    }
    return tbl;
  }

  private MCreationMetadata getCreationMetadata(String catName, String dbName, String tblName) {
    Query query = pm.newQuery(
        MCreationMetadata.class, "tblName == table && dbName == db && catalogName == cat");
    query.declareParameters("java.lang.String table, java.lang.String db, java.lang.String cat");
    query.setUnique(true);
    MCreationMetadata mcm = (MCreationMetadata) query.execute(tblName, dbName, catName);
    pm.retrieve(mcm);
    return mcm;
  }

  // TODO: move to somewhere else
  public static boolean isCurrentStatsValidForTheQuery(
      Map<String, String> statsParams, long statsWriteId, String queryValidWriteIdList,
      boolean isCompleteStatsWriter) throws MetaException {

    // Note: can be changed to debug/info to verify the calls.
    LOG.debug("isCurrentStatsValidForTheQuery with stats write ID {}; query {}; writer: {} params {}",
        statsWriteId, queryValidWriteIdList, isCompleteStatsWriter, statsParams);
    // return true since the stats does not seem to be transactional.
    if (statsWriteId < 1) {
      return true;
    }
    // This COLUMN_STATS_ACCURATE(CSA) state checking also includes the case that the stats is
    // written by an aborted transaction but TXNS has no entry for the transaction
    // after compaction. Don't check for a complete stats writer - it may replace invalid stats.
    if (!isCompleteStatsWriter && !StatsSetupConst.areBasicStatsUptoDate(statsParams)) {
      return false;
    }

    if (queryValidWriteIdList != null) { // Can be null when stats are being reset to invalid.
      ValidWriteIdList list4TheQuery = ValidReaderWriteIdList.fromValue(queryValidWriteIdList);
      // Just check if the write ID is valid. If it's valid (i.e. we are allowed to see it),
      // that means it cannot possibly be a concurrent write. If it's not valid (we are not
      // allowed to see it), that means it's either concurrent or aborted, same thing for us.
      if (list4TheQuery.isWriteIdValid(statsWriteId)) {
        return true;
      }
      // Updater is also allowed to overwrite stats from aborted txns, as long as they are not concurrent.
      if (isCompleteStatsWriter && list4TheQuery.isWriteIdAborted(statsWriteId)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public boolean addPartitions(TableName tableName, List<Partition> parts) throws InvalidObjectException, MetaException {
    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());
    List<MTablePrivilege> tabGrants = null;
    List<MTableColumnPrivilege> tabColumnGrants = null;
    MTable table = this.getMTable(catName, dbName, tblName);
    if (table == null) {
      throw new InvalidObjectException("Unable to add partitions because "
          + tableName + " does not exist");
    }
    if ("TRUE".equalsIgnoreCase(table.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
      tabGrants = listAllTableGrants(catName, dbName, tblName);
      tabColumnGrants = listTableAllColumnGrants(catName, dbName, tblName, null);
    }
    List<MPartition> mParts = new ArrayList<>();
    List<List<MPartitionPrivilege>> mPartPrivilegesList = new ArrayList<>();
    List<List<MPartitionColumnPrivilege>> mPartColPrivilegesList = new ArrayList<>();
    for (Partition part : parts) {
      if (!part.getTableName().equalsIgnoreCase(tblName) || !part.getDbName().equalsIgnoreCase(dbName)) {
        throw new MetaException("Partition does not belong to target table "
            + dbName + "." + tblName + ": " + part);
      }
      MPartition mpart = convertToMPart(part, table);
      mParts.add(mpart);
      int now = (int) (System.currentTimeMillis() / 1000);
      List<MPartitionPrivilege> mPartPrivileges = new ArrayList<>();
      if (tabGrants != null) {
        for (MTablePrivilege tab: tabGrants) {
          MPartitionPrivilege mPartPrivilege = new MPartitionPrivilege(tab.getPrincipalName(), tab.getPrincipalType(),
              mpart, tab.getPrivilege(), now, tab.getGrantor(), tab.getGrantorType(), tab.getGrantOption(),
              tab.getAuthorizer());
          mPartPrivileges.add(mPartPrivilege);
        }
      }

      List<MPartitionColumnPrivilege> mPartColumnPrivileges = new ArrayList<>();
      if (tabColumnGrants != null) {
        for (MTableColumnPrivilege col : tabColumnGrants) {
          MPartitionColumnPrivilege mPartColumnPrivilege = new MPartitionColumnPrivilege(col.getPrincipalName(),
              col.getPrincipalType(), mpart, col.getColumnName(), col.getPrivilege(), now, col.getGrantor(),
              col.getGrantorType(), col.getGrantOption(), col.getAuthorizer());
          mPartColumnPrivileges.add(mPartColumnPrivilege);
        }
      }
      mPartPrivilegesList.add(mPartPrivileges);
      mPartColPrivilegesList.add(mPartColumnPrivileges);
    }
    if (CollectionUtils.isNotEmpty(mParts)) {
      GetHelper<TableName, Void> helper = new GetHelper<>(this, tableName) {
        @Override
        protected Void getSqlResult() throws MetaException {
          getDirectSql().addPartitions(mParts, mPartPrivilegesList, mPartColPrivilegesList);
          return null;
        }

        @Override
        protected Void getJdoResult() {
          List<Object> toPersist = new ArrayList<>(mParts);
          mPartPrivilegesList.forEach(toPersist::addAll);
          mPartColPrivilegesList.forEach(toPersist::addAll);
          pm.makePersistentAll(toPersist);
          pm.flush();
          return null;
        }

        @Override
        protected String describeResult() {
          return "add partitions";
        }
      };
      try {
        helper.run(false);
      } catch (NoSuchObjectException e) {
        throw newMetaException(e);
      }
    }
    return true;
  }

  private List<MTablePrivilege> listAllTableGrants(String catName, String dbName, String tableName) {
    List<MTablePrivilege> mSecurityTabList = new ArrayList<>();
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);
    LOG.debug("Executing listAllTableGrants");

    String queryStr = "table.tableName == t1 && table.database.name == t2" +
        "&& table.database.catalogName == t3";
    Query query = pm.newQuery(MTablePrivilege.class, queryStr);
    query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
    List<MTablePrivilege> mPrivs  =
        (List<MTablePrivilege>) query.executeWithArray(tableName, dbName, catName);
    LOG.debug("Done executing query for listAllTableGrants");
    pm.retrieveAll(mPrivs);
    mSecurityTabList.addAll(mPrivs);
    LOG.debug("Done retrieving all objects for listAllTableGrants");
    return mSecurityTabList;
  }

  private List<MTableColumnPrivilege> listTableAllColumnGrants(
      String catName, String dbName, String tableName, String authorizer) {
    Query query;
    List<MTableColumnPrivilege> mTblColPrivilegeList = new ArrayList<>();
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);
    LOG.debug("Executing listTableAllColumnGrants");
    List<MTableColumnPrivilege> mPrivs = null;
    if (authorizer != null) {
      String queryStr = "table.tableName == t1 && table.database.name == t2 &&" +
          "table.database.catalogName == t3 && authorizer == t4";
      query = pm.newQuery(MTableColumnPrivilege.class, queryStr);
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, " +
          "java.lang.String t4");
      mPrivs = (List<MTableColumnPrivilege>) query.executeWithArray(tableName, dbName, catName, authorizer);
    } else {
      String queryStr = "table.tableName == t1 && table.database.name == t2 &&" +
          "table.database.catalogName == t3";
      query = pm.newQuery(MTableColumnPrivilege.class, queryStr);
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      mPrivs = (List<MTableColumnPrivilege>) query.executeWithArray(tableName, dbName, catName);
    }
    LOG.debug("Query to obtain objects for listTableAllColumnGrants finished");
    pm.retrieveAll(mPrivs);
    LOG.debug("RetrieveAll on all the objects for listTableAllColumnGrants finished");
    mTblColPrivilegeList.addAll(mPrivs);
    LOG.debug("Done retrieving " + mPrivs.size() + " objects for listTableAllColumnGrants");
    return mTblColPrivilegeList;
  }

  @Override
  public Partition getPartition(TableName tabName, List<String> part_vals, String validWriteIds)
      throws MetaException, NoSuchObjectException {
    String catName = normalizeIdentifier(tabName.getCat());
    String dbName = normalizeIdentifier(tabName.getDb());
    String tableName = normalizeIdentifier(tabName.getTable());
    Partition part = null;
    MTable table = this.getMTable(catName, dbName, tableName);
    if (table == null) {
      throw new NoSuchObjectException("Unable to get partition because "
          + TableName.getQualified(catName, dbName, tableName) +
          " does not exist");
    }
    MPartition mpart = getMPartition(catName, dbName, tableName, part_vals, table);
    part = convertToPart(catName, dbName, tableName, mpart,
        TxnUtils.isAcidTable(table.getParameters()), conf);
    if (part == null) {
      throw new NoSuchObjectException("partition values="
          + part_vals.toString());
    }

    part.setValues(part_vals);
    // If transactional table partition, check whether the current version partition
    // statistics in the metastore comply with the client query's snapshot isolation.
    long statsWriteId = mpart.getWriteId();
    if (TxnUtils.isTransactionalTable(table.getParameters())) {
      if (!areTxnStatsSupported) {
        // Do not make persistent the following state since it is query specific (not global).
        StatsSetupConst.setBasicStatsState(part.getParameters(), StatsSetupConst.FALSE);
        LOG.info("Removed COLUMN_STATS_ACCURATE from Partition object's parameters.");
      } else if (validWriteIds != null) {
        if (isCurrentStatsValidForTheQuery(part.getParameters(), statsWriteId, validWriteIds, false)) {
          part.setIsStatsCompliant(true);
        } else {
          part.setIsStatsCompliant(false);
          // Do not make persistent the following state since it is query specific (not global).
          StatsSetupConst.setBasicStatsState(part.getParameters(), StatsSetupConst.FALSE);
          LOG.info("Removed COLUMN_STATS_ACCURATE from Partition object's parameters.");
        }
      }
    }
    return part;
  }

  /**
   * Getting MPartition object. Use this method only if the partition name is not available,
   * since then the table will be queried to get the partition keys.
   * @param catName The catalogue
   * @param dbName The database
   * @param tableName The table
   * @param part_vals The values defining the partition
   * @return The MPartition object in the backend database
   */
  private MPartition getMPartition(String catName, String dbName, String tableName, List<String> part_vals, MTable mtbl)
      throws MetaException {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tableName = normalizeIdentifier(tableName);
    if (mtbl == null) {
      mtbl = getMTable(catName, dbName, tableName);
      if (mtbl == null) {
        return null;
      }
    }
    // Change the query to use part_vals instead of the name which is
    // redundant TODO: callers of this often get part_vals out of name for no reason...
    String name =
        Warehouse.makePartName(convertToFieldSchemas(mtbl.getPartitionKeys()), part_vals);
    MPartition result = getMPartition(catName, dbName, tableName, name);
    return result;
  }

  @Override
  public List<Partition> getPartitions(TableName table, GetPartitionsArgs args)
      throws MetaException, NoSuchObjectException {
    String catName = normalizeIdentifier(table.getCat());
    String dbName = normalizeIdentifier(table.getDb());
    String tblName = normalizeIdentifier(table.getTable());
    return new GetListHelper<TableName, Partition>(this, table) {
      @Override
      protected List<Partition> getSqlResult() throws MetaException {
        return getDirectSql().getPartitions(catName, dbName, tblName, args);
      }
      @Override
      protected List<Partition> getJdoResult() throws MetaException {
        try {
          return convertToParts(catName, dbName, tblName,
              listMPartitions(catName, dbName, tblName, args.getMax()), false, conf, args);
        } catch (Exception e) {
          LOG.error("Failed to convert to parts", e);
          throw new MetaException(e.getMessage());
        }
      }
    }.run(false);
  }

  private List<MPartition> listMPartitions(String catName, String dbName, String tableName, int max) {
    LOG.debug("Executing listMPartitions");
    dbName = normalizeIdentifier(dbName);
    tableName = normalizeIdentifier(tableName);

    Query query = pm.newQuery(MPartition.class,
        "table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3");
    query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
    query.setOrdering("partitionName ascending");
    if (max >= 0) {
      query.setRange(0, max);
    }
    final List<MPartition> mparts = (List<MPartition>) query.execute(tableName, dbName, catName);
    LOG.debug("Done executing query for listMPartitions");

    pm.retrieveAll(mparts);
    pm.makeTransientAll(mparts);

    LOG.debug("Done retrieving all objects for listMPartitions {}", mparts);

    return Collections.unmodifiableList(new ArrayList<>(mparts));
  }

  @Override
  public Table alterTable(TableName tableName, Table newTable, String queryValidWriteIds)
      throws InvalidObjectException, MetaException {
    String name = normalizeIdentifier(tableName.getTable());
    String dbname = normalizeIdentifier(tableName.getDb());
    String catName = normalizeIdentifier(tableName.getCat());
    MTable newt = convertToMTable(newTable, baseStore);
    if (newt == null) {
      throw new InvalidObjectException("new table is invalid");
    }

    MTable oldt = getMTable(catName, dbname, name);
    if (oldt == null) {
      throw new MetaException("table " + dbname + "." + name + " doesn't exist");
    }

    // For now only alter name, owner, parameters, cols, bucketcols are allowed
    oldt.setDatabase(newt.getDatabase());
    oldt.setTableName(normalizeIdentifier(newt.getTableName()));
    boolean isTxn = TxnUtils.isTransactionalTable(newTable);
    boolean isToTxn = isTxn && !TxnUtils.isTransactionalTable(oldt.getParameters());
    if (!isToTxn && isTxn && areTxnStatsSupported) {
      // Transactional table is altered without a txn. Make sure there are no changes to the flag.
      String errorMsg = verifyStatsChangeCtx(TableName.getDbTable(name, dbname), oldt.getParameters(),
          newTable.getParameters(), newTable.getWriteId(), queryValidWriteIds, false);
      if (errorMsg != null) {
        throw new MetaException(errorMsg);
      }
    }
    oldt.setParameters(newt.getParameters());
    oldt.setOwner(newt.getOwner());
    oldt.setOwnerType(newt.getOwnerType());
    // Fully copy over the contents of the new SD into the old SD,
    // so we don't create an extra SD in the metastore db that has no references.
    MColumnDescriptor oldCD = null;
    MStorageDescriptor oldSD = oldt.getSd();
    if (oldSD != null) {
      oldCD = oldSD.getCD();
    }
    copyMSD(newt.getSd(), oldt.getSd());
    removeUnusedColumnDescriptor(oldCD);
    oldt.setRetention(newt.getRetention());
    oldt.setPartitionKeys(newt.getPartitionKeys());
    oldt.setTableType(newt.getTableType());
    oldt.setLastAccessTime(newt.getLastAccessTime());
    oldt.setViewOriginalText(newt.getViewOriginalText());
    oldt.setViewExpandedText(newt.getViewExpandedText());
    oldt.setRewriteEnabled(newt.isRewriteEnabled());

    // If transactional, update the stats state for the current Stats updater query.
    // Set stats invalid for ACID conversion; it doesn't pass in the write ID.
    if (isTxn) {
      if (!areTxnStatsSupported || isToTxn) {
        StatsSetupConst.setBasicStatsState(oldt.getParameters(), StatsSetupConst.FALSE);
      } else if (queryValidWriteIds != null && newTable.getWriteId() > 0) {
        // Check concurrent INSERT case and set false to the flag.
        if (!isCurrentStatsValidForTheQuery(oldt.getParameters(), oldt.getWriteId(), queryValidWriteIds, true)) {
          StatsSetupConst.setBasicStatsState(oldt.getParameters(), StatsSetupConst.FALSE);
          LOG.info("Removed COLUMN_STATS_ACCURATE from the parameters of the table " +
              dbname + "." + name + ". will be made persistent.");
        }
        assert newTable.getWriteId() > 0;
        oldt.setWriteId(newTable.getWriteId());
      }
    }
    newTable = convertToTable(oldt, conf);
    return newTable;
  }


  private void copyMSD(MStorageDescriptor newSd, MStorageDescriptor oldSd) {
    oldSd.setLocation(newSd.getLocation());
    // If the columns of the old column descriptor != the columns of the new one,
    // then change the old storage descriptor's column descriptor.
    // Convert the MFieldSchema's to their thrift object counterparts, because we maintain
    // datastore identity (i.e., identity of the model objects are managed by JDO,
    // not the application).
    List<FieldSchema> oldCols = oldSd.getCD() != null && oldSd.getCD().getCols() != null ?
        convertToFieldSchemas(oldSd.getCD().getCols()) : null;
    List<FieldSchema> newCols = newSd.getCD() != null && newSd.getCD().getCols() != null ?
        convertToFieldSchemas(newSd.getCD().getCols()) : null;
    if (oldCols == null || !oldCols.equals(newCols)) {
      // First replace any constraints that may be associated with this CD
      // Create mapping from old col indexes to new col indexes
      if (oldCols != null && newCols != null) {
        Map<Integer, Integer> mapping = new HashMap<>();
        for (int i = 0; i < oldCols.size(); i++) {
          FieldSchema oldCol = oldCols.get(i);
          //TODO: replace for loop with list.indexOf()
          for (int j = 0; j < newCols.size(); j++) {
            FieldSchema newCol = newCols.get(j);
            if (oldCol.equals(newCol)) {
              mapping.put(i, j);
              break;
            }
          }
        }
        // If we find it, we will change the reference for the CD.
        // If we do not find it, i.e., the column will be deleted, we do not change it
        // and we let the logic in removeUnusedColumnDescriptor take care of it
        try (QueryWrapper query = new QueryWrapper(pm.newQuery(MConstraint.class, "parentColumn == inCD || childColumn == inCD"))) {
          query.declareParameters("MColumnDescriptor inCD");
          List<MConstraint> mConstraintsList = (List<MConstraint>) query.execute(oldSd.getCD());
          pm.retrieveAll(mConstraintsList);
          for (MConstraint mConstraint : mConstraintsList) {
            if (oldSd.getCD().equals(mConstraint.getParentColumn())) {
              Integer newIdx = mapping.get(mConstraint.getParentIntegerIndex());
              if (newIdx != null) {
                mConstraint.setParentColumn(newSd.getCD());
                mConstraint.setParentIntegerIndex(newIdx);
              }
            }
            if (oldSd.getCD().equals(mConstraint.getChildColumn())) {
              Integer newIdx = mapping.get(mConstraint.getChildIntegerIndex());
              if (newIdx != null) {
                mConstraint.setChildColumn(newSd.getCD());
                mConstraint.setChildIntegerIndex(newIdx);
              }
            }
          }
          pm.makePersistentAll(mConstraintsList);
        }
        // Finally replace CD
        oldSd.setCD(newSd.getCD());
      }
    }

    oldSd.setBucketCols(newSd.getBucketCols());
    oldSd.setIsCompressed(newSd.isCompressed());
    oldSd.setInputFormat(newSd.getInputFormat());
    oldSd.setOutputFormat(newSd.getOutputFormat());
    oldSd.setNumBuckets(newSd.getNumBuckets());
    oldSd.getSerDeInfo().setName(newSd.getSerDeInfo().getName());
    oldSd.getSerDeInfo().setSerializationLib(
        newSd.getSerDeInfo().getSerializationLib());
    oldSd.getSerDeInfo().setParameters(newSd.getSerDeInfo().getParameters());
    oldSd.getSerDeInfo().setDescription(newSd.getSerDeInfo().getDescription());
    oldSd.setSkewedColNames(newSd.getSkewedColNames());
    oldSd.setSkewedColValues(newSd.getSkewedColValues());
    oldSd.setSkewedColValueLocationMaps(newSd.getSkewedColValueLocationMaps());
    oldSd.setSortCols(newSd.getSortCols());
    oldSd.setParameters(newSd.getParameters());
    oldSd.setStoredAsSubDirectories(newSd.isStoredAsSubDirectories());
  }

  @Override
  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    MTable mtbl = convertToMTable(tbl, baseStore);;

    if (TxnUtils.isTransactionalTable(tbl)) {
      mtbl.setWriteId(tbl.getWriteId());
    }
    pm.makePersistent(mtbl);

    if (tbl.getCreationMetadata() != null) {
      MCreationMetadata mcm = convertToMCreationMetadata(tbl.getCreationMetadata(), baseStore);
      pm.makePersistent(mcm);
    }
    tbl.setId(mtbl.getId());

    PrincipalPrivilegeSet principalPrivs = tbl.getPrivileges();
    List<Object> toPersistPrivObjs = new ArrayList<>();
    if (principalPrivs != null) {
      int now = (int) (System.currentTimeMillis() / 1000);

      Map<String, List<PrivilegeGrantInfo>> userPrivs = principalPrivs.getUserPrivileges();
      putPersistentPrivObjects(mtbl, toPersistPrivObjs, now, userPrivs, PrincipalType.USER, "SQL");

      Map<String, List<PrivilegeGrantInfo>> groupPrivs = principalPrivs.getGroupPrivileges();
      putPersistentPrivObjects(mtbl, toPersistPrivObjs, now, groupPrivs, PrincipalType.GROUP, "SQL");

      Map<String, List<PrivilegeGrantInfo>> rolePrivs = principalPrivs.getRolePrivileges();
      putPersistentPrivObjects(mtbl, toPersistPrivObjs, now, rolePrivs, PrincipalType.ROLE, "SQL");
    }
    pm.makePersistentAll(toPersistPrivObjs);
  }

  @Override
  public boolean dropPartitions(TableName tableName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    if (CollectionUtils.isEmpty(partNames)) {
      return false;
    }
    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());
    new GetListHelper<TableName, Void>(this, tableName) {
      @Override
      protected List<Void> getSqlResult() throws MetaException {
        getDirectSql().dropPartitionsViaSqlFilter(catName, dbName, tblName, partNames);
        return Collections.emptyList();
      }
      @Override
      protected List<Void> getJdoResult() throws MetaException {
        dropPartitionsViaJdo(catName, dbName, tblName, partNames, new AtomicReference<>());
        return Collections.emptyList();
      }
    }.run(false);
    return true;
  }

  @Override
  public List<String> getTables(String catName, String dbName, String pattern, TableType tableType, int limit)
      throws MetaException {
    try {
      final String db_name = normalizeIdentifier(dbName);
      final String cat_name = normalizeIdentifier(catName);
      return new GetListHelper<TableName, String>(this, null) {
        @Override
        protected boolean canUseDirectSql() throws MetaException {
          return (pattern == null || pattern.equals(".*"));
        }

        @Override
        protected List<String> getSqlResult() throws MetaException {
          return getDirectSql().getTables(cat_name, db_name, tableType, limit);
        }

        @Override
        protected List<String> getJdoResult() throws MetaException, NoSuchObjectException {
          return getTablesInternalViaJdo(cat_name, db_name, pattern, tableType, limit);
        }
      }.run(false);
    } catch (NoSuchObjectException nse) {
      throw new MetaException(nse.getMessage());
    }
  }

  private List<String> getTablesInternalViaJdo(String catName, String dbName, String pattern,
      TableType tableType, int limit) {
    dbName = normalizeIdentifier(dbName);
    // Take the pattern and split it on the | to get all the composing
    // patterns
    List<String> parameterVals = new ArrayList<>();
    StringBuilder filterBuilder = new StringBuilder();
    //adds database.name == dbName to the filter
    appendSimpleCondition(filterBuilder, "database.name", new String[] {dbName}, parameterVals);
    appendSimpleCondition(filterBuilder, "database.catalogName", new String[] {catName}, parameterVals);
    if(pattern != null) {
      appendPatternCondition(filterBuilder, "tableName", pattern, parameterVals);
    }
    if(tableType != null) {
      appendSimpleCondition(filterBuilder, "tableType", new String[] {tableType.toString()}, parameterVals);
    }

    Query query = pm.newQuery(MTable.class, filterBuilder.toString());
    query.setResult("tableName");
    query.setOrdering("tableName ascending");
    if (limit >= 0) {
      query.setRange(0, limit);
    }
    Collection<String> names = (Collection<String>) query.executeWithArray(parameterVals.toArray(new String[0]));
    return new ArrayList<>(names);
  }


  @Override
  public List<Table> getTableObjectsByName(String catName, String db, List<String> tbl_names,
      GetProjectionsSpec projectionSpec, String tablePattern) throws MetaException, UnknownDBException {
    List<Table> tables = new ArrayList<>();
    List<MTable> mtables = null;
    catName = normalizeIdentifier(catName);

    List<String> lowered_tbl_names = new ArrayList<>();
    if(tbl_names != null) {
      lowered_tbl_names = new ArrayList<>(tbl_names.size());
      for (String t : tbl_names) {
        lowered_tbl_names.add(normalizeIdentifier(t));
      }
    }

    StringBuilder filterBuilder = new StringBuilder();
    List<String> parameterVals = new ArrayList<>();
    appendPatternCondition(filterBuilder, "database.name", db, parameterVals);
    appendSimpleCondition(filterBuilder, "database.catalogName", new String[] {catName}, parameterVals);
    if(tbl_names != null){
      appendSimpleCondition(filterBuilder, "tableName", lowered_tbl_names.toArray(new String[0]), parameterVals);
    }
    if(tablePattern != null){
      appendPatternCondition(filterBuilder, "tableName", tablePattern, parameterVals);
    }
    Query query = pm.newQuery(MTable.class, filterBuilder.toString()) ;
    List<String> projectionFields = null;

    // If a projection specification has been set, validate it and translate it to JDO columns.
    if (projectionSpec != null) {
      //Validate the projection fields for multi-valued fields.
      projectionFields = TableFields.getMFieldNames(projectionSpec.getFieldList());
    }

    // If the JDO translation resulted in valid JDO columns names, use it to create a projection for the JDO query.
    if (projectionFields != null) {
      // fetch partially filled tables using result clause
      query.setResult(Joiner.on(',').join(projectionFields));
    }

    if (projectionFields == null) {
      mtables = (List<MTable>) query.executeWithArray(parameterVals.toArray(new String[parameterVals.size()]));
    } else {
      if (projectionFields.size() > 1) {
        // Execute the query to fetch the partial results.
        List<Object[]> results = (List<Object[]>) query.executeWithArray(parameterVals.toArray(new String[parameterVals.size()]));
        // Declare the tables array to return the list of tables
        mtables = new ArrayList<>(results.size());
        // Iterate through each row of the result and create the MTable object.
        for (Object[] row : results) {
          MTable mtable = new MTable();
          int i = 0;
          for (Object val : row) {
            MetaStoreServerUtils.setNestedProperty(mtable, projectionFields.get(i), val, true);
            i++;
          }
          mtables.add(mtable);
        }
      } else if (projectionFields.size() == 1) {
        // Execute the query to fetch the partial results.
        List<Object[]> results = (List<Object[]>) query.executeWithArray(parameterVals.toArray(new String[parameterVals.size()]));
        // Iterate through each row of the result and create the MTable object.
        mtables = new ArrayList<>(results.size());
        for (Object row : results) {
          MTable mtable = new MTable();
          MetaStoreServerUtils.setNestedProperty(mtable, projectionFields.get(0), row, true);
          mtables.add(mtable);
        }
      }
    }

    if (mtables == null || mtables.isEmpty()) {
      try {
        baseStore.ensureGetMDatabase(catName, db);
      } catch (NoSuchObjectException nse) {
        throw new UnknownDBException(nse.getMessage());
      }
    } else {
      for (Iterator iter = mtables.iterator(); iter.hasNext(); ) {
        Table tbl = convertToTable((MTable) iter.next(), conf);
        // Retrieve creation metadata if needed
        if (TableType.MATERIALIZED_VIEW.toString().equals(tbl.getTableType())) {
          tbl.setCreationMetadata(
              convertToCreationMetadata(
                  getCreationMetadata(tbl.getCatName(), tbl.getDbName(), tbl.getTableName()), baseStore));
        }
        tables.add(tbl);
      }
    }
    return tables;
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String catName, String dbName)
      throws MetaException, NoSuchObjectException {
    catName = normalizeIdentifier(catName);
    List<Object> params = new ArrayList<>(Arrays.asList(catName, TableType.MATERIALIZED_VIEW.toString(), true));
    if (dbName != null) {
      params.add(normalizeIdentifier(dbName));
    }
    Query query = pm.newQuery(MTable.class,
        "database.catalogName == cat && tableType == tt && rewriteEnabled == re" +
            (dbName != null ? " && database.name == db" : ""));
    query.declareParameters(
        "java.lang.String cat, java.lang.String tt, boolean re" + ((dbName != null) ? " , java.lang.String db" : ""));
    query.setResult("tableName");
    Collection<String> names = (Collection<String>) query.executeWithArray(params.toArray());
    return new ArrayList<>(names);
  }

  @Override
  public List<TableMeta> getTableMeta(String catName, String dbNames, String tableNames, List<String> tableTypes)
      throws MetaException {
    List<TableMeta> metas = new ArrayList<>();
    try {
      // Take the pattern and split it on the | to get all the composing
      // patterns
      StringBuilder filterBuilder = new StringBuilder();
      List<String> parameterVals = new ArrayList<>();
      appendSimpleCondition(filterBuilder, "database.catalogName", new String[] {catName}, parameterVals);
      if (dbNames != null && !dbNames.equals("*")) {
        appendPatternCondition(filterBuilder, "database.name", dbNames, parameterVals);
      }
      if (tableNames != null && !tableNames.equals("*")) {
        appendPatternCondition(filterBuilder, "tableName", tableNames, parameterVals);
      }
      if (tableTypes != null && !tableTypes.isEmpty()) {
        appendSimpleCondition(filterBuilder, "tableType", tableTypes.toArray(new String[0]), parameterVals);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("getTableMeta with filter " + filterBuilder + " params: " +
            StringUtils.join(parameterVals, ", "));
      }
      // Add the fetch group here which retrieves the database object along with the MTable
      // objects. If we don't prefetch the database object, we could end up in a situation where
      // the database gets dropped while we are looping through the tables throwing a
      // JDOObjectNotFoundException. This causes HMS to go into a retry loop which greatly degrades
      // performance of this function when called with dbNames="*" and tableNames="*" (fetch all
      // tables in all databases, essentially a full dump)
      pm.getFetchPlan().addGroup(FetchGroups.FETCH_DATABASE_ON_MTABLE);
      Query query = pm.newQuery(MTable.class, filterBuilder.toString()) ;
      query.setResult("database.name, tableName, tableType, parameters.get(\"comment\"), owner, ownerType");
      List<Object[]> tables = (List<Object[]>) query.executeWithArray(parameterVals.toArray(new String[0]));
      for (Object[] table : tables) {
        TableMeta metaData = new TableMeta(table[0].toString(), table[1].toString(), table[2].toString());
        metaData.setCatName(catName);
        if (table[3] != null) {
          metaData.setComments(table[3].toString());
        }
        if (table[4] != null) {
          metaData.setOwnerName(table[4].toString());
        }
        if (table[5] != null) {
          metaData.setOwnerType(getPrincipalTypeFromStr(table[5].toString()));
        }
        metas.add(metaData);
      }
    } finally {
      pm.getFetchPlan().removeGroup(FetchGroups.FETCH_DATABASE_ON_MTABLE);
    }
    return metas;
  }

  @Override
  public List<String> listTableNamesByFilter(String catName, String dbName, String filter, short maxTables)
      throws MetaException, UnknownDBException {
    Query query = null;
    List<String> tableNames = new ArrayList<>();
    LOG.debug("Executing listTableNamesByFilter");
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);

    try {
      baseStore.ensureGetMDatabase(catName, dbName);
    } catch (NoSuchObjectException nse) {
      throw new UnknownDBException(nse.getMessage());
    }

    Map<String, Object> params = new HashMap<>();
    String queryFilterString = makeQueryFilterString(catName, dbName, null, filter, params);
    query = pm.newQuery(MTable.class);
    query.declareImports("import java.lang.String");
    query.setResult("tableName");
    query.setResultClass(java.lang.String.class);
    if (maxTables >= 0) {
      query.setRange(0, maxTables);
    }
    LOG.debug("filter specified is {}, JDOQL filter is {}", filter, queryFilterString);
    if (LOG.isDebugEnabled()) {
      for (Map.Entry<String, Object> entry : params.entrySet()) {
        LOG.debug("key: {} value: {} class: {}", entry.getKey(), entry.getValue(),
            entry.getValue().getClass().getName());
      }
    }
    String parameterDeclaration = makeParameterDeclarationStringObj(params);
    query.declareParameters(parameterDeclaration);
    query.setFilter(queryFilterString);
    Collection<String> names = (Collection<String>)query.executeWithMap(params);
    // have to emulate "distinct", otherwise tables with the same name may be returned
    tableNames = new ArrayList<>(new HashSet<>(names));
    LOG.debug("Done executing query for listTableNamesByFilter");
    return tableNames;
  }

  @Override
  public List<String> listPartitionNamesByFilter(TableName tableName, GetPartitionsArgs args)
      throws MetaException, NoSuchObjectException {

    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());

    MTable mTable = ensureGetMTable(tableName);
    List<FieldSchema> partitionKeys = convertToFieldSchemas(mTable.getPartitionKeys());
    String filter = args.getFilter();
    final ExpressionTree tree = (filter != null && !filter.isEmpty())
        ? PartFilterExprUtil.parseFilterTree(filter) : ExpressionTree.EMPTY_TREE;
    return new GetListHelper<TableName, String>(this, tableName) {
      private final MetaStoreDirectSql.SqlFilterForPushdown filter = new MetaStoreDirectSql.SqlFilterForPushdown();

      @Override
      protected boolean canUseDirectSql() throws MetaException {
        return getDirectSql().generateSqlFilterForPushdown(catName, dbName, tblName,
            partitionKeys, tree, null, filter);
      }

      @Override
      protected List<String> getSqlResult() throws MetaException {
        return getDirectSql().getPartitionNamesViaSql(filter, partitionKeys,
            getDefaultPartitionName(args.getDefaultPartName()), null, args.getMax());
      }

      @Override
      protected List<String> getJdoResult()
          throws MetaException, NoSuchObjectException, InvalidObjectException {
        return getPartitionNamesViaOrm(catName, dbName, tblName, tree, null,
            args.getMax(), true, partitionKeys);
      }
    }.run(false);
  }

  private String makeParameterDeclarationStringObj(Map<String, Object> params) {
    //Create the parameter declaration string
    StringBuilder paramDecl = new StringBuilder();
    for (Map.Entry<String, Object> entry : params.entrySet()) {
      paramDecl.append(", ");
      paramDecl.append(entry.getValue().getClass().getName());
      paramDecl.append(' ');
      paramDecl.append(entry.getKey());
    }
    return paramDecl.toString();
  }

  /**
   * Makes a JDO query filter string for tables or partitions.
   * @param dbName Database name.
   * @param table Table. If null, the query returned is over tables in a database.
   *   If not null, the query returned is over partitions in a table.
   * @param tree The expression tree from which JDOQL filter will be made.
   * @param params Parameters for the filter. Some parameters may be added here.
   * @param isValidatedFilter Whether the filter was pre-validated for JDOQL pushdown
   *   by the client; if it was and we fail to create a filter, we will throw.
   * @return Resulting filter. Can be null if isValidatedFilter is false, and there was error.
   */
  private String makeQueryFilterString(String catName, String dbName, Table table,
      ExpressionTree tree, Map<String, Object> params,
      boolean isValidatedFilter) throws MetaException {
    assert tree != null;
    ExpressionTree.FilterBuilder queryBuilder = new ExpressionTree.FilterBuilder(isValidatedFilter);
    if (table != null) {
      queryBuilder.append("table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3");
      params.put("t1", table.getTableName());
      params.put("t2", table.getDbName());
      params.put("t3", table.getCatName());
    } else {
      queryBuilder.append("database.name == dbName && database.catalogName == catName");
      params.put("dbName", dbName);
      params.put("catName", catName);
    }

    tree.accept(new ExpressionTree.JDOFilterGenerator(baseStore.getConf(),
        table != null ? table.getPartitionKeys() : null, queryBuilder, params));
    if (queryBuilder.hasError()) {
      assert !isValidatedFilter;
      LOG.debug("JDO filter pushdown cannot be used: {}", queryBuilder.getErrorMessage());
      return null;
    }
    String jdoFilter = queryBuilder.getFilter();
    LOG.debug("jdoFilter = {}", jdoFilter);
    return jdoFilter;
  }

  private String makeQueryFilterString(String catName, String dbName, String tblName,
      ExpressionTree tree, Map<String, Object> params,
      boolean isValidatedFilter, List<FieldSchema> partitionKeys) throws MetaException {
    assert tree != null;
    ExpressionTree.FilterBuilder queryBuilder = new ExpressionTree.FilterBuilder(isValidatedFilter);
    queryBuilder.append("table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3");
    params.put("t1", tblName);
    params.put("t2", dbName);
    params.put("t3", catName);
    tree.accept(new ExpressionTree.JDOFilterGenerator(baseStore.getConf(), partitionKeys, queryBuilder, params));
    if (queryBuilder.hasError()) {
      assert !isValidatedFilter;
      LOG.debug("JDO filter pushdown cannot be used: {}", queryBuilder.getErrorMessage());
      return null;
    }
    String jdoFilter = queryBuilder.getFilter();
    LOG.debug("jdoFilter = {}", jdoFilter);
    return jdoFilter;
  }


  private List<String> getPartitionNamesViaOrm(String catName, String dbName, String tblName,
      ExpressionTree tree, String order, Integer maxParts, boolean isValidatedFilter,
      List<FieldSchema> partitionKeys) throws MetaException {
    Map<String, Object> params = new HashMap<String, Object>();
    String jdoFilter = makeQueryFilterString(catName, dbName, tblName, tree,
        params, isValidatedFilter, partitionKeys);
    if (jdoFilter == null) {
      assert !isValidatedFilter;
      throw new MetaException("Failed to generate filter.");
    }

    try (QueryWrapper query = new QueryWrapper(pm.newQuery(
        "select partitionName from org.apache.hadoop.hive.metastore.model.MPartition"))) {
      query.setFilter(jdoFilter);
      List<Object[]> orderSpecs = MetaStoreUtils.makeOrderSpecs(order);
      StringBuilder builder = new StringBuilder();
      for (Object[] spec : orderSpecs) {
        // TODO: order by casted value if the type of partition key is not string
        builder.append("values.get(").append(spec[0]).append(") ").append(spec[1]).append(",");
      }
      if (builder.length() > 0) {
        builder.setLength(builder.length() - 1);
        query.setOrdering(builder.toString());
      } else {
        query.setOrdering("partitionName ascending");
      }

      if (maxParts > -1) {
        query.setRange(0, maxParts);
      }

      String parameterDeclaration = makeParameterDeclarationStringObj(params);
      query.declareParameters(parameterDeclaration);
      Collection jdoRes = (Collection) query.executeWithMap(params);
      List<String> result = new LinkedList<String>();
      for (Object partName : jdoRes) {
        result.add((String) partName);
      }
      return result;
    }
  }

  /**
   * Gets the default partition name.
   * @param inputDefaultPartName Incoming default partition name.
   * @return Valid default partition name
   */
  private String getDefaultPartitionName(String inputDefaultPartName) {
    return (((inputDefaultPartName == null) || (inputDefaultPartName.isEmpty()))
        ? MetastoreConf.getVar(baseStore.getConf(), MetastoreConf.ConfVars.DEFAULTPARTITIONNAME)
        : inputDefaultPartName);
  }

  /**
   * Gets the table object for a given table, throws if anything goes wrong.
   * @param tableName Table name.
   * @return Table object.
   */
  @Override
  public MTable ensureGetMTable(TableName tableName)
      throws NoSuchObjectException {
    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());
    MTable mtable = getMTable(catName, dbName, tblName);
    if (mtable == null) {
      throw new NoSuchObjectException(
          "Specified catalog.database.table does not exist : " + tableName);
    }
    return mtable;
  }

  @Override
  public List<String> listPartitionNames(TableName tableName, String defaultPartName, byte[] exprBytes, String order,
      int maxParts) throws MetaException, NoSuchObjectException {
    final String defaultPartitionName = getDefaultPartitionName(defaultPartName);
    final boolean isEmptyFilter = exprBytes == null || (exprBytes.length == 1 && exprBytes[0] == -1);
    ExpressionTree tmp = null;
    if (!isEmptyFilter) {
      tmp = PartFilterExprUtil.makeExpressionTree(expressionProxy, exprBytes,
          getDefaultPartitionName(defaultPartName), baseStore.getConf());
    }
    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());
    final ExpressionTree exprTree = tmp;
    return new GetListHelper<TableName, String>(this, tableName) {
      private List<String> getPartNamesPrunedByExpr(Table table, boolean isJdoQuery) throws MetaException {
        int max = isEmptyFilter ? maxParts : -1;
        List<String> result;
        if (isJdoQuery) {
          result = getPartitionNamesViaOrm(catName, dbName, tblName, ExpressionTree.EMPTY_TREE,
              order, max, true, table.getPartitionKeys());
        } else {
          MetaStoreDirectSql.SqlFilterForPushdown filter = new MetaStoreDirectSql.SqlFilterForPushdown(table, false);
          result = getDirectSql().getPartitionNamesViaSql(filter, table.getPartitionKeys(),
              defaultPartitionName, order, max);
        }
        if (!isEmptyFilter) {
          prunePartitionNamesByExpr(catName, dbName, tblName, result,
              new GetPartitionsArgs.GetPartitionsArgsBuilder()
                  .expr(exprBytes).defaultPartName(defaultPartName).max(maxParts).build());
        }
        return result;
      }
      @Override
      protected List<String> getSqlResult() throws MetaException {
        MetaStoreDirectSql.SqlFilterForPushdown filter = new MetaStoreDirectSql.SqlFilterForPushdown(getTable(), false);
        List<String> partNames = null;
        Table table = getTable();
        if (exprTree != null) {
          if (getDirectSql().generateSqlFilterForPushdown(table.getCatName(), table.getDbName(), table.getTableName(),
              getTable().getPartitionKeys(), exprTree, defaultPartitionName, filter)) {
            partNames = getDirectSql().getPartitionNamesViaSql(filter, table.getPartitionKeys(),
                defaultPartitionName, order, (int)maxParts);
          }
        }
        if (partNames == null) {
          partNames = getPartNamesPrunedByExpr(table, false);
        }
        return partNames;
      }
      @Override
      protected List<String> getJdoResult() throws MetaException, NoSuchObjectException {
        List<String> result = null;
        if (exprTree != null) {
          try {
            result = getPartitionNamesViaOrm(catName, dbName, tblName, exprTree, order,
                maxParts, true, getTable().getPartitionKeys());
          } catch (MetaException e) {
            result = null;
          }
        }
        if (result == null) {
          result = getPartNamesPrunedByExpr(getTable(), true);
        }
        return result;
      }
    }.run(true);
  }

  private boolean prunePartitionNamesByExpr(String catName, String dbName, String tblName,
      List<String> result, GetPartitionsArgs args) throws MetaException {
    MTable mTable = getMTable(catName, dbName, tblName);
    List<FieldSchema> partitionKeys = convertToFieldSchemas(mTable.getPartitionKeys());
    boolean hasUnknownPartitions = expressionProxy.filterPartitionsByExpr(
        partitionKeys,
        args.getExpr(),
        getDefaultPartitionName(args.getDefaultPartName()),
        result);
    if (args.getMax() >= 0 && result.size() > args.getMax()) {
      result = result.subList(0, args.getMax());
    }
    return hasUnknownPartitions;
  }

  @Override
  public boolean getPartitionsByExpr(TableName tableName, List<Partition> result, GetPartitionsArgs args)
      throws TException {
    assert result != null;
    byte[] expr = args.getExpr();
    final ExpressionTree exprTree = expr.length != 0 ? PartFilterExprUtil.makeExpressionTree(
        expressionProxy, expr, getDefaultPartitionName(args.getDefaultPartName()), baseStore.getConf()) : ExpressionTree.EMPTY_TREE;
    final AtomicBoolean hasUnknownPartitions = new AtomicBoolean(false);

    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());
    MTable mTable = ensureGetMTable(tableName);
    List<FieldSchema> partitionKeys = convertToFieldSchemas(mTable.getPartitionKeys());
    boolean isAcidTable = TxnUtils.isAcidTable(mTable.getParameters());
    result.addAll(new GetListHelper<TableName, Partition>(this, tableName) {
      @Override
      protected List<Partition> getSqlResult() throws MetaException {
        // If we have some sort of expression tree, try SQL filter pushdown.
        if (exprTree != null) {
          MetaStoreDirectSql.SqlFilterForPushdown filter = new MetaStoreDirectSql.SqlFilterForPushdown();
          if (getDirectSql().generateSqlFilterForPushdown(catName, dbName, tblName, partitionKeys,
              exprTree, args.getDefaultPartName(), filter)) {
            String catalogName = (catName != null) ? catName : getDefaultCatalog(baseStore.getConf());
            return getDirectSql().getPartitionsViaSqlFilter(catalogName, dbName, tblName, filter,
                isAcidTable, args);
          }
        }
        // We couldn't do SQL filter pushdown. Get names via normal means.
        List<String> partNames = new LinkedList<>();
        hasUnknownPartitions.set(getPartitionNamesPrunedByExprNoTxn(
            catName, dbName, tblName, partitionKeys, expr, args.getDefaultPartName(), (short) args.getMax(), partNames));
        GetPartitionsArgs newArgs = new GetPartitionsArgs.GetPartitionsArgsBuilder(args).partNames(partNames).build();
        return getDirectSql().getPartitionsViaPartNames(catName, dbName, tblName, newArgs);
      }

      @Override
      protected List<Partition> getJdoResult() throws MetaException, NoSuchObjectException {
        // If we have some sort of expression tree, try JDOQL filter pushdown.
        List<Partition> result = null;
        if (exprTree != null) {
          result = getPartitionsViaOrmFilter(catName, dbName, tblName, exprTree,
              false, partitionKeys, isAcidTable, args);
        }
        if (result == null) {
          // We couldn't do JDOQL filter pushdown. Get names via normal means.
          List<String> partNames = new ArrayList<>();
          hasUnknownPartitions.set(getPartitionNamesPrunedByExprNoTxn(
              catName, dbName, tblName, partitionKeys, expr, args.getDefaultPartName(), (short) args.getMax(), partNames));
          GetPartitionsArgs newArgs = new GetPartitionsArgs.GetPartitionsArgsBuilder(args).partNames(partNames).build();
          result = getPartitionsViaOrmFilter(catName, dbName, tblName, isAcidTable, newArgs);
        }
        return result;
      }
    }.run(false));
    return hasUnknownPartitions.get();
  }

  /**
   * Gets partition names from the table via ORM (JDOQL) filter pushdown.
   * @param tblName The table.
   * @param tree The expression tree from which JDOQL filter will be made.
   * @param isValidatedFilter Whether the filter was pre-validated for JDOQL pushdown by a client
   *   (old hive client or non-hive one); if it was and we fail to create a filter, we will throw.
   * @param args additional arguments for getting partitions
   * @return Resulting partitions. Can be null if isValidatedFilter is false, and
   *         there was error deriving the JDO filter.
   */
  private List<Partition> getPartitionsViaOrmFilter(String catName, String dbName, String tblName, ExpressionTree tree,
      boolean isValidatedFilter, List<FieldSchema> partitionKeys, boolean isAcidTable,
      GetPartitionsArgs args) throws MetaException {
    Map<String, Object> params = new HashMap<>();
    String jdoFilter =
        makeQueryFilterString(catName, dbName, tblName, tree, params, isValidatedFilter, partitionKeys);
    if (jdoFilter == null) {
      assert !isValidatedFilter;
      return null;
    }
    Query query = pm.newQuery(MPartition.class, jdoFilter);
    if (args.getMax() >= 0) {
      // User specified a row limit, set it on the Query
      query.setRange(0, args.getMax());
    }
    String parameterDeclaration = makeParameterDeclarationStringObj(params);
    query.declareParameters(parameterDeclaration);
    query.setOrdering("partitionName ascending");
    List<MPartition> mparts = (List<MPartition>) query.executeWithMap(params);
    LOG.debug("Done executing query for getPartitionsViaOrmFilter");
    pm.retrieveAll(mparts); // TODO: why is this inconsistent with what we get by names?
    LOG.debug("Done retrieving all objects for getPartitionsViaOrmFilter");
    List<Partition> results =
        convertToParts(catName, dbName, tblName, mparts, isAcidTable, conf, args);
    return results;
  }

  /**
   * Gets partition names from the table via ORM (JDOQL) name filter.
   * @param dbName Database name.
   * @param tblName Table name.
   * @param isAcidTable True if the table is ACID
   * @param args additional arguments for getting partitions
   * @return Resulting partitions.
   */
  private List<Partition> getPartitionsViaOrmFilter(String catName, String dbName, String tblName,
      boolean isAcidTable, GetPartitionsArgs args) throws MetaException {
    List<String> partNames = args.getPartNames();
    if (partNames.isEmpty()) {
      return Collections.emptyList();
    }
    return Batchable.runBatched(batchSize, partNames, new Batchable<String, Partition>() {
      @Override
      public List<Partition> run(List<String> input) throws MetaException {
        Pair<Query, Map<String, String>> queryWithParams =
            getPartQueryWithParams(pm, catName, dbName, tblName, input);

        try (QueryWrapper query = new QueryWrapper(queryWithParams.getLeft())) {
          query.setResultClass(MPartition.class);
          query.setClass(MPartition.class);
          query.setOrdering("partitionName ascending");

          List<MPartition> mparts = (List<MPartition>) query.executeWithMap(queryWithParams.getRight());
          List<Partition> partitions = convertToParts(catName, dbName, tblName, mparts,
              isAcidTable, conf, args);

          return partitions;
        }
      }
    });
  }


  /**
   * Gets the partition names from a table, pruned using an expression.
   * @param catName
   * @param dbName
   * @param tblName
   * @param expr Expression.
   * @param defaultPartName Default partition name from job config, if any.
   * @param maxParts Maximum number of partition names to return.
   * @param result The resulting names.
   * @return Whether the result contains any unknown partitions.
   */
  private boolean getPartitionNamesPrunedByExprNoTxn(String catName, String dbName, String tblName, List<FieldSchema> partColumns, byte[] expr,
      String defaultPartName, short maxParts, List<String> result) throws MetaException {
    result.addAll(getPartitionNamesNoTxn(catName, dbName, tblName, (short) -1));
    return prunePartitionNamesByExpr(catName, dbName, tblName, result,
        new GetPartitionsArgs.GetPartitionsArgsBuilder()
            .expr(expr).defaultPartName(defaultPartName).max(maxParts).build());
  }

  private List<String> getPartitionNamesNoTxn(String catName, String dbName, String tableName, short max) {
    List<String> pns = new ArrayList<>();
    if (max == 0) {
      return pns;
    }
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tableName = normalizeIdentifier(tableName);
    try (QueryWrapper query = new QueryWrapper(
        pm.newQuery("select partitionName from org.apache.hadoop.hive.metastore.model.MPartition "
            + "where table.database.name == t1 && table.tableName == t2 && table.database.catalogName == t3 "
            + "order by partitionName asc"))) {
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      query.setResult("partitionName");

      if (max > 0) {
        query.setRange(0, max);
      }
      Collection<String> names = (Collection<String>) query.execute(dbName, tableName, catName);
      pns.addAll(names);

      return pns;
    }
  }

  @Override
  public List<Partition> getPartitionsByNames(TableName tableName, GetPartitionsArgs args)
      throws MetaException, NoSuchObjectException {
    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());
    return new GetListHelper<TableName, Partition>(this, tableName) {
      @Override
      protected List<Partition> getSqlResult() throws MetaException {
        return getDirectSql().getPartitionsViaPartNames(catName, dbName, tblName, args);
      }
      @Override
      protected List<Partition> getJdoResult() throws MetaException, NoSuchObjectException {
        return getPartitionsViaOrmFilter(catName, dbName, tblName, false, args);
      }
    }.run(false);
  }

  @Override
  public Partition alterPartition(TableName tableName, List<String> part_vals, Partition new_part,
      String queryValidWriteIds) throws InvalidObjectException, MetaException {
    String catName = normalizeIdentifier(tableName.getCat());
    String dbname = normalizeIdentifier(tableName.getDb());
    String name = normalizeIdentifier(tableName.getTable());
    AtomicReference<MColumnDescriptor> oldCd = new AtomicReference<>();
    Partition result = alterPartitionNoTxn(catName, dbname, name, part_vals, new_part, queryValidWriteIds, oldCd);
    removeUnusedColumnDescriptor(oldCd.get());
    return result;
  }

  /**
   * Alters an existing partition. Initiates copy of SD. Returns the old CD.
   * @param part_vals Partition values (of the original partition instance)
   * @param newPart Partition object containing new information
   */
  private Partition alterPartitionNoTxn(String catName, String dbname, String name,
      List<String> part_vals, Partition newPart, String validWriteIds, AtomicReference<MColumnDescriptor> oldCd)
      throws InvalidObjectException, MetaException {
    MTable table = this.getMTable(newPart.getCatName(), newPart.getDbName(), newPart.getTableName());
    MPartition oldp = getMPartition(catName, dbname, name, part_vals, table);
    return alterPartitionNoTxn(catName, dbname, name, oldp, newPart,
        validWriteIds, oldCd, table);
  }

  private Partition alterPartitionNoTxn(String catName, String dbname,
      String name, MPartition oldp, Partition newPart,
      String validWriteIds,
      AtomicReference<MColumnDescriptor> oldCd, MTable table)
      throws InvalidObjectException, MetaException {
    catName = normalizeIdentifier(catName);
    name = normalizeIdentifier(name);
    dbname = normalizeIdentifier(dbname);
    MPartition newp = convertToMPart(newPart, table);
    MColumnDescriptor oldCD = null;
    MStorageDescriptor oldSD = oldp.getSd();
    if (oldSD != null) {
      oldCD = oldSD.getCD();
    }
    if (newp == null) {
      throw new InvalidObjectException("partition does not exist.");
    }
    oldp.setValues(newp.getValues());
    oldp.setPartitionName(newp.getPartitionName());
    boolean isTxn = TxnUtils.isTransactionalTable(table.getParameters());
    if (isTxn && areTxnStatsSupported) {
      // Transactional table is altered without a txn. Make sure there are no changes to the flag.
      String errorMsg = verifyStatsChangeCtx(TableName.getDbTable(dbname, name),
          oldp.getParameters(),
          newPart.getParameters(), newPart.getWriteId(), validWriteIds, false);
      if (errorMsg != null) {
        throw new MetaException(errorMsg);
      }
    }
    oldp.setParameters(newPart.getParameters());
    if (!TableType.VIRTUAL_VIEW.name().equals(oldp.getTable().getTableType())) {
      copyMSD(newp.getSd(), oldp.getSd());
    }
    if (newp.getCreateTime() != oldp.getCreateTime()) {
      oldp.setCreateTime(newp.getCreateTime());
    }
    if (newp.getLastAccessTime() != oldp.getLastAccessTime()) {
      oldp.setLastAccessTime(newp.getLastAccessTime());
    }

    // If transactional, add/update the MUPdaterTransaction
    // for the current updater query.
    if (isTxn) {
      if (!areTxnStatsSupported) {
        StatsSetupConst.setBasicStatsState(oldp.getParameters(), StatsSetupConst.FALSE);
      } else if (validWriteIds != null && newPart.getWriteId() > 0) {
        // Check concurrent INSERT case and set false to the flag.
        if (!isCurrentStatsValidForTheQuery(oldp.getParameters(), oldp.getWriteId(), validWriteIds, true)) {
          StatsSetupConst.setBasicStatsState(oldp.getParameters(), StatsSetupConst.FALSE);
          LOG.info("Removed COLUMN_STATS_ACCURATE from the parameters of the partition " +
              dbname + "." + name + "." + oldp.getPartitionName() + " will be made persistent.");
        }
        oldp.setWriteId(newPart.getWriteId());
      }
    }

    oldCd.set(oldCD);
    return convertToPart(catName, dbname, name, oldp, TxnUtils.isAcidTable(table.getParameters()), conf);
  }

  @Override
  public List<Partition> alterPartitions(TableName tableName, List<List<String>> part_vals,
      List<Partition> newParts, long writeId, String queryWriteIdList) throws InvalidObjectException, MetaException {
    List<Partition> results = new ArrayList<>(newParts.size());
    if (newParts.isEmpty()) {
      return results;
    }
    try {
      MTable table = ensureGetMTable(tableName);
      if (writeId > 0) {
        newParts.forEach(newPart -> newPart.setWriteId(writeId));
      }
      List<FieldSchema> partCols = convertToFieldSchemas(table.getPartitionKeys());
      List<String> partNames = new ArrayList<>();
      for (List<String> partVal : part_vals) {
        partNames.add(Warehouse.makePartName(partCols, partVal));
      }
      results = alterPartitionsInternal(table, partNames, newParts, queryWriteIdList);
    } catch (NoSuchObjectException nse) {
      throw new MetaException(nse.getMessage());
    }
    // commit the changes
    return results;
  }

  protected List<Partition> alterPartitionsInternal(MTable table,
      List<String> partNames, List<Partition> newParts, String queryWriteIdList)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    // Validate new parts: StorageDescriptor and SerDeInfo must be set in Partition.
    if (!TableType.VIRTUAL_VIEW.name().equals(table.getTableType())) {
      for (Partition newPart : newParts) {
        if (!newPart.isSetSd() || !newPart.getSd().isSetSerdeInfo()) {
          throw new InvalidObjectException("Partition does not set storageDescriptor or serdeInfo.");
        }
      }
    }

    String dbName = table.getDatabase().getName();
    String tblName = table.getTableName();
    for (Partition tmpPart : newParts) {
      if (!tmpPart.getDbName().equalsIgnoreCase(dbName)) {
        throw new MetaException("Invalid DB name : " + tmpPart.getDbName());
      }
      if (!tmpPart.getTableName().equalsIgnoreCase(tblName)) {
        throw new MetaException("Invalid table name : " + tmpPart.getDbName());
      }
    }
    return new GetListHelper<TableName, Partition>(this, null) {
      @Override
      protected List<Partition> getSqlResult()
          throws MetaException {
        return getDirectSql().alterPartitions(table, partNames, newParts, queryWriteIdList);
      }

      @Override
      protected List<Partition> getJdoResult()
          throws MetaException, InvalidObjectException {
        return alterPartitionsViaJdo(table, partNames, newParts, queryWriteIdList);
      }
    }.run(false);
  }

  private List<Partition> alterPartitionsViaJdo(MTable table, List<String> partNames,
      List<Partition> newParts, String queryWriteIdList)
      throws MetaException, InvalidObjectException {
    String catName = table.getDatabase().getCatalogName();
    String dbName = table.getDatabase().getName();
    String tblName = table.getTableName();
    List<Partition> results = new ArrayList<>(newParts.size());
    List<MPartition> mPartitionList;

    try (QueryWrapper query = new QueryWrapper(pm.newQuery(MPartition.class,
        "table.tableName == t1 && table.database.name == t2 && t3.contains(partitionName) " +
            " && table.database.catalogName == t4"))) {
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.util.Collection t3, "
          + "java.lang.String t4");
      mPartitionList = (List<MPartition>) query.executeWithArray(tblName, dbName, partNames, catName);
      pm.retrieveAll(mPartitionList);

      if (mPartitionList.size() > newParts.size()) {
        throw new MetaException("Expecting only one partition but more than one partitions are found.");
      }

      Map<List<String>, MPartition> mPartsMap = new HashMap();
      for (MPartition mPartition : mPartitionList) {
        mPartsMap.put(mPartition.getValues(), mPartition);
      }

      Set<MColumnDescriptor> oldCds = new HashSet<>();
      AtomicReference<MColumnDescriptor> oldCdRef = new AtomicReference<>();
      for (Partition tmpPart : newParts) {
        oldCdRef.set(null);
        Partition result = alterPartitionNoTxn(catName, dbName, tblName,
            mPartsMap.get(tmpPart.getValues()), tmpPart, queryWriteIdList, oldCdRef, table);
        results.add(result);
        if (oldCdRef.get() != null) {
          oldCds.add(oldCdRef.get());
        }
      }
      for (MColumnDescriptor oldCd : oldCds) {
        removeUnusedColumnDescriptor(oldCd);
      }
    }

    return results;
  }

  @Override
  public List<Partition> getPartitionsByFilter(TableName tableName, GetPartitionsArgs args)
      throws MetaException, NoSuchObjectException {

    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());

    MTable mTable = ensureGetMTable(tableName);
    List<FieldSchema> partitionKeys = convertToFieldSchemas(mTable.getPartitionKeys());
    boolean isAcidTable = TxnUtils.isAcidTable(mTable.getParameters());
    String filter = args.getFilter();
    final ExpressionTree tree = (filter != null && !filter.isEmpty())
        ? PartFilterExprUtil.parseFilterTree(filter) : ExpressionTree.EMPTY_TREE;
    return new GetListHelper<TableName, Partition>(this, tableName) {
      private final MetaStoreDirectSql.SqlFilterForPushdown filter = new MetaStoreDirectSql.SqlFilterForPushdown();

      @Override
      protected boolean canUseDirectSql() throws MetaException {
        return getDirectSql().generateSqlFilterForPushdown(catName, dbName, tblName, partitionKeys, tree, null, filter);
      }

      @Override
      protected List<Partition> getSqlResult() throws MetaException {
        return getDirectSql().getPartitionsViaSqlFilter(catName, dbName, tblName, filter, isAcidTable, args);
      }

      @Override
      protected List<Partition> getJdoResult() throws MetaException, NoSuchObjectException {
        return getPartitionsViaOrmFilter(catName, dbName, tblName, tree, true,
            partitionKeys, isAcidTable, args);
      }
    }.run(false);
  }

  @Override
  public List<Partition> getPartitionSpecsByFilterAndProjection(Table table, GetProjectionsSpec partitionsProjectSpec,
      GetPartitionsFilterSpec filterSpec) throws MetaException, NoSuchObjectException {
    List<String> fieldList = null;
    String inputIncludePattern = null;
    String inputExcludePattern = null;
    if (partitionsProjectSpec != null) {
      fieldList = partitionsProjectSpec.getFieldList();
      if (partitionsProjectSpec.isSetIncludeParamKeyPattern()) {
        inputIncludePattern = partitionsProjectSpec.getIncludeParamKeyPattern();
      }
      if (partitionsProjectSpec.isSetExcludeParamKeyPattern()) {
        inputExcludePattern = partitionsProjectSpec.getExcludeParamKeyPattern();
      }
    }
    TableName tableName = new TableName(table.getCatName(), table.getDbName(), table.getTableName());
    if (fieldList == null || fieldList.isEmpty()) {
      // no fields are requested. Fallback to regular getPartitions implementation to return all the fields
      GetPartitionsArgs.GetPartitionsArgsBuilder argsBuilder = new GetPartitionsArgs.GetPartitionsArgsBuilder()
          .excludeParamKeyPattern(inputExcludePattern)
          .includeParamKeyPattern(inputIncludePattern);
      return getPartitions(tableName, argsBuilder.build());
    }

    // anonymous class below requires final String objects
    final String includeParamKeyPattern = inputIncludePattern;
    final String excludeParamKeyPattern = inputExcludePattern;

    return new GetListHelper<TableName, Partition>(this, tableName, fieldList) {
      private final MetaStoreDirectSql.SqlFilterForPushdown filter = new MetaStoreDirectSql.SqlFilterForPushdown();
      private ExpressionTree tree;

      @Override
      protected boolean canUseDirectSql() throws MetaException {
        if (filterSpec.isSetFilterMode() && filterSpec.getFilterMode().equals(PartitionFilterMode.BY_EXPR)) {
          // if the filter mode is BY_EXPR initialize the filter and generate the expression tree
          // if there are more than one filter string we AND them together
          initExpressionTree();
          return getDirectSql().generateSqlFilterForPushdown(table.getCatName(), table.getDbName(), table.getTableName(),
              table.getPartitionKeys(), tree, null, filter);
        }
        // BY_VALUES and BY_NAMES are always supported
        return true;
      }

      private void initExpressionTree() throws MetaException {
        StringBuilder filterBuilder = new StringBuilder();
        int len = filterSpec.getFilters().size();
        List<String> filters = filterSpec.getFilters();
        for (int i = 0; i < len; i++) {
          filterBuilder.append('(');
          filterBuilder.append(filters.get(i));
          filterBuilder.append(')');
          if (i + 1 < len) {
            filterBuilder.append(" AND ");
          }
        }
        String filterStr = filterBuilder.toString();
        tree = PartFilterExprUtil.parseFilterTree(filterStr);
      }

      @Override
      protected List<Partition> getSqlResult() throws MetaException {
        return getDirectSql().getPartitionsUsingProjectionAndFilterSpec(getTable(), getPartitionFields(),
                includeParamKeyPattern, excludeParamKeyPattern, filterSpec, filter);
      }

      @Override
      protected List<Partition> getJdoResult() throws MetaException {
        // For single-valued fields we can use setResult() to implement projection of fields but
        // JDO doesn't support multi-valued fields in setResult() so currently JDO implementation
        // fallbacks to full-partition fetch if the requested fields contain multi-valued fields
        List<String> fieldNames = PartitionProjectionEvaluator.getMPartitionFieldNames(getPartitionFields());
        Map<String, Object> params = new HashMap<>();
        String jdoFilter = null;
        if (filterSpec.isSetFilterMode()) {
          // generate the JDO filter string
          switch(filterSpec.getFilterMode()) {
          case BY_EXPR:
            if (tree == null) {
              // tree could be null when directSQL is disabled
              initExpressionTree();
            }
            jdoFilter =
                makeQueryFilterString(table.getCatName(), table.getDbName(), table, tree, params,
                    true);
            if (jdoFilter == null) {
              throw new MetaException("Could not generate JDO filter from given expression");
            }
            break;
          case BY_NAMES:
            jdoFilter = getJDOFilterStrForPartitionNames(table.getCatName(), table.getDbName(),
                table.getTableName(), filterSpec.getFilters(), params);
            break;
          case BY_VALUES:
            jdoFilter = getJDOFilterStrForPartitionVals(table, filterSpec.getFilters(), params);
            break;
          default:
            throw new MetaException("Unsupported filter mode " + filterSpec.getFilterMode());
          }
        } else {
          // filter mode is not set create simple JDOFilterStr and params
          jdoFilter = "table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3";
          params.put("t1", normalizeIdentifier(table.getTableName()));
          params.put("t2", normalizeIdentifier(table.getDbName()));
          params.put("t3", normalizeIdentifier(table.getCatName()));
        }
        try {
          List<MPartition> mparts = listMPartitionsWithProjection(fieldNames, jdoFilter, params);
          return convertToParts(table.getCatName(), table.getDbName(), table.getTableName(),
              mparts, false, conf, new GetPartitionsArgs.GetPartitionsArgsBuilder()
              .excludeParamKeyPattern(excludeParamKeyPattern)
              .includeParamKeyPattern(includeParamKeyPattern)
              .build());
        } catch (MetaException me) {
          throw me;
        } catch (Exception e) {
          throw new MetaException(e.getMessage());
        }
      }
    }.run(true);
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(TableName tableName, GetPartitionsArgs args)
      throws MetaException, InvalidObjectException, NoSuchObjectException {
    List<Partition> partitions;
    LOG.debug("executing listPartitionNamesPsWithAuth");
    MTable mtbl = ensureGetMTable(tableName);
    String userName = args.getUserName();
    List<String> groupNames = args.getGroupNames();
    List<String> part_vals = args.getPart_vals();
    List<String> partNames = args.getPartNames();
    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());
    boolean getauth = null != userName && null != groupNames &&
        "TRUE".equalsIgnoreCase(
            mtbl.getParameters().get("PARTITION_LEVEL_PRIVILEGE"));
    if (MetaStoreUtils.arePartValsEmpty(part_vals) && partNames == null) {
      partitions = getPartitions(tableName, args);
    } else  if (partNames != null) {
      partitions = getPartitionsByNames(tableName, args);
    } else {
      partitions = getPartitionsByPs(tableName, args);
    }
    if (getauth) {
      for (Partition part : partitions) {
        String partName = Warehouse.makePartName(convertToFieldSchemas(mtbl
            .getPartitionKeys()), part.getValues());
        PrincipalPrivilegeSet partAuth = baseStore.getPartitionPrivilegeSet(catName, dbName,
            tblName, partName, userName, groupNames);
        part.setPrivileges(partAuth);
      }
    }
    return partitions;
  }

  private List<Partition> getPartitionsByPs(TableName tableName, GetPartitionsArgs args)
      throws MetaException, NoSuchObjectException {
    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());

    return new GetListHelper<TableName, Partition>(this, tableName) {

      @Override
      protected List<Partition> getSqlResult() throws MetaException {
        return getDirectSql().getPartitionsViaSqlPs(getTable(), args);
      }

      @Override
      protected List<Partition> getJdoResult()
          throws MetaException, NoSuchObjectException {
        List<Partition> result = new ArrayList<>();
        Collection<MPartition> parts = getPartitionPsQueryResults(catName, dbName, tblName,
            args.getPart_vals(), args.getMax(), null);
        boolean isAcidTable = TxnUtils.isAcidTable(getTable());
        for (MPartition o : parts) {
          Partition part = convertToPart(catName, dbName, tblName, o, isAcidTable, conf, args);
          result.add(part);
        }
        return result;
      }
    }.run(true);
  }

  /**
   * Retrieves a Collection of partition-related results from the database that match
   *  the partial specification given for a specific table.
   * @param dbName the name of the database
   * @param tableName the name of the table
   * @param part_vals the partial specification values
   * @param max_parts the maximum number of partitions to return
   * @param resultsCol the metadata column of the data to return, e.g. partitionName, etc.
   *        if resultsCol is empty or null, a collection of MPartition objects is returned
   * @return A Collection of partition-related items from the db that match the partial spec
   *          for a table.  The type of each item in the collection corresponds to the column
   *          you want results for.  E.g., if resultsCol is partitionName, the Collection
   *          has types of String, and if resultsCol is null, the types are MPartition.
   */
  private <T> Collection<T> getPartitionPsQueryResults(String catName, String dbName,
      String tableName, List<String> part_vals,
      int max_parts, String resultsCol)
      throws MetaException, NoSuchObjectException {

    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tableName = normalizeIdentifier(tableName);
    Table table = getTable(new TableName(catName, dbName, tableName), null, -1);
    if (table == null) {
      throw new NoSuchObjectException(TableName.getQualified(catName, dbName, tableName) + " table not found");
    }
    // size is known since it contains dbName, catName, tblName and partialRegex
    // pattern
    Map<String, String> params = new HashMap<>(4);
    String filter = getJDOFilterStrForPartitionVals(table, part_vals, params);
    try (QueryWrapper query = new QueryWrapper(pm.newQuery(MPartition.class))) {
      query.setFilter(filter);
      query.setOrdering("partitionName ascending");
      query.declareParameters(makeParameterDeclarationString(params));
      if (max_parts >= 0) {
        // User specified a row limit, set it on the Query
        query.setRange(0, max_parts);
      }
      if (resultsCol != null && !resultsCol.isEmpty()) {
        query.setResult(resultsCol);
      }

      Collection<T> result = (Collection<T>) query.executeWithMap(params);

      return Collections.unmodifiableCollection(new ArrayList<>(result));
    }
  }


  private String getJDOFilterStrForPartitionVals(Table table, List<String> vals,
      Map params) throws MetaException {
    String partNameMatcher = MetaStoreUtils.makePartNameMatcher(table, vals, ".*");
    params.put("dbName", table.getDbName());
    params.put("catName", table.getCatName());
    params.put("tableName", table.getTableName());
    params.put("partialRegex", partNameMatcher);
    return "table.database.name == dbName" + " && table.database.catalogName == catName"
        + " && table.tableName == tableName" + " && partitionName.matches(partialRegex)";
  }

  // This code is only executed in JDO code path, not from direct SQL code path.
  private List<MPartition> listMPartitionsWithProjection(List<String> fieldNames, String jdoFilter,
      Map<String, Object> params) throws Exception {
    List<MPartition> mparts = null;
    LOG.debug("Executing listMPartitionsWithProjection");
    Query query = pm.newQuery(MPartition.class, jdoFilter);
    String parameterDeclaration = makeParameterDeclarationStringObj(params);
    query.declareParameters(parameterDeclaration);
    query.setOrdering("partitionName ascending");
    if (fieldNames == null || fieldNames.isEmpty()) {
      // full fetch of partitions
      mparts = (List<MPartition>) query.executeWithMap(params);
      pm.retrieveAll(mparts);
      pm.makeTransientAll(mparts);
      mparts = new ArrayList<>(mparts);
    } else {
      // fetch partially filled partitions using result clause
      query.setResult(Joiner.on(',').join(fieldNames));
      // if more than one fields are in the result class the return type is
      // List<Object[]>
      if (fieldNames.size() > 1) {
        List<Object[]> results = (List<Object[]>) query.executeWithMap(params);
        mparts = new ArrayList<>(results.size());
        for (Object[] row : results) {
          MPartition mpart = new MPartition();
          int i = 0;
          for (Object val : row) {
            MetaStoreServerUtils.setNestedProperty(mpart, fieldNames.get(i), val, true);
            i++;
          }
          mparts.add(mpart);
        }
      } else {
        // only one field is requested, return type is List<Object>
        List<Object> results = (List<Object>) query.executeWithMap(params);
        mparts = new ArrayList<>(results.size());
        for (Object row : results) {
          MPartition mpart = new MPartition();
          MetaStoreServerUtils.setNestedProperty(mpart, fieldNames.get(0), row, true);
          mparts.add(mpart);
        }
      }
    }
    return mparts;
  }

  @Override
  public int getNumPartitionsByFilter(TableName tableName, String filter) throws MetaException, NoSuchObjectException {
    final ExpressionTree exprTree = org.apache.commons.lang3.StringUtils.isNotEmpty(filter)
        ? PartFilterExprUtil.parseFilterTree(filter) : ExpressionTree.EMPTY_TREE;

    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());
    MTable mTable = ensureGetMTable(tableName);
    List<FieldSchema> partitionKeys = convertToFieldSchemas(mTable.getPartitionKeys());

    return new GetHelper<TableName, Integer>(this, tableName) {
      private final MetaStoreDirectSql.SqlFilterForPushdown filter = new MetaStoreDirectSql.SqlFilterForPushdown();

      @Override
      protected String describeResult() {
        return "Partition count";
      }

      @Override
      protected boolean canUseDirectSql() throws MetaException {
        return getDirectSql().generateSqlFilterForPushdown(catName, dbName, tblName, partitionKeys, exprTree, null, filter);
      }

      @Override
      protected Integer getSqlResult() throws MetaException {
        return getDirectSql().getNumPartitionsViaSqlFilter(filter);
      }
      @Override
      protected Integer getJdoResult() throws MetaException, NoSuchObjectException {
        return getNumPartitionsViaOrmFilter(catName ,dbName, tblName, exprTree, true, partitionKeys);
      }
    }.run(false);
  }

  private Integer getNumPartitionsViaOrmFilter(String catName, String dbName, String tblName, ExpressionTree tree, boolean isValidatedFilter, List<FieldSchema> partitionKeys)
      throws MetaException {
    Map<String, Object> params = new HashMap<>();
    String jdoFilter = makeQueryFilterString(catName, dbName, tblName, tree,
        params, isValidatedFilter, partitionKeys);
    if (jdoFilter == null) {
      assert !isValidatedFilter;
      return null;
    }

    Query query = pm.newQuery(
        "select count(partitionName) from org.apache.hadoop.hive.metastore.model.MPartition");
    query.setFilter(jdoFilter);
    String parameterDeclaration = makeParameterDeclarationStringObj(params);
    query.declareParameters(parameterDeclaration);
    Long result = (Long) query.executeWithMap(params);

    return result.intValue();
  }

  @Override
  public int getNumPartitionsByPs(TableName tableName, List<String> partVals)
      throws MetaException, NoSuchObjectException {

    return new GetHelper<TableName, Integer>(this, tableName) {
      @Override
      protected String describeResult() {
        return "Partition count by partial values";
      }

      @Override
      protected Integer getSqlResult() throws MetaException {
        return getDirectSql().getNumPartitionsViaSqlPs(getTable(), partVals);
      }

      @Override
      protected Integer getJdoResult()
          throws MetaException, NoSuchObjectException, InvalidObjectException {
        // size is known since it contains dbName, catName, tblName and partialRegex pattern
        Map<String, String> params = new HashMap<>(4);
        String filter = getJDOFilterStrForPartitionVals(getTable(), partVals, params);
        try (QueryWrapper query = new QueryWrapper(pm.newQuery(
            "select count(partitionName) from org.apache.hadoop.hive.metastore.model.MPartition"))) {
          query.setFilter(filter);
          query.declareParameters(makeParameterDeclarationString(params));
          Long result = (Long) query.executeWithMap(params);

          return result.intValue();
        }
      }
    }.run(true);
  }

  @Override
  public PartitionValuesResponse listPartitionValues(TableName table, List<FieldSchema> cols, boolean applyDistinct,
      String filter, boolean ascending, List<FieldSchema> order, long maxParts) throws MetaException {
    String catName = normalizeIdentifier(table.getCat());
    String dbName = normalizeIdentifier(table.getDb());
    String tableName = normalizeIdentifier(table.getTable());
    try {
      if (filter == null || filter.isEmpty()) {
        PartitionValuesResponse response = getDistinctValuesForPartitionsNoTxn(catName, dbName,
            tableName, cols, applyDistinct, maxParts);
        LOG.info("Number of records fetched: {}", response.getPartitionValues().size());
        return response;
      } else {
        PartitionValuesResponse response =
            extractPartitionNamesByFilter(catName, dbName, tableName, filter, cols, ascending, maxParts);
        if (response.getPartitionValues() != null) {
          LOG.info("Number of records fetched with filter: {}", response.getPartitionValues().size());
        }
        return response;
      }
    } catch (Exception t) {
      LOG.error("Exception in ORM", t);
      throw new MetaException("Error retrieving partition values: " + t);
    }
  }

  private PartitionValuesResponse extractPartitionNamesByFilter(
      String catName, String dbName, String tableName, String filter, List<FieldSchema> cols,
      boolean ascending, long maxParts)
      throws MetaException, NoSuchObjectException {

    LOG.info("Table: {} filter: \"{}\" cols: {}",
        TableName.getQualified(catName, dbName, tableName), filter, cols);
    List<String> partitionNames = null;
    List<Partition> partitions = null;
    Table tbl = getTable(new TableName(catName, dbName, tableName), null, -1);
    try {
      // Get partitions by name - ascending or descending
      partitionNames = getPartitionNamesByFilter(catName, dbName, tableName, filter, ascending,
          maxParts);
    } catch (MetaException e) {
      LOG.warn("Querying by partition names failed, trying out with partition objects, filter: {}", filter);
    }

    if (partitionNames == null) {
      partitions = getPartitionsByFilter(new TableName(catName, dbName, tableName),
          new GetPartitionsArgs.GetPartitionsArgsBuilder().filter(filter).max((short) maxParts).build());
    }

    if (partitions != null) {
      partitionNames = new ArrayList<>(partitions.size());
      for (Partition partition : partitions) {
        // Check for NULL's just to be safe
        if (tbl.getPartitionKeys() != null && partition.getValues() != null) {
          partitionNames.add(Warehouse.makePartName(tbl.getPartitionKeys(), partition.getValues()));
        }
      }
    }

    if (partitionNames == null) {
      throw new MetaException("Cannot obtain list of partitions by filter:\"" + filter +
          "\" for " + TableName.getQualified(catName, dbName, tableName));
    }

    if (!ascending) {
      partitionNames.sort(Collections.reverseOrder());
    }

    // Return proper response
    PartitionValuesResponse response = new PartitionValuesResponse();
    response.setPartitionValues(new ArrayList<>(partitionNames.size()));
    LOG.info("Converting responses to Partition values for items: {}", partitionNames.size());
    for (String partName : partitionNames) {
      ArrayList<String> vals = new ArrayList<>(Collections.nCopies(tbl.getPartitionKeys().size(), null));
      PartitionValuesRow row = new PartitionValuesRow();
      Warehouse.makeValsFromName(partName, vals);
      for (String value : vals) {
        row.addToRow(value);
      }
      response.addToPartitionValues(row);
    }
    return response;
  }

  private PartitionValuesResponse getDistinctValuesForPartitionsNoTxn(
      String catName, String dbName, String tableName, List<FieldSchema> cols,
      boolean applyDistinct, long maxParts)
      throws MetaException {
    Query q =  pm.newQuery("select partitionName from org.apache.hadoop.hive.metastore.model.MPartition "
        + "where table.database.name == t1 && table.database.catalogName == t2 && "
        + "table.tableName == t3 ");
    q.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");

    // TODO: Ordering seems to affect the distinctness, needs checking, disabling.
/*
      if (ascending) {
        q.setOrdering("partitionName ascending");
      } else {
        q.setOrdering("partitionName descending");
      }
*/
    if (maxParts > 0) {
      q.setRange(0, maxParts);
    }
    StringBuilder partValuesSelect = new StringBuilder(256);
    if (applyDistinct) {
      partValuesSelect.append("DISTINCT ");
    }
    List<FieldSchema> partitionKeys =
        getTable(new TableName(catName, dbName, tableName), null, -1).getPartitionKeys();
    for (FieldSchema key : cols) {
      partValuesSelect.append(extractPartitionKey(key, partitionKeys)).append(", ");
    }
    partValuesSelect.setLength(partValuesSelect.length() - 2);
    LOG.info("Columns to be selected from Partitions: {}", partValuesSelect);
    q.setResult(partValuesSelect.toString());

    PartitionValuesResponse response = new PartitionValuesResponse();
    response.setPartitionValues(new ArrayList<>());
    if (cols.size() > 1) {
      List<Object[]> results = (List<Object[]>) q.execute(dbName, catName, tableName);
      for (Object[] row : results) {
        PartitionValuesRow rowResponse = new PartitionValuesRow();
        for (Object columnValue : row) {
          rowResponse.addToRow((String) columnValue);
        }
        response.addToPartitionValues(rowResponse);
      }
    } else {
      List<Object> results = (List<Object>) q.execute(dbName, catName, tableName);
      for (Object row : results) {
        PartitionValuesRow rowResponse = new PartitionValuesRow();
        rowResponse.addToRow((String) row);
        response.addToPartitionValues(rowResponse);
      }
    }
    return response;
  }

  private String extractPartitionKey(FieldSchema key, List<FieldSchema> pkeys) {
    StringBuilder buffer = new StringBuilder(256);

    assert pkeys.size() >= 1;

    String partKey = "/" + key.getName() + "=";

    // Table is partitioned by single key
    if (pkeys.size() == 1 && (pkeys.get(0).getName().matches(key.getName()))) {
      buffer.append("partitionName.substring(partitionName.indexOf(\"")
          .append(key.getName()).append("=\") + ").append(key.getName().length() + 1)
          .append(")");

      // First partition key - anything between key= and first /
    } else if ((pkeys.get(0).getName().matches(key.getName()))) {

      buffer.append("partitionName.substring(partitionName.indexOf(\"")
          .append(key.getName()).append("=\") + ").append(key.getName().length() + 1).append(", ")
          .append("partitionName.indexOf(\"/\")")
          .append(")");

      // Last partition key - anything between /key= and end
    } else if ((pkeys.get(pkeys.size() - 1).getName().matches(key.getName()))) {
      buffer.append("partitionName.substring(partitionName.indexOf(\"")
          .append(partKey).append("\") + ").append(partKey.length())
          .append(")");

      // Intermediate key - anything between /key= and the following /
    } else {

      buffer.append("partitionName.substring(partitionName.indexOf(\"")
          .append(partKey).append("\") + ").append(partKey.length()).append(", ")
          .append("partitionName.indexOf(\"/\", partitionName.indexOf(\"").append(partKey)
          .append("\") + 1))");
    }
    LOG.info("Query for Key:" + key.getName() + " is :" + buffer);
    return buffer.toString();
  }

  private List<String> getPartitionNamesByFilter(String catName, String dbName, String tableName,
      String filter, boolean ascending, long maxParts)
      throws MetaException {
    List<String> partNames = new ArrayList<>();
    Query query = null;
    LOG.debug("Executing getPartitionNamesByFilter");
    catName = normalizeIdentifier(catName);
    dbName = dbName.toLowerCase();
    tableName = tableName.toLowerCase();

    MTable mtable = getMTable(catName, dbName, tableName);
    if( mtable == null ) {
      // To be consistent with the behavior of listPartitionNames, if the
      // table or db does not exist, we return an empty list
      return partNames;
    }
    Map<String, Object> params = new HashMap<>();
    String queryFilterString = makeQueryFilterString(catName, dbName, mtable, filter, params);
    query = pm.newQuery(
        "select partitionName from org.apache.hadoop.hive.metastore.model.MPartition "
            + "where " + queryFilterString);

    if (maxParts >= 0) {
      //User specified a row limit, set it on the Query
      query.setRange(0, maxParts);
    }

    LOG.debug("Filter specified is {}, JDOQL filter is {}", filter,
        queryFilterString);

    LOG.debug("Parms is {}", params);

    String parameterDeclaration = makeParameterDeclarationStringObj(params);
    query.declareParameters(parameterDeclaration);
    if (ascending) {
      query.setOrdering("partitionName ascending");
    } else {
      query.setOrdering("partitionName descending");
    }
    query.setResult("partitionName");

    Collection<String> names = (Collection<String>) query.executeWithMap(params);
    partNames = new ArrayList<>(names);

    LOG.debug("Done executing query for getPartitionNamesByFilter");
    return partNames;
  }

  @Override
  public List<String> listPartitionNamesPs(TableName tableName, List<String> partVals, short maxParts)
      throws MetaException, NoSuchObjectException {
    LOG.debug("Executing listPartitionNamesPs");
    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());
    Collection<String> names = getPartitionPsQueryResults(catName, dbName, tblName,
        partVals, maxParts, "partitionName");
    return new ArrayList<>(names);
  }

  @Override
  public Partition getPartitionWithAuth(TableName tableName, List<String> partVals, String user_name,
      List<String> group_names) throws MetaException, NoSuchObjectException, InvalidObjectException {
    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());
    MPartition mpart = getMPartition(catName, dbName, tblName, partVals, null);
    if (mpart == null) {
      throw new NoSuchObjectException("partition values="
          + partVals.toString());
    }
    MTable mtbl = mpart.getTable();

    Partition part = convertToPart(catName, dbName, tblName, mpart, TxnUtils.isAcidTable(mtbl.getParameters()), conf);
    if ("TRUE".equalsIgnoreCase(mtbl.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
      String partName = Warehouse.makePartName(convertToFieldSchemas(mtbl
          .getPartitionKeys()), partVals);
      PrincipalPrivilegeSet partAuth = baseStore.getPartitionPrivilegeSet(catName, dbName,
          tblName, partName, user_name, group_names);
      part.setPrivileges(partAuth);
    }
    return part;
  }

  /**
   * Makes a JDO query filter string.
   * Makes a JDO query filter string for tables or partitions.
   * @param dbName Database name.
   * @param mtable Table. If null, the query returned is over tables in a database.
   *   If not null, the query returned is over partitions in a table.
   * @param filter The filter from which JDOQL filter will be made.
   * @param params Parameters for the filter. Some parameters may be added here.
   * @return Resulting filter.
   */
  private String makeQueryFilterString(String catName, String dbName, MTable mtable, String filter,
      Map<String, Object> params) throws MetaException {
    ExpressionTree tree = (filter != null && !filter.isEmpty())
        ? PartFilterExprUtil.parseFilterTree(filter) : ExpressionTree.EMPTY_TREE;
    return makeQueryFilterString(catName, dbName, convertToTable(mtable, baseStore.getConf()), tree, params, true);
  }

  @Override
  public void updateCreationMetadata(TableName tableName, CreationMetadata cm) throws MetaException {
    // Update creation metadata
    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String name = normalizeIdentifier(tableName.getTable());
    MCreationMetadata newMcm = convertToMCreationMetadata(cm, baseStore);
    MCreationMetadata mcm = getCreationMetadata(catName, dbName, name);
    mcm.setTables(newMcm.getTables());
    mcm.setMaterializationTime(newMcm.getMaterializationTime());
    mcm.setTxnList(newMcm.getTxnList());
    // commit the changes
    cm.setMaterializationTime(newMcm.getMaterializationTime());
  }

  @Override
  public List<Table> getAllMaterializedViewObjectsForRewriting(String catName) throws MetaException {
    List<Table> allMaterializedViews = new ArrayList<>();
    Query query = null;
    catName = normalizeIdentifier(catName);
    query = pm.newQuery(MTable.class);
    query.setFilter("database.catalogName == catName && tableType == tt && rewriteEnabled == re");
    query.declareParameters("java.lang.String catName, java.lang.String tt, boolean re");
    Collection<MTable> mTbls = (Collection<MTable>) query.executeWithArray(
        catName, TableType.MATERIALIZED_VIEW.toString(), true);
    for (MTable mTbl : mTbls) {
      Table tbl = convertToTable(mTbl, conf);
      tbl.setCreationMetadata(
          convertToCreationMetadata(
              getCreationMetadata(tbl.getCatName(), tbl.getDbName(), tbl.getTableName()), baseStore));
      allMaterializedViews.add(tbl);
    }
    return allMaterializedViews;
  }

  @Override
  public List<String> isPartOfMaterializedView(TableName tableName) {
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());
    List<String> mViewList = new ArrayList<>();
    Query query = pm.newQuery("select from org.apache.hadoop.hive.metastore.model.MCreationMetadata");
    List<MCreationMetadata> creationMetadata = (List<MCreationMetadata>) query.execute();
    Iterator<MCreationMetadata> iter = creationMetadata.iterator();

    while (iter.hasNext()) {
      MCreationMetadata p = iter.next();
      Set<MMVSource> tables = p.getTables();
      for (MMVSource sourceTable : tables) {
        MTable table = sourceTable.getTable();
        if (dbName.equals(table.getDatabase().getName()) && tblName.equals(table.getTableName())) {
          LOG.info("Cannot drop table {} as it is being used by MView {}", table.getTableName(), p.getTblName());
          mViewList.add(p.getDbName() + "." + p.getTblName());
        }
      }
    }
    return mViewList;
  }

  @Override
  public Table markPartitionForEvent(TableName tableName, Map<String, String> partVals, PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
    LOG.debug("Begin executing markPartitionForEvent");
    Table tbl = getTable(tableName, null, -1);
    if(null == tbl) {
      throw new UnknownTableException("Table: "+ tableName + " is not found.");
    }

    pm.makePersistent(new MPartitionEvent(normalizeIdentifier(tableName.getCat()),
        normalizeIdentifier(tableName.getDb()), normalizeIdentifier(tableName.getTable()),
        getPartitionStr(tbl, partVals), evtType.getValue()));
    LOG.debug("Done executing markPartitionForEvent");
    return tbl;
  }

  private String getPartitionStr(Table tbl, Map<String,String> partVals) throws InvalidPartitionException{
    if(tbl.getPartitionKeysSize() != partVals.size()){
      throw new InvalidPartitionException("Number of partition columns in table: "+ tbl.getPartitionKeysSize() +
          " doesn't match with number of supplied partition values: "+ partVals.size());
    }
    final List<String> storedVals = new ArrayList<>(tbl.getPartitionKeysSize());
    for(FieldSchema partKey : tbl.getPartitionKeys()){
      String partVal = partVals.get(partKey.getName());
      if(null == partVal) {
        throw new InvalidPartitionException("No value found for partition column: "+partKey.getName());
      }
      storedVals.add(partVal);
    }
    return join(storedVals,',');
  }

  @Override
  public boolean isPartitionMarkedForEvent(TableName tableName, Map<String, String> partName,
      PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
    LOG.debug("Begin Executing isPartitionMarkedForEvent");
    Query query = pm.newQuery(MPartitionEvent.class,
        "dbName == t1 && tblName == t2 && partName == t3 && eventType == t4 && catalogName == t5");
    query
        .declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, int t4," +
            "java.lang.String t5");
    Table tbl = getTable(tableName, null, -1); // Make sure dbName and tblName are valid.
    if (null == tbl) {
      throw new UnknownTableException("Table: " + tableName + " is not found.");
    }
    Collection<MPartitionEvent> partEvents = (Collection<MPartitionEvent>) query.executeWithArray(
        normalizeIdentifier(tableName.getDb()), normalizeIdentifier(tableName.getTable()),
        getPartitionStr(tbl, partName), evtType.getValue(), normalizeIdentifier(tableName.getCat()));
    pm.retrieveAll(partEvents);
    LOG.debug("Done executing isPartitionMarkedForEvent");
    return partEvents != null && !partEvents.isEmpty();
  }

  @Override
  public int getObjectCount(String fieldName, String objName) {
    String queryStr = "select count(" + fieldName + ") from " + objName;
    Query query = pm.newQuery(queryStr);
    Long result = (Long) query.execute();
    return result != null ? result.intValue() : 0;
  }

  @Override
  public long updateParameterWithExpectedValue(Table table, String key, String expectedValue, String newValue)
      throws MetaException, NoSuchObjectException {
    return new GetHelper<TableName, Long>(this, new TableName(table.getCatName(), table.getDbName(), table.getCatName())) {
      @Override
      protected String describeResult() {
        return "Affected rows";
      }
      @Override
      protected Long getSqlResult() throws MetaException {
        return getDirectSql().updateTableParam(table, key, expectedValue, newValue);
      }
      @Override
      protected Long getJdoResult()
          throws MetaException, NoSuchObjectException, InvalidObjectException {
        throw new UnsupportedOperationException(
            "Cannot update parameter with JDO, make sure direct SQL is enabled");
      }
      @Override
      protected boolean canUseJdoQuery() throws MetaException {
        return false;
      }
    }.run(false);
  }

  @Override
  public MPartition ensureGetMPartition(TableName tableName, List<String> partVals) throws MetaException {
    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());
    MPartition result = null;
    MTable mtbl = getMTable(catName, dbName, tblName);
    if (mtbl == null) {
      // throw exception?
      return null;
    }
    // Change the query to use part_vals instead of the name which is
    // redundant TODO: callers of this often get part_vals out of name for no reason...
    String name =
        Warehouse.makePartName(convertToFieldSchemas(mtbl.getPartitionKeys()), partVals);
    result = getMPartition(catName, dbName, tblName, name);

    return result;
  }

  /**
   * Getting MPartition object. Use this method if the partition name is available, so we do not
   * query the table object again.
   * @param catName The catalogue
   * @param dbName The database
   * @param tableName The table
   * @param name The partition name
   * @return The MPartition object in the backend database
   */
  private MPartition getMPartition(String catName, String dbName, String tableName,
      String name) throws MetaException {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tableName = normalizeIdentifier(tableName);
    MPartition ret = null;
    Query query =
        pm.newQuery(MPartition.class,
            "table.tableName == t1 && table.database.name == t2 && partitionName == t3 " +
                " && table.database.catalogName == t4");
    query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, "
        + "java.lang.String t4");
    List<MPartition> mparts = (List<MPartition>) query.executeWithArray(tableName, dbName, name, catName);
    pm.retrieveAll(mparts);
    // We need to compare partition name with requested name since some DBs
    // (like MySQL, Derby) considers 'a' = 'a ' whereas others like (Postgres,
    // Oracle) doesn't exhibit this problem.
    if (CollectionUtils.isNotEmpty(mparts)) {
      if (mparts.size() > 1) {
        throw new MetaException(
            "Expecting only one partition but more than one partitions are found.");
      } else {
        MPartition mpart = mparts.get(0);
        if (name.equals(mpart.getPartitionName())) {
          ret = mpart;
        } else {
          throw new MetaException("Expecting a partition with name " + name
              + ", but metastore is returning a partition with name " + mpart.getPartitionName()
              + ".");
        }
      }
    }
    return ret;
  }
}
