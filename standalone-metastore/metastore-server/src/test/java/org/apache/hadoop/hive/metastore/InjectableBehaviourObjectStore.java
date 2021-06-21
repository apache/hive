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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;

import static org.junit.Assert.assertEquals;

/**
 * A wrapper around {@link ObjectStore} that allows us to inject custom behaviour
 * on to some of the methods for testing.
 */
public class InjectableBehaviourObjectStore extends ObjectStore {
  public InjectableBehaviourObjectStore() {
    super();
  }

  /**
   * A utility class that allows people injecting behaviour to determine if their injections occurred.
   */
  public static abstract class BehaviourInjection<T, F>
      implements com.google.common.base.Function<T, F>{
    protected boolean injectionPathCalled = false;
    protected boolean nonInjectedPathCalled = false;

    public void assertInjectionsPerformed(
        boolean expectedInjectionCalled, boolean expectedNonInjectedPathCalled){
      assertEquals(expectedInjectionCalled, injectionPathCalled);
      assertEquals(expectedNonInjectedPathCalled, nonInjectedPathCalled);
    }
  }

  /**
   * A utility class to pass the arguments of the caller to the stub method.
   */
  public class CallerArguments {
    public String dbName;
    public String tblName;
    public String funcName;
    public String constraintTblName;

    public CallerArguments(String dbName) {
      this.dbName = dbName;
    }
  }

  private static com.google.common.base.Function<Table, Table> getTableModifier =
      com.google.common.base.Functions.identity();
  private static com.google.common.base.Function<Partition, Partition> getPartitionModifier =
          com.google.common.base.Functions.identity();
  private static com.google.common.base.Function<List<String>, List<String>> listPartitionNamesModifier =
          com.google.common.base.Functions.identity();
  private static com.google.common.base.Function<NotificationEventResponse, NotificationEventResponse>
          getNextNotificationModifier = com.google.common.base.Functions.identity();

  private static com.google.common.base.Function<CallerArguments, Boolean> callerVerifier = null;

  private static com.google.common.base.Function<NotificationEvent, Boolean> addNotificationEventModifier = null;

  private static com.google.common.base.Function<CallerArguments, Boolean> alterTableModifier = null;

  private static com.google.common.base.Function<CurrentNotificationEventId, CurrentNotificationEventId>
          getCurrNotiEventIdModifier = null;

  private static com.google.common.base.Function<List<Partition>, Boolean> alterPartitionsModifier = null;

  private static com.google.common.base.Function<List<Partition>, Boolean> addPartitionsModifier = null;

  private static com.google.common.base.Function<Table, Boolean> updatePartColStatsModifier = null;

  // Methods to set/reset getTable modifier
  public static void setGetTableBehaviour(com.google.common.base.Function<Table, Table> modifier){
    getTableModifier = (modifier == null) ? com.google.common.base.Functions.identity() : modifier;
  }

  public static void resetGetTableBehaviour(){
    setGetTableBehaviour(null);
  }

  // Methods to set/reset getPartition modifier
  public static void setGetPartitionBehaviour(com.google.common.base.Function<Partition, Partition> modifier){
    getPartitionModifier = (modifier == null) ? com.google.common.base.Functions.identity() : modifier;
  }

  public static void resetGetPartitionBehaviour(){
    setGetPartitionBehaviour(null);
  }

  // Methods to set/reset listPartitionNames modifier
  public static void setListPartitionNamesBehaviour(com.google.common.base.Function<List<String>, List<String>> modifier){
    listPartitionNamesModifier = (modifier == null)? com.google.common.base.Functions.identity() : modifier;
  }

  public static void resetListPartitionNamesBehaviour(){
    setListPartitionNamesBehaviour(null);
  }

  // Methods to set/reset getNextNotification modifier
  public static void setGetNextNotificationBehaviour(
          com.google.common.base.Function<NotificationEventResponse,NotificationEventResponse> modifier){
    getNextNotificationModifier = (modifier == null)? com.google.common.base.Functions.identity() : modifier;
  }

  public static void setAddNotificationModifier(com.google.common.base.Function<NotificationEvent, Boolean> modifier) {
    addNotificationEventModifier = modifier;
  }

  public static void resetAddNotificationModifier() {
    setAddNotificationModifier(null);
  }

  public static void resetGetNextNotificationBehaviour(){
    setGetNextNotificationBehaviour(null);
  }

  // Methods to set/reset caller checker
  public static void setCallerVerifier(com.google.common.base.Function<CallerArguments, Boolean> verifier){
    callerVerifier = verifier;
  }

  public static void resetCallerVerifier(){
    setCallerVerifier(null);
  }

  public static void setAlterTableModifier(com.google.common.base.Function<CallerArguments, Boolean> modifier) {
    alterTableModifier = modifier;
  }
  public static void resetAlterTableModifier() {
    setAlterTableModifier(null);
  }

  public static void resetAddPartitionModifier() {
    setAddPartitionsBehaviour(null);
  }

  public static void setAlterPartitionsBehaviour(com.google.common.base.Function<List<Partition>, Boolean> modifier){
    alterPartitionsModifier = modifier;
  }

  public static void setAddPartitionsBehaviour(com.google.common.base.Function<List<Partition>, Boolean> modifier){
    addPartitionsModifier = modifier;
  }

  public static void setUpdatePartColStatsBehaviour(com.google.common.base.Function<Table, Boolean> modifier){
    updatePartColStatsModifier = modifier;
  }

  // ObjectStore methods to be overridden with injected behavior
  @Override
  public Table getTable(String catName, String dbName, String tableName) throws MetaException {
    return getTableModifier.apply(super.getTable(catName, dbName, tableName));
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName, String writeIdList) throws MetaException {
    return getTableModifier.apply(super.getTable(catName, dbName, tableName, writeIdList));
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tableName,
                                List<String> partVals) throws NoSuchObjectException, MetaException {
    return getPartitionModifier.apply(super.getPartition(catName, dbName, tableName, partVals));
  }

  @Override
  public List<String> listPartitionNames(String catName, String dbName, String tableName, short max)
          throws MetaException {
    return listPartitionNamesModifier.apply(super.listPartitionNames(catName, dbName, tableName, max));
  }

  @Override
  public NotificationEventResponse getNextNotification(NotificationEventRequest rqst) {
    return getNextNotificationModifier.apply(super.getNextNotification(rqst));
  }

  @Override
  public Table alterTable(String catName, String dbname, String name, Table newTable, String queryValidWriteIds)
          throws InvalidObjectException, MetaException {
    if (alterTableModifier != null) {
      CallerArguments args = new CallerArguments(dbname);
      args.tblName = name;
      Boolean success = alterTableModifier.apply(args);
      if ((success != null) && !success) {
        throw new MetaException("InjectableBehaviourObjectStore: Invalid alterTable operation on Catalog : " + catName +
                " DB: " + dbname + " table: " + name);
      }
    }
    return super.alterTable(catName, dbname, name, newTable, queryValidWriteIds);
  }

  @Override
  public void addNotificationEvent(NotificationEvent entry) throws MetaException {
    if (addNotificationEventModifier != null) {
      Boolean success = addNotificationEventModifier.apply(entry);
      if ((success != null) && !success) {
        throw new MetaException("InjectableBehaviourObjectStore: Invalid addNotificationEvent operation on DB: "
                + entry.getDbName() + " table: " + entry.getTableName() + " event : " + entry.getEventType());
      }
    }
    super.addNotificationEvent(entry);
  }

  @Override
  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    if (callerVerifier != null) {
      CallerArguments args = new CallerArguments(tbl.getDbName());
      args.tblName = tbl.getTableName();
      Boolean success = callerVerifier.apply(args);
      if ((success != null) && !success) {
        throw new MetaException("InjectableBehaviourObjectStore: Invalid Create Table operation on DB: "
                + args.dbName + " table: " + args.tblName);
      }
    }
    super.createTable(tbl);
  }

  @Override
  public void createFunction(Function func) throws InvalidObjectException, MetaException {
    if (callerVerifier != null) {
      CallerArguments args = new CallerArguments(func.getDbName());
      args.funcName = func.getFunctionName();
      Boolean success = callerVerifier.apply(args);
      if ((success != null) && !success) {
        throw new MetaException("InjectableBehaviourObjectStore: Invalid Create Function operation on DB: "
                + args.dbName + " function: " + args.funcName);
      }
    }
    super.createFunction(func);
  }

  @Override
  public List<SQLPrimaryKey> addPrimaryKeys(List<SQLPrimaryKey> pks) throws InvalidObjectException,
          MetaException {
    if (callerVerifier != null) {
      CallerArguments args = new CallerArguments(pks.get(0).getTable_db());
      args.constraintTblName = pks.get(0).getTable_name();
      Boolean success = callerVerifier.apply(args);
      if ((success != null) && !success) {
        throw new MetaException("InjectableBehaviourObjectStore: Invalid Add Primary Key operation on DB: "
                + args.dbName + " table: " + args.constraintTblName);
      }
    }
    return super.addPrimaryKeys(pks);
  }

  @Override
  public List<SQLForeignKey> addForeignKeys(List<SQLForeignKey> fks) throws InvalidObjectException,
          MetaException {
    if (callerVerifier != null) {
      CallerArguments args = new CallerArguments(fks.get(0).getFktable_db());
      args.constraintTblName = fks.get(0).getFktable_name();
      Boolean success = callerVerifier.apply(args);
      if ((success != null) && !success) {
        throw new MetaException("InjectableBehaviourObjectStore: Invalid Add Foreign Key operation on DB: "
                + args.dbName + " table: " + args.constraintTblName);
      }
    }
    return super.addForeignKeys(fks);
  }

  @Override
  public boolean alterDatabase(String catalogName, String dbname, Database db)
          throws NoSuchObjectException, MetaException {
    if (callerVerifier != null) {
      CallerArguments args = new CallerArguments(dbname);
      callerVerifier.apply(args);
    }
    return super.alterDatabase(catalogName, dbname, db);
  }

  // Methods to set/reset getCurrentNotificationEventId modifier
  public static void setGetCurrentNotificationEventIdBehaviour(
          com.google.common.base.Function<CurrentNotificationEventId, CurrentNotificationEventId> modifier){
    getCurrNotiEventIdModifier = modifier;
  }
  public static void resetGetCurrentNotificationEventIdBehaviour(){
    setGetCurrentNotificationEventIdBehaviour(null);
  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() {
    CurrentNotificationEventId id = super.getCurrentNotificationEventId();
    if (getCurrNotiEventIdModifier != null) {
      id = getCurrNotiEventIdModifier.apply(id);
      if (id == null) {
        throw new RuntimeException("InjectableBehaviourObjectStore: Invalid getCurrentNotificationEventId");
      }
    }
    return id;
  }

  @Override
  public List<Partition> alterPartitions(String catName, String dbname, String name,
                                  List<List<String>> part_vals, List<Partition> newParts,
                                  long writeId, String queryWriteIdList)
          throws InvalidObjectException, MetaException {
    if (alterPartitionsModifier != null) {
      Boolean success = alterPartitionsModifier.apply(newParts);
      if ((success != null) && !success) {
        throw new MetaException("InjectableBehaviourObjectStore: Invalid alterPartitions operation on Catalog : "
                + catName + " DB: " + dbname + " table: " + name);
      }
    }
    return super.alterPartitions(catName, dbname, name, part_vals, newParts, writeId, queryWriteIdList);
  }

  @Override
  public boolean addPartitions(String catName, String dbName, String tblName, List<Partition> parts)
    throws InvalidObjectException, MetaException {
    if (addPartitionsModifier != null) {
      Boolean success = addPartitionsModifier.apply(parts);
      if ((success != null) && !success) {
        throw new MetaException("InjectableBehaviourObjectStore: Invalid addPartitions operation on Catalog : "
          + catName + " DB: " + dbName + " table: " + tblName);
      }
    }
    return super.addPartitions(catName, dbName, tblName, parts);
  }

  @Override
  public Map<String, Map<String, String>> updatePartitionColumnStatisticsInBatch(
          Map<String, ColumnStatistics> partColStatsMap,
          Table tbl, List<TransactionalMetaStoreEventListener> listeners,
          String validWriteIds, long writeId)
          throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    if (updatePartColStatsModifier != null) {
      Boolean success = updatePartColStatsModifier.apply(tbl);
      if ((success != null) && !success) {
        throw new MetaException("InjectableBehaviourObjectStore: Invalid updatePartitionColumnStatisticsInBatch  : "
          + "operation on Catalog : " + tbl.getCatName() + " DB: " + tbl.getDbName() + " table: " + tbl.getTableName());
      }
    }
    return super.updatePartitionColumnStatisticsInBatch(partColStatsMap, tbl, listeners, validWriteIds, writeId);
  }
}
