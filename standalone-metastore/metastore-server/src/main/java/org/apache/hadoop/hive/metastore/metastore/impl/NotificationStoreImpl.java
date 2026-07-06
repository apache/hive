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

import com.google.common.collect.Lists;

import javax.jdo.Query;
import javax.jdo.datastore.JDOConnection;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.directsql.MetaStoreDirectSql;
import org.apache.hadoop.hive.metastore.PersistenceManagerProvider;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metastore.RawStoreAware;
import org.apache.hadoop.hive.metastore.model.MNotificationLog;
import org.apache.hadoop.hive.metastore.model.MNotificationNextId;
import org.apache.hadoop.hive.metastore.model.MTxnWriteNotificationLog;
import org.apache.hadoop.hive.metastore.metastore.iface.NotificationStore;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.utils.RetryingExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.ObjectStore.appendSimpleCondition;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

public class NotificationStoreImpl extends RawStoreAware implements NotificationStore {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationStoreImpl.class);
  private Configuration conf;
  private SQLGenerator sqlGenerator;
  private MetaStoreDirectSql directSql;

  @Override
  public void setBaseStore(RawStore store) {
    super.setBaseStore(store);
    this.conf = baseStore.getConf();
    DatabaseProduct dbType = PersistenceManagerProvider.getDatabaseProduct();
    this.sqlGenerator = new SQLGenerator(dbType, conf);
  }

  @Override
  public NotificationEventResponse getNextNotification(NotificationEventRequest rqst) {
    NotificationEventResponse result = new NotificationEventResponse();
    result.setEvents(new ArrayList<>());
    long lastEvent = rqst.getLastEvent();
    List<Object> parameterVals = new ArrayList<>();
    parameterVals.add(lastEvent);
    // filterBuilder parameter is used for construction of conditional clause in the select query
    StringBuilder filterBuilder = new StringBuilder("eventId > para" + parameterVals.size());
    // parameterBuilder parameter is used for specify what types of parameters will go into the filterBuilder
    StringBuilder parameterBuilder = new StringBuilder("java.lang.Long para" + parameterVals.size());
      /* A fully constructed query would like:
      ->  filterBuilder: eventId > para0 && catalogName == para1 && dbName == para2 && (tableName == para3
          || tableName == para4) && eventType != para5
      ->  parameterBuilder: java.lang.Long para0, java.lang.String para1, java.lang.String para2
          , java.lang.String para3, java.lang.String para4, java.lang.String para5
       */
    if (rqst.isSetCatName()) {
      parameterVals.add(normalizeIdentifier(rqst.getCatName()));
      parameterBuilder.append(", java.lang.String para" + parameterVals.size());
      filterBuilder.append(" && catalogName == para" + parameterVals.size());
    }
    if (rqst.isSetDbName()) {
      parameterVals.add(normalizeIdentifier(rqst.getDbName()));
      parameterBuilder.append(", java.lang.String para" + parameterVals.size());
      filterBuilder.append(" && dbName == para" + parameterVals.size());
    }
    if (rqst.isSetTableNames() && !rqst.getTableNames().isEmpty()) {
      filterBuilder.append(" && (");
      for (String tableName : rqst.getTableNames()) {
        parameterVals.add(normalizeIdentifier(tableName));
        parameterBuilder.append(", java.lang.String para" + parameterVals.size());
        filterBuilder.append("tableName == para" + parameterVals.size()+ " || ");
      }
      filterBuilder.setLength(filterBuilder.length() - 4); // remove the last " || "
      filterBuilder.append(") ");
    }
    if (rqst.isSetEventTypeList()) {
      filterBuilder.append(" && (");
      for (String eventType : rqst.getEventTypeList()) {
        parameterVals.add(eventType);
        parameterBuilder.append(", java.lang.String para" + parameterVals.size());
        filterBuilder.append("eventType == para" + parameterVals.size() + " || ");
      }
      filterBuilder.setLength(filterBuilder.length() - 4); // remove the last " || "
      filterBuilder.append(") ");
    }
    if (rqst.isSetEventTypeSkipList()) {
      for (String eventType : rqst.getEventTypeSkipList()) {
        parameterVals.add(eventType);
        parameterBuilder.append(", java.lang.String para" + parameterVals.size());
        filterBuilder.append(" && eventType != para" + parameterVals.size());
      }
    }
    Query query = pm.newQuery(MNotificationLog.class, filterBuilder.toString());
    query.declareParameters(parameterBuilder.toString());
    query.setOrdering("eventId ascending");
    int maxEventResponse = MetastoreConf.getIntVar(baseStore.getConf(), MetastoreConf.ConfVars.METASTORE_MAX_EVENT_RESPONSE);
    int maxEvents = (rqst.getMaxEvents() < maxEventResponse && rqst.getMaxEvents() > 0) ? rqst.getMaxEvents() : maxEventResponse;
    query.setRange(0, maxEvents);
    Collection<MNotificationLog> events =
        (Collection) query.executeWithArray(parameterVals.toArray(new Object[0]));
    if (events == null) {
      return result;
    }
    Iterator<MNotificationLog> i = events.iterator();
    while (i.hasNext()) {
      result.addToEvents(translateDbToThrift(i.next()));
    }
    return result;
  }

  private NotificationEvent translateDbToThrift(MNotificationLog dbEvent) {
    NotificationEvent event = new NotificationEvent();
    event.setEventId(dbEvent.getEventId());
    event.setEventTime(dbEvent.getEventTime());
    event.setEventType(dbEvent.getEventType());
    event.setCatName(dbEvent.getCatalogName());
    event.setDbName(dbEvent.getDbName());
    event.setTableName(dbEvent.getTableName());
    event.setMessage((dbEvent.getMessage()));
    event.setMessageFormat(dbEvent.getMessageFormat());
    return event;
  }

  private void lockNotificationSequenceForUpdate() throws MetaException {
    int maxRetries =
        MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.NOTIFICATION_SEQUENCE_LOCK_MAX_RETRIES);
    long sleepInterval = MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.NOTIFICATION_SEQUENCE_LOCK_RETRY_SLEEP_INTERVAL, TimeUnit.MILLISECONDS);
    if (sqlGenerator.getDbProduct().isDERBY()) {
      // Derby doesn't allow FOR UPDATE to lock the row being selected (See https://db.apache
      // .org/derby/docs/10.1/ref/rrefsqlj31783.html) . So lock the whole table. Since there's
      // only one row in the table, this shouldn't cause any performance degradation.
      new RetryingExecutor<Void>(maxRetries, () -> {
        if (directSql == null) {
          directSql = new MetaStoreDirectSql(pm, conf, "");
        }
        directSql.lockDbTable("NOTIFICATION_SEQUENCE");
        return null;
      }).commandName("lockNotificationSequenceForUpdate").sleepInterval(sleepInterval).run();
    } else {
      String selectQuery = "select \"NEXT_EVENT_ID\" from \"NOTIFICATION_SEQUENCE\"";
      String lockingQuery = sqlGenerator.addForUpdateClause(selectQuery);
      new RetryingExecutor<Void>(maxRetries, () -> {
        String s = sqlGenerator.getDbProduct().getPrepareTxnStmt();
        assert pm.currentTransaction().isActive();
        JDOConnection jdoConn = pm.getDataStoreConnection();
        Connection conn = (Connection) jdoConn.getNativeConnection();
        try (Statement statement = conn.createStatement()) {
          if (s != null) {
            statement.execute(s);
          }
          statement.execute(lockingQuery);
        } finally {
          jdoConn.close();
        }
        return null;
      }).commandName("lockNotificationSequenceForUpdate").sleepInterval(sleepInterval).run();
    }
  }

  @Override
  public void addNotificationEvent(NotificationEvent entry) throws MetaException {
    pm.flush();
    lockNotificationSequenceForUpdate();
    Query query = pm.newQuery(MNotificationNextId.class);
    Collection<MNotificationNextId> ids = (Collection) query.execute();
    MNotificationNextId mNotificationNextId = null;
    boolean needToPersistId;
    if (CollectionUtils.isEmpty(ids)) {
      mNotificationNextId = new MNotificationNextId(1L);
      needToPersistId = true;
    } else {
      mNotificationNextId = ids.iterator().next();
      needToPersistId = false;
    }
    entry.setEventId(mNotificationNextId.getNextEventId());
    mNotificationNextId.incrementEventId();
    if (needToPersistId) {
      pm.makePersistent(mNotificationNextId);
    }
    pm.makePersistent(translateThriftToDb(entry));
  }

  private MNotificationLog translateThriftToDb(NotificationEvent entry) {
    MNotificationLog dbEntry = new MNotificationLog();
    dbEntry.setEventId(entry.getEventId());
    dbEntry.setEventTime(entry.getEventTime());
    dbEntry.setEventType(entry.getEventType());
    dbEntry.setCatalogName(entry.isSetCatName() ? entry.getCatName() : getDefaultCatalog(baseStore.getConf()));
    dbEntry.setDbName(entry.getDbName());
    dbEntry.setTableName(entry.getTableName());
    dbEntry.setMessage(entry.getMessage());
    dbEntry.setMessageFormat(entry.getMessageFormat());
    return dbEntry;
  }

  @Override
  public void cleanNotificationEvents(int olderThan) {
    cleanOlderEvents(olderThan, MNotificationLog.class, "NotificationLog");
  }

  private void cleanOlderEvents(int olderThan, Class table, String tableName) {
    final int eventBatchSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.EVENT_CLEAN_MAX_EVENTS);
    final long ageSec = olderThan;
    final Instant now = Instant.now();
    final int tooOld = Math.toIntExact(now.getEpochSecond() - ageSec);
    final Optional<Integer> batchSize = (eventBatchSize > 0) ? Optional.of(eventBatchSize) : Optional.empty();

    final long start = System.nanoTime();
    int deleteCount = doCleanNotificationEvents(tooOld, batchSize, table, tableName);

    if (deleteCount == 0) {
      LOG.info("No {} events found to be cleaned with eventTime < {}", tableName, tooOld);
    } else {
      int batchCount = 0;
      do {
        batchCount = doCleanNotificationEvents(tooOld, batchSize, table, tableName);
        deleteCount += batchCount;
      } while (batchCount > 0);
    }

    final long finish = System.nanoTime();
    LOG.info("Deleted {} {} events older than epoch:{} in {}ms", deleteCount, tableName, tooOld,
        TimeUnit.NANOSECONDS.toMillis(finish - start));
  }

  private <T> int doCleanNotificationEvents(final int ageSec, final Optional<Integer> batchSize, Class<T> tableClass, String tableName) {
    int eventsCount = 0;
    Query query = pm.newQuery(tableClass, "eventTime <= tooOld");
    String key = null;
    query.declareParameters("java.lang.Integer tooOld");
    if (MNotificationLog.class.equals(tableClass)) {
      key = "eventId";
    } else if (MTxnWriteNotificationLog.class.equals(tableClass)) {
      key = "txnId";
    }
    query.setOrdering(key + " ascending");
    if (batchSize.isPresent()) {
      query.setRange(0, batchSize.get());
    }

    List<T> events = (List) query.execute(ageSec);
    if (CollectionUtils.isNotEmpty(events)) {
      eventsCount = events.size();
      if (LOG.isDebugEnabled()) {
        int minEventTime, maxEventTime;
        long minId, maxId;
        T firstNotification = events.get(0);
        T lastNotification = events.get(eventsCount - 1);
        if (MNotificationLog.class.equals(tableClass)) {
          minEventTime = ((MNotificationLog) firstNotification).getEventTime();
          minId = ((MNotificationLog) firstNotification).getEventId();
          maxEventTime = ((MNotificationLog) lastNotification).getEventTime();
          maxId = ((MNotificationLog) lastNotification).getEventId();
        } else if (MTxnWriteNotificationLog.class.equals(tableClass)) {
          minEventTime = ((MTxnWriteNotificationLog) firstNotification).getEventTime();
          minId = ((MTxnWriteNotificationLog) firstNotification).getTxnId();
          maxEventTime = ((MTxnWriteNotificationLog) lastNotification).getEventTime();
          maxId = ((MTxnWriteNotificationLog) lastNotification).getTxnId();
        } else {
          throw new RuntimeException(
              "Cleaning of older " + tableName + " events failed. " + "Reason: Unknown table encountered " + tableClass.getName());
        }
        LOG.debug(
            "Remove {} batch of {} events with eventTime < {}, min {}: {}, max {}: {}, min eventTime {}, max eventTime {}",
            tableName, eventsCount, ageSec, key, minId, key, maxId, minEventTime, maxEventTime);
      }
      pm.deletePersistentAll(events);
    }
    return eventsCount;
  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() {
    Query query = pm.newQuery(MNotificationNextId.class);
    Collection<MNotificationNextId> ids = (Collection) query.execute();
    long id = 0;
    if (CollectionUtils.isNotEmpty(ids)) {
      id = ids.iterator().next().getNextEventId() - 1;
    }
    return new CurrentNotificationEventId(id);
  }

  @Override
  public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst) {
    Long result = 0L;
    long fromEventId = rqst.getFromEventId();
    String inputDbName = rqst.getDbName();
    String catName = rqst.isSetCatName() ? rqst.getCatName() : getDefaultCatalog(conf);
    long toEventId;
    String paramSpecs;
    List<Object> paramVals = new ArrayList<>();

    // We store a catalog name in lower case in metastore and also use the same way everywhere in
    // hive.
    assert catName.equals(catName.toLowerCase());

    // Build the query to count events, part by part
    String queryStr = "select count(eventId) from " + MNotificationLog.class.getName();
    // count fromEventId onwards events
    queryStr = queryStr + " where eventId > fromEventId";
    paramSpecs = "java.lang.Long fromEventId";
    paramVals.add(Long.valueOf(fromEventId));

    // Input database name can be a database name or a *. In the first case we add a filter
    // condition on dbName column, but not in the second case, since a * means all the
    // databases. In case we support more elaborate database name patterns in future, we will
    // have to apply a method similar to getNextNotification() method of MetaStoreClient.
    if (!inputDbName.equals("*")) {
      // dbName could be NULL in case of transaction related events, which also need to be
      // counted.
      queryStr = queryStr + " && (dbName == inputDbName || dbName == null)";
      paramSpecs = paramSpecs + ", java.lang.String inputDbName";
      // We store a database name in lower case in metastore.
      paramVals.add(inputDbName.toLowerCase());
    }

    // catName could be NULL in case of transaction related events, which also need to be
    // counted.
    queryStr = queryStr + " && (catalogName == catName || catalogName == null)";
    paramSpecs = paramSpecs +", java.lang.String catName";
    paramVals.add(catName);

    // count events upto toEventId if specified
    if (rqst.isSetToEventId()) {
      toEventId = rqst.getToEventId();
      queryStr = queryStr + " && eventId <= toEventId";
      paramSpecs = paramSpecs + ", java.lang.Long toEventId";
      paramVals.add(Long.valueOf(toEventId));
    }
    // Specify list of table names in the query string and parameter types
    if (rqst.isSetTableNames() && !rqst.getTableNames().isEmpty()) {
      queryStr = queryStr + " && (";
      for (String tableName : rqst.getTableNames()) {
        paramVals.add(tableName.toLowerCase());
        queryStr = queryStr + "tableName == tableName" + paramVals.size() + " || ";
        paramSpecs = paramSpecs + ", java.lang.String tableName" + paramVals.size();
      }
      queryStr = queryStr.substring(0, queryStr.length() - 4); // remove the last " || "
      queryStr += ")";
    }

    Query query = pm.newQuery(queryStr);
    query.declareParameters(paramSpecs);
    result = (Long) query.executeWithArray(paramVals.toArray());
    // Cap the event count by limit if specified.
    long  eventCount = result.longValue();
    if (rqst.isSetLimit() && eventCount > rqst.getLimit()) {
      eventCount = rqst.getLimit();
    }
    return new NotificationEventsCountResponse(eventCount);
  }

  @Override
  public void cleanWriteNotificationEvents(int olderThan) {
    cleanOlderEvents(olderThan, MTxnWriteNotificationLog.class, "TxnWriteNotificationLog");
  }

  @Override
  public List<WriteEventInfo> getAllWriteEventInfo(long txnId, String dbName, String tableName) throws MetaException {
    List<WriteEventInfo> writeEventInfoList = null;
    List<String> parameterVals = new ArrayList<>();
    StringBuilder filterBuilder = new StringBuilder(" txnId == " + Long.toString(txnId));
    if (dbName != null && !"*".equals(dbName)) { // * means get all database, so no need to add filter
      appendSimpleCondition(filterBuilder, "database", new String[]{dbName}, parameterVals);
    }
    if (tableName != null && !"*".equals(tableName)) {
      appendSimpleCondition(filterBuilder, "table", new String[]{tableName}, parameterVals);
    }
    Query query = pm.newQuery(MTxnWriteNotificationLog.class, filterBuilder.toString());
    query.setOrdering("database,table ascending");
    List<MTxnWriteNotificationLog> mplans = (List<MTxnWriteNotificationLog>) query.executeWithArray(
        parameterVals.toArray(new String[0]));
    pm.retrieveAll(mplans);
    if (mplans != null && mplans.size() > 0) {
      writeEventInfoList = Lists.newArrayList();
      for (MTxnWriteNotificationLog mplan : mplans) {
        WriteEventInfo writeEventInfo = new WriteEventInfo(mplan.getWriteId(), mplan.getDatabase(),
            mplan.getTable(), mplan.getFiles());
        writeEventInfo.setPartition(mplan.getPartition());
        writeEventInfo.setPartitionObj(mplan.getPartObject());
        writeEventInfo.setTableObj(mplan.getTableObject());
        writeEventInfoList.add(writeEventInfo);
      }
    }
    return writeEventInfoList;
  }
}
