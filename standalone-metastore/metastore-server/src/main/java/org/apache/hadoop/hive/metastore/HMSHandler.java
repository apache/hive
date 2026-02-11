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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.api.Package;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.dataconnector.DataConnectorProviderFactory;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.hadoop.hive.metastore.handler.AbstractRequestHandler;
import org.apache.hadoop.hive.metastore.handler.AddPartitionsHandler;
import org.apache.hadoop.hive.metastore.handler.AppendPartitionHandler;
import org.apache.hadoop.hive.metastore.handler.BaseHandler;
import org.apache.hadoop.hive.metastore.handler.DropPartitionsHandler;
import org.apache.hadoop.hive.metastore.handler.GetPartitionsHandler;
import org.apache.hadoop.hive.metastore.handler.PrivilegeHandler;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hadoop.hive.metastore.metrics.PerfLogger;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.properties.PropertyException;
import org.apache.hadoop.hive.metastore.properties.PropertyManager;
import org.apache.hadoop.hive.metastore.properties.PropertyMap;
import org.apache.hadoop.hive.metastore.properties.PropertyStore;
import org.apache.hadoop.hive.metastore.txn.*;
import org.apache.hadoop.hive.metastore.utils.FilterUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.join;

import static org.apache.hadoop.hive.common.repl.ReplConst.REPL_RESUME_STARTED_AFTER_FAILOVER;
import static org.apache.hadoop.hive.common.repl.ReplConst.REPL_TARGET_DATABASE_PROPERTY;
import static org.apache.hadoop.hive.metastore.HiveMetaStoreClient.RENAME_PARTITION_MAKE_COPY;
import static org.apache.hadoop.hive.metastore.ExceptionHandler.handleException;
import static org.apache.hadoop.hive.metastore.ExceptionHandler.newMetaException;
import static org.apache.hadoop.hive.metastore.ExceptionHandler.rethrowException;
import static org.apache.hadoop.hive.metastore.ExceptionHandler.throwMetaException;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_IN_TEST;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.getPartValsFromName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.isDbReplicationTarget;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.CAT_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.DB_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.parseDbName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.prependNotNullCatToDbName;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

/**
 * Default handler for all Hive Metastore methods. Implements methods defined in hive_metastore.thrift.
 */
public class HMSHandler extends PrivilegeHandler {
  public static final Logger LOG = LoggerFactory.getLogger(HMSHandler.class);
  private final boolean isInTest;
  private StorageSchemaReader storageSchemaReader;

  public static final String PARTITION_NUMBER_EXCEED_LIMIT_MSG =
      "Number of partitions scanned (=%d) on table '%s' exceeds limit (=%d). This is controlled on the metastore server by %s.";

  private static final String AVRO_SERDE_CLASS = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";

  public static RawStore getRawStore() {
    return HMSHandlerContext.getRawStore().orElse(null);
  }

  static void cleanupHandlerContext() {
    AtomicBoolean cleanedRawStore = new AtomicBoolean(false);
    try {
      HMSHandlerContext.getRawStore().ifPresent(rs -> {
        logAndAudit("Cleaning up thread local RawStore...");
        rs.shutdown();
        cleanedRawStore.set(true);
      });
    } finally {
      HMSHandlerContext.getHMSHandler().ifPresent(BaseHandler::notifyMetaListenersOnShutDown);
      if (cleanedRawStore.get()) {
        logAndAudit("Done cleaning up thread local RawStore");
      }
      HMSHandlerContext.clear();
    }
  }

  public static String getIPAddress() {
    if (HiveMetaStore.useSasl) {
      if (HiveMetaStore.saslServer != null && HiveMetaStore.saslServer.getRemoteAddress() != null) {
        return HiveMetaStore.saslServer.getRemoteAddress().getHostAddress();
      }
    } else {
      // if kerberos is not enabled
      return getThreadLocalIpAddress();
    }
    return null;
  }

  public HMSHandler(String name, Configuration conf) {
    super(name, conf);
    isInTest = MetastoreConf.getBoolVar(this.conf, HIVE_IN_TEST);
  }

  public static RawStore getMSForConf(Configuration conf) throws MetaException {
    RawStore ms = getRawStore();
    if (ms == null) {
      ms = newRawStoreForConf(conf);
      try {
        ms.verifySchema();
      } catch (MetaException e) {
        ms.shutdown();
        throw e;
      }
      HMSHandlerContext.setRawStore(ms);
      LOG.info("Created RawStore: {}", ms);
    }
    return ms;
  }

  static RawStore newRawStoreForConf(Configuration conf) throws MetaException {
    Configuration newConf = new Configuration(conf);
    String rawStoreClassName = MetastoreConf.getVar(newConf, ConfVars.RAW_STORE_IMPL);
    LOG.info("Opening raw store with implementation class: {}", rawStoreClassName);
    return RawStoreProxy.getProxy(newConf, conf, rawStoreClassName);
  }

  @VisibleForTesting
  public static void createDefaultCatalog(RawStore ms, Warehouse wh) throws MetaException,
      InvalidOperationException {
    try {
      Catalog defaultCat = ms.getCatalog(DEFAULT_CATALOG_NAME);
      // Null check because in some test cases we get a null from ms.getCatalog.
      if (defaultCat !=null && defaultCat.getLocationUri().equals("TBD")) {
        // One time update issue.  When the new 'hive' catalog is created in an upgrade the
        // script does not know the location of the warehouse.  So we need to update it.
        LOG.info("Setting location of default catalog, as it hasn't been done after upgrade");
        defaultCat.setLocationUri(wh.getWhRoot().toString());
        ms.alterCatalog(defaultCat.getName(), defaultCat);
      }

    } catch (NoSuchObjectException e) {
      Catalog cat = new Catalog(DEFAULT_CATALOG_NAME, wh.getWhRoot().toString());
      long time = System.currentTimeMillis() / 1000;
      cat.setCreateTime((int) time);
      cat.setDescription(Warehouse.DEFAULT_CATALOG_COMMENT);
      ms.createCatalog(cat);
    }
  }

  @Override
  public void shutdown() {
    cleanupHandlerContext();
    PerfLogger.getPerfLogger(false).cleanupPerfLogMetrics();
  }

  @Override
  public void create_catalog(CreateCatalogRequest rqst)
      throws AlreadyExistsException, InvalidObjectException, MetaException {
    Catalog catalog = rqst.getCatalog();
    startFunction("create_catalog", ": " + catalog.toString());
    boolean success = false;
    Exception ex = null;
    try {
      try {
        getMS().getCatalog(catalog.getName());
        throw new AlreadyExistsException("Catalog " + catalog.getName() + " already exists");
      } catch (NoSuchObjectException e) {
        // expected
      }

      if (!MetaStoreUtils.validateName(catalog.getName(), null)) {
        throw new InvalidObjectException(catalog.getName() + " is not a valid catalog name");
      }

      if (catalog.getLocationUri() == null) {
        throw new InvalidObjectException("You must specify a path for the catalog");
      }

      RawStore ms = getMS();
      Path catPath = new Path(catalog.getLocationUri());
      boolean madeDir = false;
      Map<String, String> transactionalListenersResponses = Collections.emptyMap();
      try {
        firePreEvent(new PreCreateCatalogEvent(this, catalog));
        if (!wh.isDir(catPath)) {
          if (!wh.mkdirs(catPath)) {
            throw new MetaException("Unable to create catalog path " + catPath +
                ", failed to create catalog " + catalog.getName());
          }
          madeDir = true;
        }
        // set the create time of catalog
        long time = System.currentTimeMillis() / 1000;
        catalog.setCreateTime((int) time);
        ms.openTransaction();
        ms.createCatalog(catalog);

        // Create a default database inside the catalog
        CreateDatabaseRequest cdr = new CreateDatabaseRequest(DEFAULT_DATABASE_NAME);
        cdr.setCatalogName(catalog.getName());
        cdr.setLocationUri(catalog.getLocationUri());
        cdr.setParameters(Collections.emptyMap());
        cdr.setDescription("Default database for catalog " + catalog.getName());
        AbstractRequestHandler.offer(this, cdr).getResult();

        if (!transactionalListeners.isEmpty()) {
          transactionalListenersResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventType.CREATE_CATALOG,
                  new CreateCatalogEvent(true, this, catalog));
        }

        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
          if (madeDir) {
            wh.deleteDir(catPath, false, false);
          }
        }

        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
              EventType.CREATE_CATALOG,
              new CreateCatalogEvent(success, this, catalog),
              null,
              transactionalListenersResponses, ms);
        }
      }
      success = true;
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class)
          .defaultMetaException();
    } finally {
      endFunction("create_catalog", success, ex);
    }
  }

  @Override
  public void alter_catalog(AlterCatalogRequest rqst) throws TException {
    startFunction("alter_catalog " + rqst.getName());
    boolean success = false;
    Exception ex = null;
    RawStore ms = getMS();
    Map<String, String> transactionalListenersResponses = Collections.emptyMap();
    GetCatalogResponse oldCat = null;

    try {
      oldCat = get_catalog(new GetCatalogRequest(rqst.getName()));
      // Above should have thrown NoSuchObjectException if there is no such catalog
      assert oldCat != null && oldCat.getCatalog() != null;
      firePreEvent(new PreAlterCatalogEvent(oldCat.getCatalog(), rqst.getNewCat(), this));

      ms.openTransaction();
      ms.alterCatalog(rqst.getName(), rqst.getNewCat());

      if (!transactionalListeners.isEmpty()) {
        transactionalListenersResponses =
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                EventType.ALTER_CATALOG,
                new AlterCatalogEvent(oldCat.getCatalog(), rqst.getNewCat(), true, this));
      }

      success = ms.commitTransaction();
    } catch (MetaException|NoSuchObjectException e) {
      ex = e;
      throw e;
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }

      if ((null != oldCat) && (!listeners.isEmpty())) {
        MetaStoreListenerNotifier.notifyEvent(listeners,
            EventType.ALTER_CATALOG,
            new AlterCatalogEvent(oldCat.getCatalog(), rqst.getNewCat(), success, this),
            null, transactionalListenersResponses, ms);
      }
      endFunction("alter_catalog", success, ex);
    }

  }

  @Override
  public GetCatalogResponse get_catalog(GetCatalogRequest rqst)
      throws NoSuchObjectException, TException {
    String catName = rqst.getName();
    startFunction("get_catalog", ": " + catName);
    Catalog cat = null;
    Exception ex = null;
    try {
      cat = getMS().getCatalog(catName);
      firePreEvent(new PreReadCatalogEvent(this, cat));
      return new GetCatalogResponse(cat);
    } catch (MetaException|NoSuchObjectException e) {
      ex = e;
      throw e;
    } finally {
      endFunction("get_catalog", cat != null, ex);
    }
  }

  @Override
  public GetCatalogsResponse get_catalogs() throws MetaException {
    startFunction("get_catalogs");

    List<String> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getCatalogs();
    } catch (Exception e) {
      ex = e;
      throw e;
    } finally {
      endFunction("get_catalog", ret != null, ex);
    }
    return new GetCatalogsResponse(ret == null ? Collections.emptyList() : ret);

  }

  @Override
  public void drop_catalog(DropCatalogRequest rqst)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    String catName = rqst.getName();
    boolean ifExists = rqst.isIfExists();
    startFunction("drop_catalog", ": " + catName);
    if (DEFAULT_CATALOG_NAME.equalsIgnoreCase(catName)) {
      endFunction("drop_catalog", false, null);
      throw new MetaException("Can not drop " + DEFAULT_CATALOG_NAME + " catalog");
    }

    boolean success = false;
    Exception ex = null;
    try {
      dropCatalogCore(catName, ifExists);
      success = true;
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(NoSuchObjectException.class, InvalidOperationException.class, MetaException.class)
          .defaultMetaException();
    } finally {
      endFunction("drop_catalog", success, ex);
    }

  }

  private void dropCatalogCore(String catName, boolean ifExists)
      throws MetaException, NoSuchObjectException, InvalidOperationException {
    boolean success = false;
    Catalog cat = null;
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      cat = ms.getCatalog(catName);

      firePreEvent(new PreDropCatalogEvent(this, cat));

      List<String> allDbs = get_databases(prependNotNullCatToDbName(catName, null));
      if (allDbs != null && !allDbs.isEmpty()) {
        // It might just be the default, in which case we can drop that one if it's empty
        if (allDbs.size() == 1 && allDbs.get(0).equals(DEFAULT_DATABASE_NAME)) {
          try {
            DropDatabaseRequest req = new DropDatabaseRequest();
            req.setName(DEFAULT_DATABASE_NAME);
            req.setCatalogName(catName);
            req.setDeleteData(true);
            req.setCascade(false);
            drop_database_req(req);
          } catch (InvalidOperationException e) {
            // This means there are tables of something in the database
            throw new InvalidOperationException("There are still objects in the default " +
                "database for catalog " + catName);
          }
        } else {
          throw new InvalidOperationException("There are non-default databases in the catalog " +
              catName + " so it cannot be dropped.");
        }
      }

      ms.dropCatalog(catName);
      if (!transactionalListeners.isEmpty()) {
        transactionalListenerResponses =
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                EventType.DROP_CATALOG,
                new DropCatalogEvent(true, this, cat));
      }

      success = ms.commitTransaction();
    } catch (NoSuchObjectException e) {
      if (!ifExists) {
        throw new NoSuchObjectException(e.getMessage());
      } else {
        ms.rollbackTransaction();
      }
    } finally {
      if (success) {
        wh.deleteDir(wh.getDnsPath(new Path(cat.getLocationUri())), false, false);
      } else {
        ms.rollbackTransaction();
      }

      if (!listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners,
            EventType.DROP_CATALOG,
            new DropCatalogEvent(success, this, cat),
            null,
            transactionalListenerResponses, ms);
      }
    }
  }

  @Override
  @Deprecated
  public void create_database(final Database db)
      throws AlreadyExistsException, InvalidObjectException, MetaException {
    CreateDatabaseRequest req = new CreateDatabaseRequest(db.getName());
    req.setDescription(db.getDescription());
    req.setLocationUri(db.getLocationUri());
    req.setParameters(db.getParameters());
    req.setPrivileges(db.getPrivileges());
    req.setOwnerName(db.getOwnerName());
    req.setOwnerType(db.getOwnerType());
    req.setCatalogName(db.getCatalogName());
    req.setCreateTime(db.getCreateTime());
    req.setManagedLocationUri(db.getManagedLocationUri());
    req.setType(db.getType());
    req.setDataConnectorName(db.getConnector_name());
    req.setRemote_dbname(db.getRemote_dbname());

    create_database_req(req);
    //location and manged location might be set/changed.
    db.setLocationUri(req.getLocationUri());
    db.setManagedLocationUri(req.getManagedLocationUri());
  }

  @Override
  public void create_database_req(final CreateDatabaseRequest createDatabaseRequest)
          throws AlreadyExistsException, InvalidObjectException, MetaException {
    startFunction("create_database_req", ": " + createDatabaseRequest.getDatabaseName());
    boolean success = false;
    Exception ex = null;
    if (!createDatabaseRequest.isSetCatalogName()) {
      createDatabaseRequest.setCatalogName(getDefaultCatalog(conf));
    }

    try {
      try {
        if (null != get_database_core(createDatabaseRequest.getCatalogName(), createDatabaseRequest.getDatabaseName())) {
          throw new AlreadyExistsException("Database " + createDatabaseRequest.getDatabaseName() + " already exists");
        }
      } catch (NoSuchObjectException e) {
        // expected
      }
      success = AbstractRequestHandler.offer(this, createDatabaseRequest).success();
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidObjectException.class, AlreadyExistsException.class)
          .defaultMetaException();
    } finally {
      endFunction("create_database_req", success, ex);
    }
  }

  @Override
  @Deprecated
  public Database get_database(final String name)
      throws NoSuchObjectException, MetaException {
    GetDatabaseRequest request = new GetDatabaseRequest();
    String[] parsedDbName = parseDbName(name, conf);
    request.setName(parsedDbName[DB_NAME]);
    if (parsedDbName[CAT_NAME] != null) {
      request.setCatalogName(parsedDbName[CAT_NAME]);
    }
    return get_database_req(request);
  }

  @Override
  public Database get_database_core(String catName, final String name) throws NoSuchObjectException, MetaException {
    Database db = null;
    if (name == null) {
      throw new MetaException("Database name cannot be null.");
    }
    try {
      db = getMS().getDatabase(catName, name);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class, NoSuchObjectException.class)
          .defaultRuntimeException();
    }
    return db;
  }

  @Override
  public Database get_database_req(GetDatabaseRequest request) throws NoSuchObjectException, MetaException {
    startFunction("get_database", ": " + request.getName());
    Database db = null;
    Exception ex = null;
    if (request.getName() == null) {
      throw new MetaException("Database name cannot be null.");
    }
    List<String> processorCapabilities = request.getProcessorCapabilities();
    String processorId = request.getProcessorIdentifier();
    try {
      db = getMS().getDatabase(request.getCatalogName(), request.getName());
      firePreEvent(new PreReadDatabaseEvent(db, this));
      if (transformer != null) {
        db = transformer.transformDatabase(db, processorCapabilities, processorId);
      }
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, NoSuchObjectException.class)
          .defaultRuntimeException();
    } finally {
      endFunction("get_database", db != null, ex);
    }
    return db;
  }

  @Override
  @Deprecated
  public void alter_database(final String dbName, final Database newDB) throws TException {
    AlterDatabaseRequest alterDbReq = new AlterDatabaseRequest(dbName, newDB);
    alter_database_req(alterDbReq);
  }

  @Override
  public void alter_database_req(final AlterDatabaseRequest alterDbReq) throws TException {
    startFunction("alter_database_req " + alterDbReq.getOldDbName());
    boolean success = false;
    Exception ex = null;
    RawStore ms = getMS();
    Database oldDB = null;
    Database newDB = alterDbReq.getNewDb();
    String dbName = alterDbReq.getOldDbName();
    Map<String, String> transactionalListenersResponses = Collections.emptyMap();

    // Perform the same URI normalization as create_database_core.
    if (newDB.getLocationUri() != null) {
      newDB.setLocationUri(wh.getDnsPath(new Path(newDB.getLocationUri())).toString());
    }

    String[] parsedDbName = parseDbName(dbName, conf);

    // We can replicate into an empty database, in which case newDB will have indication that
    // it's target of replication but not oldDB. But replication flow will never alter a
    // database so that oldDB indicates that it's target or replication but not the newDB. So,
    // relying solely on newDB to check whether the database is target of replication works.
    boolean isReplicated = isDbReplicationTarget(newDB);
    try {
      oldDB = get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
      if (oldDB == null) {
        throw new MetaException("Could not alter database \"" + parsedDbName[DB_NAME] +
            "\". Could not retrieve old definition.");
      }

      // Add replication target event id.
      if (isReplicationEventIdUpdate(oldDB, newDB)) {
        Map<String, String> oldParams = new LinkedHashMap<>(newDB.getParameters());
        String currentNotificationLogID = Long.toString(ms.getCurrentNotificationEventId().getEventId());
        oldParams.put(REPL_TARGET_DATABASE_PROPERTY, currentNotificationLogID);
        LOG.debug("Adding the {} property for database {} with event id {}", REPL_TARGET_DATABASE_PROPERTY,
            newDB.getName(), currentNotificationLogID);
        newDB.setParameters(oldParams);
      }
      firePreEvent(new PreAlterDatabaseEvent(oldDB, newDB, this));

      ms.openTransaction();
      ms.alterDatabase(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], newDB);

      if (!transactionalListeners.isEmpty()) {
        AlterDatabaseEvent event = new AlterDatabaseEvent(oldDB, newDB, true, this, isReplicated);
        if (!event.shouldSkipCapturing()) {
          transactionalListenersResponses =
                  MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventType.ALTER_DATABASE, event);
        }
      }

      success = ms.commitTransaction();
    } catch (MetaException|NoSuchObjectException e) {
      ex = e;
      throw e;
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }

      if ((null != oldDB) && (!listeners.isEmpty())) {
        AlterDatabaseEvent event = new AlterDatabaseEvent(oldDB, newDB, success, this, isReplicated);
        if (!event.shouldSkipCapturing()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
                  EventType.ALTER_DATABASE, event, null, transactionalListenersResponses, ms);
        }
      }
      endFunction("alter_database_req", success, ex);
    }
  }

  /**
   * Checks whether the repl.last.id is being updated.
   * @param oldDb the old db object
   * @param newDb the new db object
   * @return true if repl.last.id is being changed.
   */
  private boolean isReplicationEventIdUpdate(Database oldDb, Database newDb) {
    Map<String, String> oldDbProp = oldDb.getParameters();
    Map<String, String> newDbProp = newDb.getParameters();
    if (newDbProp == null || newDbProp.isEmpty() ||
      Boolean.parseBoolean(newDbProp.get(REPL_RESUME_STARTED_AFTER_FAILOVER))) {
      return false;
    }
    String newReplId = newDbProp.get(ReplConst.REPL_TARGET_TABLE_PROPERTY);
    String oldReplId = oldDbProp != null ? oldDbProp.get(ReplConst.REPL_TARGET_TABLE_PROPERTY) : null;
    return newReplId != null && !newReplId.equalsIgnoreCase(oldReplId);
  }

  @Override
  @Deprecated
  public void drop_database(final String dbName, final boolean deleteData, final boolean cascade)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    String[] parsedDbName = parseDbName(dbName, conf);
    
    DropDatabaseRequest req = new DropDatabaseRequest();
    req.setName(parsedDbName[DB_NAME]);
    req.setCatalogName(parsedDbName[CAT_NAME]);
    req.setDeleteData(deleteData);
    req.setCascade(cascade);
    drop_database_req(req);  
  }

  @Override
  public AsyncOperationResp drop_database_req(final DropDatabaseRequest req)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    logAndAudit("drop_database: " + req.getName() + ", async: " + req.isAsyncDrop() + ", id: " + req.getId());

    if (DEFAULT_CATALOG_NAME.equalsIgnoreCase(req.getCatalogName())
          && DEFAULT_DATABASE_NAME.equalsIgnoreCase(req.getName())) {
      throw new MetaException("Can not drop " + DEFAULT_DATABASE_NAME + " database in catalog "
        + DEFAULT_CATALOG_NAME);
    }
    Exception ex = null;
    try {
      return AbstractRequestHandler.offer(this, req).getRequestStatus().toAsyncOperationResp();
    } catch (Exception e) {
      ex = e;
      // Reset the id of the request in case of RetryingHMSHandler retries
      if (req.getId() != null && req.isAsyncDrop() && !req.isCancel()) {
        req.setId(null);
      }
      throw handleException(e)
          .throwIfInstance(NoSuchObjectException.class, InvalidOperationException.class, MetaException.class)
          .defaultMetaException();
    } finally {
      for (MetaStoreEndFunctionListener listener : endFunctionListeners) {
        listener.onEndFunction("drop_database_req", new MetaStoreEndFunctionContext(ex == null, ex, null));
      }
    }
  }

  @Override
  public List<String> get_databases(final String pattern) throws MetaException {
    startFunction("get_databases", ": " + pattern);

    String[] parsedDbNamed = parseDbName(pattern, conf);
    List<String> ret = null;
    Exception ex = null;
    try {
      GetDatabaseObjectsRequest req = new GetDatabaseObjectsRequest();
      req.setCatalogName(parsedDbNamed[CAT_NAME]);
      if (parsedDbNamed[DB_NAME] != null) {
        req.setPattern(parsedDbNamed[DB_NAME]);
      }

      GetDatabaseObjectsResponse response = get_databases_req(req);
      ret = response.getDatabases().stream()
          .map(Database::getName)
          .collect(Collectors.toList());
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_databases", ret != null, ex);
    }
    return ret;
  }

  @Override
  public List<String> get_all_databases() throws MetaException {
    // get_databases filters results already. No need to filter here
    return get_databases(MetaStoreUtils.prependCatalogToDbName(null, null, conf));
  }

  @Override
  public GetDatabaseObjectsResponse get_databases_req(GetDatabaseObjectsRequest request)
      throws MetaException {
    String pattern = request.isSetPattern() ? request.getPattern() : null;
    String catName = request.isSetCatalogName() ? request.getCatalogName() : getDefaultCatalog(conf);

    startFunction("get_databases_req", ": catalogName=" + catName + " pattern=" + pattern);

    GetDatabaseObjectsResponse response = new GetDatabaseObjectsResponse();
    List<Database> dbs = null;
    Exception ex = null;

    try {
      dbs = getMS().getDatabaseObjects(catName, pattern);
      dbs = FilterUtils.filterDatabaseObjectsIfEnabled(isServerFilterEnabled, filterHook, dbs);
      response.setDatabases(dbs);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_databases_req", dbs != null, ex);
    }

    return response;
  }

  private void create_dataconnector_core(RawStore ms, final DataConnector connector)
      throws AlreadyExistsException, InvalidObjectException, MetaException {
    if (!MetaStoreUtils.validateName(connector.getName(), conf)) {
      throw new InvalidObjectException(connector.getName() + " is not a valid dataconnector name");
    }

    if (connector.getOwnerName() == null){
      try {
        connector.setOwnerName(SecurityUtils.getUGI().getShortUserName());
      }catch (Exception e){
        LOG.warn("Failed to get owner name for create dataconnector operation.", e);
      }
    }
    long time = System.currentTimeMillis()/1000;
    connector.setCreateTime((int) time);
    boolean success = false;
    Map<String, String> transactionalListenersResponses = Collections.emptyMap();
    try {
      firePreEvent(new PreCreateDataConnectorEvent(connector, this));

      ms.openTransaction();
      ms.createDataConnector(connector);

      if (!transactionalListeners.isEmpty()) {
        transactionalListenersResponses =
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                EventType.CREATE_DATACONNECTOR,
                new CreateDataConnectorEvent(connector, true, this));
      }

      success = ms.commitTransaction();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }

      if (!listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners,
            EventType.CREATE_DATACONNECTOR,
            new CreateDataConnectorEvent(connector, success, this),
            null,
            transactionalListenersResponses, ms);
      }
    }
  }

  @Override
  public void create_dataconnector_req(final CreateDataConnectorRequest connectorReq)
      throws AlreadyExistsException, InvalidObjectException, MetaException {
    startFunction("create_dataconnector_req", ": " + connectorReq.toString());
    boolean success = false;
    Exception ex = null;
    try {
      DataConnector connector = connectorReq.getConnector();
      try {
        if (null != get_dataconnector_core(connector.getName())) {
          throw new AlreadyExistsException("DataConnector " + connector.getName() + " already exists");
        }
      } catch (NoSuchObjectException e) {
        // expected
      }
      create_dataconnector_core(getMS(), connector);
      success = true;
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidObjectException.class, AlreadyExistsException.class)
          .defaultMetaException();
    } finally {
      endFunction("create_dataconnector_req", success, ex);
    }
  }

  @Override
  public DataConnector get_dataconnector_core(final String name) throws NoSuchObjectException, MetaException {
    DataConnector connector = null;
    if (name == null) {
      throw new MetaException("Data connector name cannot be null.");
    }
    try {
      connector = getMS().getDataConnector(name);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class, NoSuchObjectException.class)
          .defaultRuntimeException();
    }
    return connector;
  }

  @Override
  public DataConnector get_dataconnector_req(GetDataConnectorRequest request) throws NoSuchObjectException, MetaException {
    startFunction("get_dataconnector", ": " + request.getConnectorName());
    DataConnector connector = null;
    Exception ex = null;
    try {
      connector = get_dataconnector_core(request.getConnectorName());
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, NoSuchObjectException.class)
          .defaultRuntimeException();
    } finally {
      endFunction("get_dataconnector", connector != null, ex);
    }
    return connector;
  }

  @Override
  public void alter_dataconnector_req(final AlterDataConnectorRequest alterReq) throws TException {
    boolean success = false;
    Exception ex = null;
    RawStore ms = getMS();
    DataConnector oldDC = null;
    Map<String, String> transactionalListenersResponses = Collections.emptyMap();
    String dcName = alterReq.getConnectorName();
    DataConnector newDC = alterReq.getNewConnector();
    startFunction("alter_dataconnector " + dcName);
    try {
      oldDC = get_dataconnector_core(dcName);
      if (oldDC == null) {
        throw new MetaException("Could not alter dataconnector \"" + dcName +
            "\". Could not retrieve old definition.");
      }
      firePreEvent(new PreAlterDataConnectorEvent(oldDC, newDC, this));

      ms.openTransaction();
      ms.alterDataConnector(dcName, newDC);
      DataConnectorProviderFactory.invalidateDataConnectorFromCache(dcName);

        /*
        if (!transactionalListeners.isEmpty()) {
          transactionalListenersResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventType.ALTER_DATACONNECTOR,
                  new AlterDataConnectorEvent(oldDC, newDC, true, this));
        }
         */

      success = ms.commitTransaction();
    } catch (MetaException|NoSuchObjectException e) {
      ex = e;
      throw e;
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }
/*
        if ((null != oldDC) && (!listeners.isEmpty())) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
              EventType.ALTER_DATACONNECTOR,
              new AlterDataConnectorEvent(oldDC, newDC, success, this),
              null,
              transactionalListenersResponses, ms);
        }
 */
      endFunction("alter_database", success, ex);
    }
  }

  @Override
  public List<String> get_dataconnectors() throws MetaException {
    startFunction("get_dataconnectors");

    List<String> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getAllDataConnectorNames();
      ret = FilterUtils.filterDataConnectorsIfEnabled(isServerFilterEnabled, filterHook, ret);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_dataconnectors", ret != null, ex);
    }
    return ret;
  }

  @Override
  public void drop_dataconnector_req(final DropDataConnectorRequest dropDcReq) throws NoSuchObjectException, InvalidOperationException, MetaException {
    boolean success = false;
    DataConnector connector = null;
    Exception ex = null;
    RawStore ms = getMS();
    String dcName = dropDcReq.getConnectorName();
    boolean ifNotExists = dropDcReq.isIfNotExists();
    boolean checkReferences = dropDcReq.isCheckReferences();
    startFunction("drop_dataconnector_req", ": " + dcName);
    try {
      connector = getMS().getDataConnector(dcName);
      DataConnectorProviderFactory.invalidateDataConnectorFromCache(dcName);
    } catch (NoSuchObjectException e) {
      if (!ifNotExists) {
        throw new NoSuchObjectException("DataConnector " + dcName + " doesn't exist");
      } else {
        return;
      }
    }
    try {
      ms.openTransaction();
      // TODO find DBs with references to this connector
      // if any existing references and checkReferences=true, do not drop

      firePreEvent(new PreDropDataConnectorEvent(connector, this));

      if (!ms.dropDataConnector(dcName)) {
        throw new MetaException("Unable to drop dataconnector " + dcName);
      } else {
/*
          // TODO
          if (!transactionalListeners.isEmpty()) {
            transactionalListenerResponses =
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                    EventType.DROP_TABLE,
                    new DropTableEvent(tbl, true, deleteData,
                        this, isReplicated),
                    envContext);
          }
 */
        success = ms.commitTransaction();
      }
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }
/*
        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
              EventType.DROP_TABLE,
              new DropTableEvent(tbl, success, deleteData, this, isReplicated),
              envContext,
              transactionalListenerResponses, ms);
        }
 */
      endFunction("drop_dataconnector", success, ex);
    }
  }

  private void create_type_core(final RawStore ms, final Type type)
      throws AlreadyExistsException, MetaException, InvalidObjectException {
    if (!MetaStoreUtils.validateName(type.getName(), null)) {
      throw new InvalidObjectException("Invalid type name");
    }

    boolean success = false;
    try {
      ms.openTransaction();
      if (is_type_exists(ms, type.getName())) {
        throw new AlreadyExistsException("Type " + type.getName() + " already exists");
      }
      ms.createType(type);
      success = ms.commitTransaction();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }
    }
  }

  @Override
  public boolean create_type(final Type type) throws AlreadyExistsException,
      MetaException, InvalidObjectException {
    startFunction("create_type", ": " + type.toString());
    boolean success = false;
    Exception ex = null;
    try {
      create_type_core(getMS(), type);
      success = true;
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidObjectException.class, AlreadyExistsException.class)
          .defaultMetaException();
    } finally {
      endFunction("create_type", success, ex);
    }

    return success;
  }

  @Override
  public Type get_type(final String name) throws MetaException, NoSuchObjectException {
    startFunction("get_type", ": " + name);

    Type ret = null;
    Exception ex = null;
    try {
      ret = getMS().getType(name);
      if (null == ret) {
        throw new NoSuchObjectException("Type \"" + name + "\" not found.");
      }
    } catch (Exception e) {
      ex = e;
      throwMetaException(e);
    } finally {
      endFunction("get_type", ret != null, ex);
    }
    return ret;
  }

  private boolean is_type_exists(RawStore ms, String typeName)
      throws MetaException {
    return (ms.getType(typeName) != null);
  }

  @Override
  public boolean drop_type(final String name) throws MetaException, NoSuchObjectException {
    startFunction("drop_type", ": " + name);

    boolean success = false;
    Exception ex = null;
    try {
      // TODO:pc validate that there are no types that refer to this
      success = getMS().dropType(name);
    } catch (Exception e) {
      ex = e;
      throwMetaException(e);
    } finally {
      endFunction("drop_type", success, ex);
    }
    return success;
  }

  @Override
  public Table translate_table_dryrun(final CreateTableRequest req) throws AlreadyExistsException,
          MetaException, InvalidObjectException, InvalidInputException {
    Table transformedTbl = null;
    Table tbl = req.getTable();
    List<String> processorCapabilities = req.getProcessorCapabilities();
    String processorId = req.getProcessorIdentifier();
    if (!tbl.isSetCatName()) {
      tbl.setCatName(getDefaultCatalog(conf));
    }
    if (transformer != null) {
      transformedTbl = transformer.transformCreateTable(tbl, processorCapabilities, processorId);
    }
    return transformedTbl != null ? transformedTbl : tbl;
  }

  @Override
  @Deprecated
  public void create_table(final Table tbl) throws AlreadyExistsException,
      MetaException, InvalidObjectException, InvalidInputException {
    CreateTableRequest createTableReq = new CreateTableRequest(tbl);
    create_table_req(createTableReq);
  }

  @Override
  @Deprecated
  public void create_table_with_environment_context(final Table tbl,
                                                    final EnvironmentContext envContext)
      throws AlreadyExistsException, MetaException, InvalidObjectException,
      InvalidInputException {
    startFunction("create_table_with_environment_context", ": " + tbl.getTableName());
    boolean success = false;
    Exception ex = null;
    try {
      CreateTableRequest createTableReq = new CreateTableRequest(tbl);
      if (envContext != null) {
        createTableReq.setEnvContext(envContext);
      }
      create_table_req(createTableReq);
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class)
              .throwIfInstance(AlreadyExistsException.class, InvalidInputException.class)
              .defaultMetaException();
    } finally {
      endFunction("create_table_with_environment_context", success, ex, tbl.getTableName());
    }
  }

  @Override
  public void create_table_req(final CreateTableRequest req)
      throws AlreadyExistsException, MetaException, InvalidObjectException,
      InvalidInputException {
    Table tbl = req.getTable();
    startFunction("create_table_req", ": " + tbl.toString());
    boolean success = false;
    Exception ex = null;
    try {
      success = AbstractRequestHandler.offer(this, req).success();
    } catch (Exception e) {
      LOG.warn("create_table_req got ", e);
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class)
          .throwIfInstance(AlreadyExistsException.class, InvalidInputException.class)
          .convertIfInstance(NoSuchObjectException.class, InvalidObjectException.class)
          .defaultMetaException();
    } finally {
      endFunction("create_table_req", success, ex, tbl.getTableName());
    }
  }

  @Override
  @Deprecated
  public void create_table_with_constraints(final Table tbl,
                                            final List<SQLPrimaryKey> primaryKeys, final List<SQLForeignKey> foreignKeys,
                                            List<SQLUniqueConstraint> uniqueConstraints,
                                            List<SQLNotNullConstraint> notNullConstraints,
                                            List<SQLDefaultConstraint> defaultConstraints,
                                            List<SQLCheckConstraint> checkConstraints)
      throws AlreadyExistsException, MetaException, InvalidObjectException,
      InvalidInputException {
    startFunction("create_table_with_constraints", ": " + tbl.toString());
    boolean success = false;
    Exception ex = null;
    try {
      CreateTableRequest req = new CreateTableRequest(tbl);
      req.setPrimaryKeys(primaryKeys);
      req.setForeignKeys(foreignKeys);
      req.setUniqueConstraints(uniqueConstraints);
      req.setNotNullConstraints(notNullConstraints);
      req.setDefaultConstraints(defaultConstraints);
      req.setCheckConstraints(checkConstraints);
      create_table_req(req);
      success = true;
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class)
          .throwIfInstance(AlreadyExistsException.class, InvalidInputException.class)
          .defaultMetaException();
    } finally {
      endFunction("create_table_with_constraints", success, ex, tbl.getTableName());
    }
  }

  @Override
  public void drop_constraint(DropConstraintRequest req)
      throws MetaException, InvalidObjectException {
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    String dbName = req.getDbname();
    String tableName = req.getTablename();
    String constraintName = req.getConstraintname();
    startFunction("drop_constraint", ": " + constraintName);
    boolean success = false;
    Exception ex = null;
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      ms.dropConstraint(catName, dbName, tableName, constraintName);
      if (transactionalListeners.size() > 0) {
        DropConstraintEvent dropConstraintEvent = new DropConstraintEvent(catName, dbName,
            tableName, constraintName, true, this);
        for (MetaStoreEventListener transactionalListener : transactionalListeners) {
          transactionalListener.onDropConstraint(dropConstraintEvent);
        }
      }
      success = ms.commitTransaction();
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class)
          .convertIfInstance(NoSuchObjectException.class, InvalidObjectException.class)
          .defaultMetaException();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      } else {
        for (MetaStoreEventListener listener : listeners) {
          DropConstraintEvent dropConstraintEvent = new DropConstraintEvent(catName, dbName,
              tableName, constraintName, true, this);
          listener.onDropConstraint(dropConstraintEvent);
        }
      }
      endFunction("drop_constraint", success, ex, constraintName);
    }
  }

  @Override
  public void add_primary_key(AddPrimaryKeyRequest req)
      throws MetaException, InvalidObjectException {
    List<SQLPrimaryKey> primaryKeyCols = req.getPrimaryKeyCols();
    String constraintName = (CollectionUtils.isNotEmpty(primaryKeyCols)) ?
        primaryKeyCols.get(0).getPk_name() : "null";
    startFunction("add_primary_key", ": " + constraintName);
    boolean success = false;
    Exception ex = null;
    if (CollectionUtils.isNotEmpty(primaryKeyCols) && !primaryKeyCols.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      primaryKeyCols.forEach(pk -> pk.setCatName(defaultCat));
    }
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      List<SQLPrimaryKey> primaryKeys = ms.addPrimaryKeys(primaryKeyCols);
      if (transactionalListeners.size() > 0) {
        if (CollectionUtils.isNotEmpty(primaryKeys)) {
          AddPrimaryKeyEvent addPrimaryKeyEvent = new AddPrimaryKeyEvent(primaryKeys, true, this);
          for (MetaStoreEventListener transactionalListener : transactionalListeners) {
            transactionalListener.onAddPrimaryKey(addPrimaryKeyEvent);
          }
        }
      }
      success = ms.commitTransaction();
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class)
          .defaultMetaException();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      } else if (primaryKeyCols != null && primaryKeyCols.size() > 0) {
        for (MetaStoreEventListener listener : listeners) {
          AddPrimaryKeyEvent addPrimaryKeyEvent = new AddPrimaryKeyEvent(primaryKeyCols, true, this);
          listener.onAddPrimaryKey(addPrimaryKeyEvent);
        }
      }
      endFunction("add_primary_key", success, ex, constraintName);
    }
  }

  @Override
  public void add_foreign_key(AddForeignKeyRequest req)
      throws MetaException, InvalidObjectException {
    List<SQLForeignKey> foreignKeys = req.getForeignKeyCols();
    String constraintName = CollectionUtils.isNotEmpty(foreignKeys) ?
        foreignKeys.get(0).getFk_name() : "null";
    startFunction("add_foreign_key", ": " + constraintName);
    boolean success = false;
    Exception ex = null;
    if (CollectionUtils.isNotEmpty(foreignKeys) && !foreignKeys.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      foreignKeys.forEach(pk -> pk.setCatName(defaultCat));
    }
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      foreignKeys = ms.addForeignKeys(foreignKeys);
      if (transactionalListeners.size() > 0) {
        if (CollectionUtils.isNotEmpty(foreignKeys)) {
          AddForeignKeyEvent addForeignKeyEvent = new AddForeignKeyEvent(foreignKeys, true, this);
          for (MetaStoreEventListener transactionalListener : transactionalListeners) {
            transactionalListener.onAddForeignKey(addForeignKeyEvent);
          }
        }
      }
      success = ms.commitTransaction();
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class)
          .defaultMetaException();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      } else if (CollectionUtils.isNotEmpty(foreignKeys)) {
        for (MetaStoreEventListener listener : listeners) {
          AddForeignKeyEvent addForeignKeyEvent = new AddForeignKeyEvent(foreignKeys, true, this);
          listener.onAddForeignKey(addForeignKeyEvent);
        }
      }
      endFunction("add_foreign_key", success, ex, constraintName);
    }
  }

  @Override
  public void add_unique_constraint(AddUniqueConstraintRequest req)
      throws MetaException, InvalidObjectException {
    List<SQLUniqueConstraint> uniqueConstraints = req.getUniqueConstraintCols();
    String constraintName = (uniqueConstraints != null && uniqueConstraints.size() > 0) ?
        uniqueConstraints.get(0).getUk_name() : "null";
    startFunction("add_unique_constraint", ": " + constraintName);
    boolean success = false;
    Exception ex = null;
    if (!uniqueConstraints.isEmpty() && !uniqueConstraints.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      uniqueConstraints.forEach(pk -> pk.setCatName(defaultCat));
    }
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      uniqueConstraints = ms.addUniqueConstraints(uniqueConstraints);
      if (transactionalListeners.size() > 0) {
        if (CollectionUtils.isNotEmpty(uniqueConstraints)) {
          AddUniqueConstraintEvent addUniqueConstraintEvent = new AddUniqueConstraintEvent(uniqueConstraints, true, this);
          for (MetaStoreEventListener transactionalListener : transactionalListeners) {
            transactionalListener.onAddUniqueConstraint(addUniqueConstraintEvent);
          }
        }
      }
      success = ms.commitTransaction();
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class)
          .defaultMetaException();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      } else if (CollectionUtils.isNotEmpty(uniqueConstraints)) {
        for (MetaStoreEventListener listener : listeners) {
          AddUniqueConstraintEvent addUniqueConstraintEvent = new AddUniqueConstraintEvent(uniqueConstraints, true, this);
          listener.onAddUniqueConstraint(addUniqueConstraintEvent);
        }
      }
      endFunction("add_unique_constraint", success, ex, constraintName);
    }
  }

  @Override
  public void add_not_null_constraint(AddNotNullConstraintRequest req)
      throws MetaException, InvalidObjectException {
    List<SQLNotNullConstraint> notNullConstraints = req.getNotNullConstraintCols();
    String constraintName = (notNullConstraints != null && notNullConstraints.size() > 0) ?
        notNullConstraints.get(0).getNn_name() : "null";
    startFunction("add_not_null_constraint", ": " + constraintName);
    boolean success = false;
    Exception ex = null;
    if (!notNullConstraints.isEmpty() && !notNullConstraints.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      notNullConstraints.forEach(pk -> pk.setCatName(defaultCat));
    }
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      notNullConstraints = ms.addNotNullConstraints(notNullConstraints);

      if (transactionalListeners.size() > 0) {
        if (CollectionUtils.isNotEmpty(notNullConstraints)) {
          AddNotNullConstraintEvent addNotNullConstraintEvent = new AddNotNullConstraintEvent(notNullConstraints, true, this);
          for (MetaStoreEventListener transactionalListener : transactionalListeners) {
            transactionalListener.onAddNotNullConstraint(addNotNullConstraintEvent);
          }
        }
      }
      success = ms.commitTransaction();
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class).defaultMetaException();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      } else if (CollectionUtils.isNotEmpty(notNullConstraints)) {
        for (MetaStoreEventListener listener : listeners) {
          AddNotNullConstraintEvent addNotNullConstraintEvent = new AddNotNullConstraintEvent(notNullConstraints, true, this);
          listener.onAddNotNullConstraint(addNotNullConstraintEvent);
        }
      }
      endFunction("add_not_null_constraint", success, ex, constraintName);
    }
  }

  @Override
  public void add_default_constraint(AddDefaultConstraintRequest req) throws MetaException, InvalidObjectException {
    List<SQLDefaultConstraint> defaultConstraints = req.getDefaultConstraintCols();
    String constraintName =
        CollectionUtils.isNotEmpty(defaultConstraints) ? defaultConstraints.get(0).getDc_name() : "null";
    startFunction("add_default_constraint", ": " + constraintName);
    boolean success = false;
    Exception ex = null;
    if (!defaultConstraints.isEmpty() && !defaultConstraints.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      defaultConstraints.forEach(pk -> pk.setCatName(defaultCat));
    }
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      defaultConstraints = ms.addDefaultConstraints(defaultConstraints);
      if (transactionalListeners.size() > 0) {
        if (CollectionUtils.isNotEmpty(defaultConstraints)) {
          AddDefaultConstraintEvent addDefaultConstraintEvent =
              new AddDefaultConstraintEvent(defaultConstraints, true, this);
          for (MetaStoreEventListener transactionalListener : transactionalListeners) {
            transactionalListener.onAddDefaultConstraint(addDefaultConstraintEvent);
          }
        }
      }
      success = ms.commitTransaction();
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class).defaultMetaException();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      } else if (CollectionUtils.isNotEmpty(defaultConstraints)) {
        for (MetaStoreEventListener listener : listeners) {
          AddDefaultConstraintEvent addDefaultConstraintEvent =
              new AddDefaultConstraintEvent(defaultConstraints, true, this);
          listener.onAddDefaultConstraint(addDefaultConstraintEvent);
        }
      }
      endFunction("add_default_constraint", success, ex, constraintName);
    }
  }

  @Override
  public void add_check_constraint(AddCheckConstraintRequest req)
      throws MetaException, InvalidObjectException {
    List<SQLCheckConstraint> checkConstraints= req.getCheckConstraintCols();
    String constraintName = CollectionUtils.isNotEmpty(checkConstraints) ?
        checkConstraints.get(0).getDc_name() : "null";
    startFunction("add_check_constraint", ": " + constraintName);
    boolean success = false;
    Exception ex = null;
    if (!checkConstraints.isEmpty() && !checkConstraints.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      checkConstraints.forEach(pk -> pk.setCatName(defaultCat));
    }
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      checkConstraints = ms.addCheckConstraints(checkConstraints);
      if (transactionalListeners.size() > 0) {
        if (CollectionUtils.isNotEmpty(checkConstraints)) {
          AddCheckConstraintEvent addcheckConstraintEvent = new AddCheckConstraintEvent(checkConstraints, true, this);
          for (MetaStoreEventListener transactionalListener : transactionalListeners) {
            transactionalListener.onAddCheckConstraint(addcheckConstraintEvent);
          }
        }
      }
      success = ms.commitTransaction();
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class).defaultMetaException();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      } else if (CollectionUtils.isNotEmpty(checkConstraints)) {
        for (MetaStoreEventListener listener : listeners) {
          AddCheckConstraintEvent addCheckConstraintEvent = new AddCheckConstraintEvent(checkConstraints, true, this);
          listener.onAddCheckConstraint(addCheckConstraintEvent);
        }
      }
      endFunction("add_check_constraint", success, ex, constraintName);
    }
  }

  @Override
  @Deprecated
  public void drop_table(final String dbname, final String name, final boolean deleteData)
      throws NoSuchObjectException, MetaException {
    String[] parsedDbName = parseDbName(dbname, conf);
    DropTableRequest dropTableReq = new DropTableRequest(parsedDbName[DB_NAME], name);
    dropTableReq.setDeleteData(deleteData);
    dropTableReq.setCatalogName(parsedDbName[CAT_NAME]);
    dropTableReq.setDropPartitions(true);
    drop_table_req(dropTableReq);
  }

  @Override
  @Deprecated
  public void drop_table_with_environment_context(final String dbname, final String name, final boolean deleteData,
      final EnvironmentContext envContext) throws NoSuchObjectException, MetaException {
    String[] parsedDbName = parseDbName(dbname, conf);
    DropTableRequest dropTableReq = new DropTableRequest(parsedDbName[DB_NAME], name);
    dropTableReq.setDeleteData(deleteData);
    dropTableReq.setCatalogName(parsedDbName[CAT_NAME]);
    dropTableReq.setDropPartitions(true);
    dropTableReq.setEnvContext(envContext);
    drop_table_req(dropTableReq);
  }

  @Override
  public AsyncOperationResp drop_table_req(DropTableRequest dropReq)
      throws MetaException, NoSuchObjectException {
    logAndAudit("drop_table_req: " + dropReq.getTableName() +
        ", async: " + dropReq.isAsyncDrop() + ", id: " + dropReq.getId());
    Exception ex = null;
    try {
      return AbstractRequestHandler.offer(this, dropReq).getRequestStatus().toAsyncOperationResp();
    } catch (Exception e) {
      ex = e;
      // Here we get an exception, the RetryingHMSHandler might retry the call,
      // need to clear the id from the request, so AbstractRequestHandler can
      // start a new execution other than reuse the cached failed handler.
      if (dropReq.getId() != null && dropReq.isAsyncDrop() && !dropReq.isCancel()) {
        dropReq.setId(null);
      }
      throw handleException(e).throwIfInstance(MetaException.class, NoSuchObjectException.class)
              .convertIfInstance(IOException.class, MetaException.class).defaultMetaException();
    } finally {
      for (MetaStoreEndFunctionListener listener : endFunctionListeners) {
        listener.onEndFunction("drop_table_req", new MetaStoreEndFunctionContext(ex == null, ex,
            TableName.getQualified(dropReq.getCatalogName(), dropReq.getDbName(), dropReq.getTableName())));
      }
    }
  }

  @Override
  public void truncate_table(final String dbName, final String tableName, List<String> partNames)
      throws NoSuchObjectException, MetaException {
    // Deprecated path, won't work for txn tables.
    TruncateTableRequest truncateTableReq = new TruncateTableRequest(dbName, tableName);
    truncateTableReq.setPartNames(partNames);
    truncate_table_req(truncateTableReq);
  }

  @Override
  public TruncateTableResponse truncate_table_req(TruncateTableRequest req)
      throws NoSuchObjectException, MetaException {
    String[] parsedDbName = parseDbName(req.getDbName(), getConf());
    startFunction("truncate_table_req",
        ": db=" + parsedDbName[DB_NAME] + " tab=" + req.getTableName());
    Exception ex = null;
    boolean success = false;
    try {
      success =  AbstractRequestHandler.offer(this, req).success();
      return new TruncateTableResponse();
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, NoSuchObjectException.class)
          .convertIfInstance(IOException.class, MetaException.class)
          .defaultMetaException();
    } finally {
      endFunction("truncate_table_req", success, ex,
          TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], req.getTableName()));
    }
  }

  @Override
  public List<ExtendedTableInfo> get_tables_ext(final GetTablesExtRequest req) throws MetaException {
    List<String> tables = new ArrayList<String>();
    List<ExtendedTableInfo> ret = new ArrayList<ExtendedTableInfo>();
    String catalog  = req.getCatalog();
    String database = req.getDatabase();
    String pattern  = req.getTableNamePattern();
    List<String> processorCapabilities = req.getProcessorCapabilities();
    int limit = req.getLimit();
    String processorId  = req.getProcessorIdentifier();
    List<Table> tObjects = new ArrayList<>();

    startTableFunction("get_tables_ext", catalog, database, pattern);
    Exception ex = null;
    try {
      tables = getMS().getTables(catalog, database, pattern, null, limit);
      LOG.debug("get_tables_ext:getTables() returned " + tables.size());
      tables = FilterUtils.filterTableNamesIfEnabled(isServerFilterEnabled, filterHook,
          catalog, database, tables);

      if (tables.size() > 0) {
        tObjects = getMS().getTableObjectsByName(catalog, database, tables);
        LOG.debug("get_tables_ext:getTableObjectsByName() returned " + tObjects.size());
        if (processorCapabilities == null || processorCapabilities.size() == 0 ||
            processorCapabilities.contains("MANAGERAWMETADATA")) {
          LOG.info("Skipping translation for processor with " + processorId);
        } else {
          if (transformer != null) {
            Map<Table, List<String>> retMap = transformer.transform(tObjects, processorCapabilities, processorId);

            for (Map.Entry<Table, List<String>> entry : retMap.entrySet())  {
              LOG.debug("Table " + entry.getKey().getTableName() + " requires " + Arrays.toString((entry.getValue()).toArray()));
              ret.add(convertTableToExtendedTable(entry.getKey(), entry.getValue(), req.getRequestedFields()));
            }
          } else {
            for (Table table : tObjects) {
              ret.add(convertTableToExtendedTable(table, processorCapabilities, req.getRequestedFields()));
            }
          }
        }
      }
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_tables_ext", ret != null, ex);
    }
    return ret;
  }

  private ExtendedTableInfo convertTableToExtendedTable (Table table,
                                                         List<String> processorCapabilities, int mask) {
    ExtendedTableInfo extTable = new ExtendedTableInfo(table.getTableName());
    if ((mask & GetTablesExtRequestFields.ACCESS_TYPE.getValue()) == GetTablesExtRequestFields.ACCESS_TYPE.getValue()) {
      extTable.setAccessType(table.getAccessType());
    }

    if ((mask & GetTablesExtRequestFields.PROCESSOR_CAPABILITIES.getValue())
        == GetTablesExtRequestFields.PROCESSOR_CAPABILITIES.getValue()) {
      extTable.setRequiredReadCapabilities(table.getRequiredReadCapabilities());
      extTable.setRequiredWriteCapabilities(table.getRequiredWriteCapabilities());
    }

    return extTable;
  }

  @Override
  public GetTableResult get_table_req(GetTableRequest req) throws MetaException,
      NoSuchObjectException {
    req.setCatName(req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf));
    return new GetTableResult(getTableInternal(req));
  }

  /**
   * This function retrieves table from metastore. If getColumnStats flag is true,
   * then engine should be specified so the table is retrieve with the column stats
   * for that engine.
   */
  private Table getTableInternal(GetTableRequest getTableRequest) throws MetaException, NoSuchObjectException {

    Preconditions.checkArgument(!getTableRequest.isGetColumnStats() || getTableRequest.getEngine() != null,
        "To retrieve column statistics with a table, engine parameter cannot be null");

    if (isInTest) {
      assertClientHasCapability(getTableRequest.getCapabilities(), ClientCapability.TEST_CAPABILITY, "Hive tests",
          "get_table_req");
    }

    Table t = null;
    startTableFunction("get_table", getTableRequest.getCatName(), getTableRequest.getDbName(),
        getTableRequest.getTblName());
    Exception ex = null;
    try {
      t = get_table_core(getTableRequest);
      if (MetaStoreUtils.isInsertOnlyTableParam(t.getParameters())) {
        assertClientHasCapability(getTableRequest.getCapabilities(), ClientCapability.INSERT_ONLY_TABLES,
            "insert-only tables", "get_table_req");
      }

      if (CollectionUtils.isEmpty(getTableRequest.getProcessorCapabilities()) || getTableRequest
          .getProcessorCapabilities().contains("MANAGERAWMETADATA")) {
        LOG.info("Skipping translation for processor with " + getTableRequest.getProcessorIdentifier());
      } else {
        if (transformer != null) {
          List<Table> tList = new ArrayList<>();
          tList.add(t);
          Map<Table, List<String>> ret = transformer
              .transform(tList, getTableRequest.getProcessorCapabilities(), getTableRequest.getProcessorIdentifier());
          if (ret.size() > 1) {
            LOG.warn("Unexpected resultset size:" + ret.size());
            throw new MetaException("Unexpected result from metadata transformer:return list size is " + ret.size());
          }
          t = ret.keySet().iterator().next();
        }
      }

      firePreEvent(new PreReadTableEvent(t, this));
    } catch (MetaException | NoSuchObjectException e) {
      ex = e;
      throw e;
    } finally {
      endFunction("get_table", t != null, ex, getTableRequest.getTblName());
    }
    return t;
  }

  @Override
  public List<TableMeta> get_table_meta(String dbnames, String tblNames, List<String> tblTypes)
      throws MetaException, NoSuchObjectException {
    List<TableMeta> t = null;
    String[] parsedDbName = parseDbName(dbnames, conf);
    startTableFunction("get_table_metas", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblNames);
    Exception ex = null;
    try {
      t = getMS().getTableMeta(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblNames, tblTypes);
      t = FilterUtils.filterTableMetasIfEnabled(isServerFilterEnabled, filterHook, t);
      t = filterReadableTables(parsedDbName[CAT_NAME], t);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_table_metas", t != null, ex);
    }
    return t;
  }

  /**
   * This function retrieves table from metastore. If getColumnStats flag is true,
   * then engine should be specified so the table is retrieve with the column stats
   * for that engine.
   */
  @Override
  public Table get_table_core(GetTableRequest getTableRequest) throws MetaException, NoSuchObjectException {
    Preconditions.checkArgument(!getTableRequest.isGetColumnStats() || getTableRequest.getEngine() != null,
        "To retrieve column statistics with a table, engine parameter cannot be null");
    String catName = getTableRequest.getCatName();
    String dbName = getTableRequest.getDbName();
    String tblName = getTableRequest.getTblName();
    Database db = null;
    Table t = null;
    try {
      db = get_database_core(catName, dbName);
    } catch (Exception e) { /* appears exception is not thrown currently if db doesnt exist */ }

    if (MetaStoreUtils.isDatabaseRemote(db)) {
      t = DataConnectorProviderFactory.getDataConnectorProvider(db).getTable(tblName);
      if (t == null) {
        throw new NoSuchObjectException(TableName.getQualified(catName, dbName, tblName) + " table not found");
      }
      t.setDbName(dbName);
      return t;
    }

    try {
      t = getMS().getTable(catName, dbName, tblName, getTableRequest.getValidWriteIdList(), getTableRequest.getId());
      if (t == null) {
        throw new NoSuchObjectException(TableName.getQualified(catName, dbName, tblName) + " table not found");
      }

      // If column statistics was requested and is valid fetch it.
      if (getTableRequest.isGetColumnStats()) {
        ColumnStatistics colStats = getMS().getTableColumnStatistics(catName, dbName, tblName,
            StatsSetupConst.getColumnsHavingStats(t.getParameters()), getTableRequest.getEngine(),
            getTableRequest.getValidWriteIdList());
        if (colStats != null) {
          t.setColStats(colStats);
        }
      }
    } catch (Exception e) {
      throwMetaException(e);
    }
    return t;
  }

  /**
   * Gets multiple tables from the hive metastore.
   *
   * @param req
   *          The GetTablesRequest object.
   * @return A list of tables whose names are in the the list "names" and
   *         are retrievable from the database specified by "dbnames."
   *         There is no guarantee of the order of the returned tables.
   *         If there are duplicate names, only one instance of the table will be returned.
   * @throws MetaException
   * @throws InvalidOperationException
   * @throws UnknownDBException
   */

  @Override
  public GetTablesResult get_table_objects_by_name_req(GetTablesRequest req) throws TException {
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    if (isDatabaseRemote(req.getDbName())) {
      return new GetTablesResult(getRemoteTableObjectsInternal(req.getDbName(), req.getTblNames(), req.getTablesPattern()));
    }
    return new GetTablesResult(getTableObjectsInternal(catName, req.getDbName(),
        req.getTblNames(), req.getCapabilities(), req.getProjectionSpec(), req.getTablesPattern()));
  }

  private List<Table> filterTablesByName(List<Table> tables, List<String> tableNames) {
    List<Table> filteredTables = new ArrayList<>();
    for (Table table : tables) {
      if (tableNames.contains(table.getTableName())) {
        filteredTables.add(table);
      }
    }
    return filteredTables;
  }

  private List<Table> getRemoteTableObjectsInternal(String dbname, List<String> tableNames, String pattern) throws MetaException {
    String[] parsedDbName = parseDbName(dbname, conf);
    try {
      // retrieve tables from remote database
      Database db = get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
      List<Table> tables = DataConnectorProviderFactory.getDataConnectorProvider(db).getTables(null);

      // filtered out undesired tables
      if (tableNames != null) {
        tables = filterTablesByName(tables, tableNames);
      }

      // set remote tables' local hive database reference
      for (Table table : tables) {
        table.setDbName(dbname);
      }

      return FilterUtils.filterTablesIfEnabled(isServerFilterEnabled, filterHook, tables);
    } catch (Exception e) {
      LOG.warn("Unexpected exception while getting table(s) in remote database " + dbname , e);
      if (isInTest) {
        // ignore the exception
        return new ArrayList<Table>();
      } else {
        throw newMetaException(e);
      }
    }
  }

  private List<Table> getTableObjectsInternal(String catName, String dbName,
                                              List<String> tableNames,
                                              ClientCapabilities capabilities,
                                              GetProjectionsSpec projectionsSpec, String tablePattern)
      throws MetaException, InvalidOperationException, UnknownDBException {
    if (isInTest) {
      assertClientHasCapability(capabilities, ClientCapability.TEST_CAPABILITY,
          "Hive tests", "get_table_objects_by_name_req");
    }

    if (projectionsSpec != null) {
      if (!projectionsSpec.isSetFieldList() && (projectionsSpec.isSetIncludeParamKeyPattern() ||
          projectionsSpec.isSetExcludeParamKeyPattern())) {
        throw new InvalidOperationException("Include and Exclude Param key are not supported.");
      }
    }

    List<Table> tables = new ArrayList<>();
    startMultiTableFunction("get_multi_table", dbName, tableNames);
    Exception ex = null;
    int tableBatchSize = MetastoreConf.getIntVar(conf,
        ConfVars.BATCH_RETRIEVE_MAX);

    try {
      if (dbName == null || dbName.isEmpty()) {
        throw new UnknownDBException("DB name is null or empty");
      }
      RawStore ms = getMS();
      if(tablePattern != null){
        tables = ms.getTableObjectsByName(catName, dbName, tableNames, projectionsSpec, tablePattern);
      }else {
        if (tableNames == null) {
          throw new InvalidOperationException(dbName + " cannot find null tables");
        }

        // The list of table names could contain duplicates. RawStore.getTableObjectsByName()
        // only guarantees returning no duplicate table objects in one batch. If we need
        // to break into multiple batches, remove duplicates first.
        List<String> distinctTableNames = tableNames;
        if (distinctTableNames.size() > tableBatchSize) {
          List<String> lowercaseTableNames = new ArrayList<>();
          for (String tableName : tableNames) {
            lowercaseTableNames.add(normalizeIdentifier(tableName));
          }
          distinctTableNames = new ArrayList<>(new HashSet<>(lowercaseTableNames));
        }

        int startIndex = 0;
        // Retrieve the tables from the metastore in batches. Some databases like
        // Oracle cannot have over 1000 expressions in a in-list
        while (startIndex < distinctTableNames.size()) {
          int endIndex = Math.min(startIndex + tableBatchSize, distinctTableNames.size());
          tables.addAll(ms.getTableObjectsByName(catName, dbName, distinctTableNames.subList(
                  startIndex, endIndex), projectionsSpec, tablePattern));
          startIndex = endIndex;
        }
      }
      for (Table t : tables) {
        if (t.getParameters() != null && MetaStoreUtils.isInsertOnlyTableParam(t.getParameters())) {
          assertClientHasCapability(capabilities, ClientCapability.INSERT_ONLY_TABLES,
              "insert-only tables", "get_table_req");
        }
      }

      tables = FilterUtils.filterTablesIfEnabled(isServerFilterEnabled, filterHook, tables);
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidOperationException.class, UnknownDBException.class)
          .defaultMetaException();
    } finally {
      endFunction("get_multi_table", tables != null, ex, join(tableNames, ","));
    }
    return tables;
  }

  @Override
  public void update_creation_metadata(String catName, final String dbName, final String tableName, CreationMetadata cm) throws MetaException {
    getMS().updateCreationMetadata(catName, dbName, tableName, cm);
  }

  private void assertClientHasCapability(ClientCapabilities client,
                                         ClientCapability value, String what, String call) throws MetaException {
    if (!doesClientHaveCapability(client, value)) {
      throw new MetaException("Your client does not appear to support " + what + ". To skip"
          + " capability checks, please set " + ConfVars.CAPABILITY_CHECK.toString()
          + " to false. This setting can be set globally, or on the client for the current"
          + " metastore session. Note that this may lead to incorrect results, data loss,"
          + " undefined behavior, etc. if your client is actually incompatible. You can also"
          + " specify custom client capabilities via " + call + " API.");
    }
  }

  private boolean doesClientHaveCapability(ClientCapabilities client, ClientCapability value) {
    if (!MetastoreConf.getBoolVar(getConf(), ConfVars.CAPABILITY_CHECK)) {
      return true;
    }
    return (client != null && client.isSetValues() && client.getValues().contains(value));
  }

  @Override
  public List<String> get_table_names_by_filter(
      final String dbName, final String filter, final short maxTables)
      throws MetaException, InvalidOperationException, UnknownDBException {
    List<String> tables = null;
    startFunction("get_table_names_by_filter", ": db = " + dbName + ", filter = " + filter);
    Exception ex = null;
    String[] parsedDbName = parseDbName(dbName, conf);
    try {
      if (parsedDbName[CAT_NAME] == null || parsedDbName[CAT_NAME].isEmpty() ||
          parsedDbName[DB_NAME] == null || parsedDbName[DB_NAME].isEmpty()) {
        throw new UnknownDBException("DB name is null or empty");
      }
      if (filter == null) {
        throw new InvalidOperationException(filter + " cannot apply null filter");
      }
      tables = getMS().listTableNamesByFilter(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], filter, maxTables);
      tables = FilterUtils.filterTableNamesIfEnabled(
          isServerFilterEnabled, filterHook, parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tables);

    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidOperationException.class, UnknownDBException.class)
          .defaultMetaException();
    } finally {
      endFunction("get_table_names_by_filter", tables != null, ex, join(tables, ","));
    }
    return tables;
  }

  @Override
  @Deprecated
  public Partition append_partition(final String dbName, final String tableName,
      final List<String> part_vals) throws InvalidObjectException,
      AlreadyExistsException, MetaException {
    String[] parsedDbName = parseDbName(dbName, conf);
    AppendPartitionsRequest appendPartitionsReq = new AppendPartitionsRequest();
    appendPartitionsReq.setDbName(parsedDbName[DB_NAME]);
    appendPartitionsReq.setTableName(tableName);
    appendPartitionsReq.setPartVals(part_vals);
    appendPartitionsReq.setCatalogName(parsedDbName[CAT_NAME]);
    return append_partition_req(appendPartitionsReq);
  }

  @Override
  @Deprecated
  public Partition append_partition_with_environment_context(final String dbName,
      final String tableName, final List<String> part_vals, final EnvironmentContext envContext)
      throws InvalidObjectException, AlreadyExistsException, MetaException {
    String[] parsedDbName = parseDbName(dbName, conf);
    AppendPartitionsRequest appendPartitionsReq = new AppendPartitionsRequest();
    appendPartitionsReq.setDbName(parsedDbName[DB_NAME]);
    appendPartitionsReq.setTableName(tableName);
    appendPartitionsReq.setPartVals(part_vals);
    appendPartitionsReq.setCatalogName(parsedDbName[CAT_NAME]);
    appendPartitionsReq.setEnvironmentContext(envContext);
    return append_partition_req(appendPartitionsReq);
  }

  @Override
  public Partition append_partition_req(final AppendPartitionsRequest appendPartitionsReq)
      throws InvalidObjectException, AlreadyExistsException, MetaException {
    String dbName = appendPartitionsReq.getDbName();
    String catName = appendPartitionsReq.isSetCatalogName() ?
        appendPartitionsReq.getCatalogName() : getDefaultCatalog(conf);
    String tableName = appendPartitionsReq.getTableName();
    startTableFunction("append_partition_req", catName, dbName, tableName);
    Exception ex = null;
    try {
      AppendPartitionHandler appendPartition = AbstractRequestHandler.offer(this, appendPartitionsReq);
      return appendPartition.getResult().partition();
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidObjectException.class, AlreadyExistsException.class)
          .defaultMetaException();
    } finally {
      endFunction("append_partition_req", ex == null, ex, tableName);
    }
  }

  @Override
  public AddPartitionsResult add_partitions_req(AddPartitionsRequest request)
      throws TException {
    startFunction("add_partitions_req",
        ": db=" + request.getDbName() + " tab=" + request.getTblName());
    AddPartitionsResult result = new AddPartitionsResult();
    if (request.getParts().isEmpty()) {
      return result;
    }
    String catName = request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf);
    String dbName = request.getDbName();
    String tblName = request.getTblName();
    startTableFunction("add_partitions_req", catName, dbName, tblName);

    Exception ex = null;
    try {
      // Make sure all the partitions have the catalog set as well
      AddPartitionsHandler addPartsOp = AbstractRequestHandler.offer(this, request);
      if (addPartsOp.success() && request.isNeedResult()) {
        AddPartitionsHandler.AddPartitionsResult addPartsResult = addPartsOp.getResult();
        if (request.isSkipColumnSchemaForPartition()) {
          if (addPartsResult.newParts() != null && !addPartsResult.newParts().isEmpty()) {
            StorageDescriptor sd = addPartsResult.newParts().getFirst().getSd().deepCopy();
            result.setPartitionColSchema(sd.getCols());
            addPartsResult.newParts().forEach(partition -> partition.getSd().getCols().clear());
          }
        }
        result.setPartitions(addPartsResult.newParts());
      }
    } catch (Exception e) {
      ex = e;
      throw handleException(e).defaultTException();
    } finally {
      endFunction("add_partitions_req", ex == null, ex, tblName);
    }

    if (!result.isSetPartitions() && request.isNeedResult()) {
      // This is mainly for backwards compatibility,
      // old client may ask for the added partitions even if it fails.
      result.setPartitions(request.getParts());
    }
    return result;
  }

  @Deprecated
  @Override
  public int add_partitions(final List<Partition> parts) throws MetaException,
      InvalidObjectException, AlreadyExistsException {
    if (parts == null) {
      throw new MetaException("Partition list cannot be null.");
    }
    if (parts.isEmpty()) {
      return 0;
    }
    String catName = parts.get(0).isSetCatName() ? parts.get(0).getCatName() : getDefaultCatalog(conf);
    String dbName = parts.get(0).getDbName();
    String tableName = parts.get(0).getTableName();
    startTableFunction("add_partitions", catName, dbName, tableName);

    Integer ret = null;
    Exception ex = null;
    try {
      // Old API assumed all partitions belong to the same table; keep the same assumption
      if (!parts.get(0).isSetCatName()) {
        parts.forEach(p -> p.setCatName(catName));
      }
      AddPartitionsRequest addPartitionsReq = new AddPartitionsRequest();
      addPartitionsReq.setDbName(parts.get(0).getDbName());
      addPartitionsReq.setTblName(parts.get(0).getTableName());
      addPartitionsReq.setParts(parts);
      addPartitionsReq.setIfNotExists(false);
      if (parts.get(0).isSetCatName()) {
        addPartitionsReq.setCatName(parts.get(0).getCatName());
      }
      ret = add_partitions_req(addPartitionsReq).getPartitions().size();
      assert ret == parts.size();
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidObjectException.class, AlreadyExistsException.class)
          .defaultMetaException();
    } finally {
      endFunction("add_partitions", ret != null, ex, tableName);
    }
    return ret;
  }

  @Override
  public int add_partitions_pspec(final List<PartitionSpec> partSpecs)
      throws TException {
    if (partSpecs.isEmpty()) {
      return 0;
    }

    String catName = partSpecs.get(0).isSetCatName() ? partSpecs.get(0).getCatName() : getDefaultCatalog(conf);
    String dbName = partSpecs.get(0).getDbName();
    String tableName = partSpecs.get(0).getTableName();
    startTableFunction("add_partitions_pspec", catName, dbName, tableName);

    Integer ret = null;
    Exception ex = null;
    try {
      // If the catalog name isn't set, we need to go through and set it.
      if (!partSpecs.get(0).isSetCatName()) {
        partSpecs.forEach(ps -> ps.setCatName(catName));
      }
      dbName = normalizeIdentifier(dbName);
      tableName = normalizeIdentifier(tableName);

      PartitionSpecProxy partitionSpecProxy = PartitionSpecProxy.Factory.get(partSpecs);
      final PartitionSpecProxy.PartitionIterator partitionIterator = partitionSpecProxy
              .getPartitionIterator();
      List<Partition> partitionsToAdd = new ArrayList<>(partitionSpecProxy.size());
      while (partitionIterator.hasNext()) {
        final Partition part = partitionIterator.getCurrent();
        // Normalize dbName and tblName of each part
        // to follow the case-insensitive behavior of replaced add_partitions_pspec_core
        part.setDbName(normalizeIdentifier(part.getDbName()));
        part.setTableName(normalizeIdentifier(part.getTableName()));

        partitionsToAdd.add(part);
        partitionIterator.next();
      }
      AddPartitionsRequest request = new AddPartitionsRequest(dbName, tableName, partitionsToAdd, false);
      request.setCatName(catName);
      return add_partitions_req(request).getPartitionsSize();
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidObjectException.class, AlreadyExistsException.class)
          .defaultMetaException();
    } finally {
      endFunction("add_partitions_pspec", ret != null, ex, tableName);
    }
  }

  @Deprecated
  @Override
  public Partition add_partition(final Partition part)
      throws InvalidObjectException, AlreadyExistsException, MetaException {
    return add_partition_with_environment_context(part, null);
  }

  @Deprecated
  @Override
  public Partition add_partition_with_environment_context( final Partition part, EnvironmentContext envContext)
          throws InvalidObjectException, AlreadyExistsException, MetaException {
    if (part == null) {
      throw new MetaException("Partition cannot be null.");
    }
    AddPartitionsRequest addPartitionsReq = new AddPartitionsRequest(part.getDbName(), part.getTableName(),
            new ArrayList<>(Arrays.asList(part)), false);
    addPartitionsReq.setEnvironmentContext(envContext);
    try {
      return add_partitions_req(addPartitionsReq).getPartitions().get(0);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class,
          AlreadyExistsException.class).defaultMetaException();
    }
  }

  @Override
  public Partition exchange_partition(Map<String, String> partitionSpecs,
                                      String sourceDbName, String sourceTableName, String destDbName,
                                      String destTableName) throws TException {
    exchange_partitions(partitionSpecs, sourceDbName, sourceTableName, destDbName, destTableName);
    // Wouldn't it make more sense to return the first element of the list returned by the
    // previous call?
    return new Partition();
  }

  @Override
  public List<Partition> exchange_partitions(Map<String, String> partitionSpecs,
                                             String sourceDbName, String sourceTableName, String destDbName,
                                             String destTableName) throws TException {
    String[] parsedDestDbName = parseDbName(destDbName, conf);
    String[] parsedSourceDbName = parseDbName(sourceDbName, conf);
    // No need to check catalog for null as parseDbName() will never return null for the catalog.
    if (partitionSpecs == null || parsedSourceDbName[DB_NAME] == null || sourceTableName == null
        || parsedDestDbName[DB_NAME] == null || destTableName == null) {
      throw new MetaException("The DB and table name for the source and destination tables,"
          + " and the partition specs must not be null.");
    }
    if (!parsedDestDbName[CAT_NAME].equals(parsedSourceDbName[CAT_NAME])) {
      throw new MetaException("You cannot move a partition across catalogs");
    }

    boolean success = false;
    boolean pathCreated = false;
    RawStore ms = getMS();
    ms.openTransaction();

    Table destinationTable =
        ms.getTable(
            parsedDestDbName[CAT_NAME], parsedDestDbName[DB_NAME], destTableName, null);
    if (destinationTable == null) {
      throw new MetaException( "The destination table " +
          TableName.getQualified(parsedDestDbName[CAT_NAME],
              parsedDestDbName[DB_NAME], destTableName) + " not found");
    }
    Table sourceTable =
        ms.getTable(
            parsedSourceDbName[CAT_NAME], parsedSourceDbName[DB_NAME], sourceTableName, null);
    if (sourceTable == null) {
      throw new MetaException("The source table " +
          TableName.getQualified(parsedSourceDbName[CAT_NAME],
              parsedSourceDbName[DB_NAME], sourceTableName) + " not found");
    }

    List<String> partVals = MetaStoreUtils.getPvals(sourceTable.getPartitionKeys(),
        partitionSpecs);
    List<String> partValsPresent = new ArrayList<> ();
    List<FieldSchema> partitionKeysPresent = new ArrayList<> ();
    int i = 0;
    for (FieldSchema fs: sourceTable.getPartitionKeys()) {
      String partVal = partVals.get(i);
      if (partVal != null && !partVal.equals("")) {
        partValsPresent.add(partVal);
        partitionKeysPresent.add(fs);
      }
      i++;
    }
    // Passed the unparsed DB name here, as get_partitions_ps expects to parse it
    List<Partition> partitionsToExchange = get_partitions_ps(sourceDbName, sourceTableName,
        partVals, (short)-1);
    if (partitionsToExchange == null || partitionsToExchange.isEmpty()) {
      throw new MetaException("No partition is found with the values " + partitionSpecs
          + " for the table " + sourceTableName);
    }
    boolean sameColumns = MetaStoreUtils.compareFieldColumns(
        sourceTable.getSd().getCols(), destinationTable.getSd().getCols());
    boolean samePartitions = MetaStoreUtils.compareFieldColumns(
        sourceTable.getPartitionKeys(), destinationTable.getPartitionKeys());
    if (!sameColumns || !samePartitions) {
      throw new MetaException("The tables have different schemas." +
          " Their partitions cannot be exchanged.");
    }
    Path sourcePath = new Path(sourceTable.getSd().getLocation(),
        Warehouse.makePartName(partitionKeysPresent, partValsPresent));
    Path destPath = new Path(destinationTable.getSd().getLocation(),
        Warehouse.makePartName(partitionKeysPresent, partValsPresent));
    List<Partition> destPartitions = new ArrayList<>();

    Map<String, String> transactionalListenerResponsesForAddPartition = Collections.emptyMap();
    List<Map<String, String>> transactionalListenerResponsesForDropPartition =
        Lists.newArrayListWithCapacity(partitionsToExchange.size());

    // Check if any of the partitions already exists in destTable.
    List<String> destPartitionNames = ms.listPartitionNames(parsedDestDbName[CAT_NAME],
        parsedDestDbName[DB_NAME], destTableName, (short) -1);
    if (destPartitionNames != null && !destPartitionNames.isEmpty()) {
      for (Partition partition : partitionsToExchange) {
        String partToExchangeName =
            Warehouse.makePartName(destinationTable.getPartitionKeys(), partition.getValues());
        if (destPartitionNames.contains(partToExchangeName)) {
          throw new MetaException("The partition " + partToExchangeName
              + " already exists in the table " + destTableName);
        }
      }
    }

    Database srcDb = ms.getDatabase(parsedSourceDbName[CAT_NAME], parsedSourceDbName[DB_NAME]);
    Database destDb = ms.getDatabase(parsedDestDbName[CAT_NAME], parsedDestDbName[DB_NAME]);
    if (!HiveMetaStore.isRenameAllowed(srcDb, destDb)) {
      throw new MetaException("Exchange partition not allowed for " +
          TableName.getQualified(parsedSourceDbName[CAT_NAME],
              parsedSourceDbName[DB_NAME], sourceTableName) + " Dest db : " + destDbName);
    }
    try {
      for (Partition partition: partitionsToExchange) {
        Partition destPartition = new Partition(partition);
        destPartition.setDbName(parsedDestDbName[DB_NAME]);
        destPartition.setTableName(destinationTable.getTableName());
        Path destPartitionPath = new Path(destinationTable.getSd().getLocation(),
            Warehouse.makePartName(destinationTable.getPartitionKeys(), partition.getValues()));
        destPartition.getSd().setLocation(destPartitionPath.toString());
        ms.addPartition(destPartition);
        destPartitions.add(destPartition);
        ms.dropPartition(parsedSourceDbName[CAT_NAME], partition.getDbName(), sourceTable.getTableName(),
            Warehouse.makePartName(sourceTable.getPartitionKeys(), partition.getValues()));
      }
      Path destParentPath = destPath.getParent();
      if (!wh.isDir(destParentPath)) {
        if (!wh.mkdirs(destParentPath)) {
          throw new MetaException("Unable to create path " + destParentPath);
        }
      }
      /*
       * TODO: Use the hard link feature of hdfs
       * once https://issues.apache.org/jira/browse/HDFS-3370 is done
       */
      pathCreated = wh.renameDir(sourcePath, destPath, false);

      // Setting success to false to make sure that if the listener fails, rollback happens.
      success = false;

      if (!transactionalListeners.isEmpty()) {
        transactionalListenerResponsesForAddPartition =
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                EventType.ADD_PARTITION,
                new AddPartitionEvent(destinationTable, destPartitions, true, this));

        for (Partition partition : partitionsToExchange) {
          DropPartitionEvent dropPartitionEvent =
              new DropPartitionEvent(sourceTable, partition, true, true, this);
          transactionalListenerResponsesForDropPartition.add(
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventType.DROP_PARTITION,
                  dropPartitionEvent));
        }
      }

      success = ms.commitTransaction();
      return destPartitions;
    } finally {
      if (!success || !pathCreated) {
        ms.rollbackTransaction();
        if (pathCreated) {
          wh.renameDir(destPath, sourcePath, false);
        }
      }

      if (!listeners.isEmpty()) {
        AddPartitionEvent addPartitionEvent = new AddPartitionEvent(destinationTable, destPartitions, success, this);
        MetaStoreListenerNotifier.notifyEvent(listeners,
            EventType.ADD_PARTITION,
            addPartitionEvent,
            null,
            transactionalListenerResponsesForAddPartition, ms);

        i = 0;
        for (Partition partition : partitionsToExchange) {
          DropPartitionEvent dropPartitionEvent =
              new DropPartitionEvent(sourceTable, partition, success, true, this);
          Map<String, String> parameters =
              (transactionalListenerResponsesForDropPartition.size() > i)
                  ? transactionalListenerResponsesForDropPartition.get(i)
                  : null;

          MetaStoreListenerNotifier.notifyEvent(listeners,
              EventType.DROP_PARTITION,
              dropPartitionEvent,
              null,
              parameters, ms);
          i++;
        }
      }
    }
  }

  @Deprecated
  @Override
  public boolean drop_partition(final String db_name, final String tbl_name,
                                final List<String> part_vals, final boolean deleteData)
      throws TException {
    return drop_partition_with_environment_context(db_name, tbl_name, part_vals, deleteData,
        null);
  }

  @Override
  public DropPartitionsResult drop_partitions_req(
      DropPartitionsRequest request) throws TException {
    startFunction("drop_partitions_req",
        ": db=" + request.getDbName() + " tab=" + request.getTblName());
    Exception ex = null;
    try {
      DropPartitionsResult resp = new DropPartitionsResult();
      DropPartitionsHandler dropPartsOp = AbstractRequestHandler.offer(this, request);
      if (dropPartsOp.success() && request.isNeedResult()) {
        resp.setPartitions(dropPartsOp.getResult().getPartitions());
      }
      return resp;
    } catch (Exception e) {
      ex = e;
      throw handleException(e).defaultTException();
    } finally {
      endFunction("drop_partition_req", ex == null, ex,
          TableName.getQualified(request.getCatName(), request.getDbName(), request.getTblName()));
    }
  }

  @Deprecated
  @Override
  public boolean drop_partition_with_environment_context(final String db_name, final String tbl_name, final List<String> part_vals,
      final boolean deleteData, final EnvironmentContext envContext) throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    DropPartitionRequest dropPartitionReq = new DropPartitionRequest(parsedDbName[DB_NAME], tbl_name);
    dropPartitionReq.setCatName(parsedDbName[CAT_NAME]);
    dropPartitionReq.setPartVals(part_vals);
    dropPartitionReq.setDeleteData(deleteData);
    dropPartitionReq.setEnvironmentContext(envContext);
    return drop_partition_req(dropPartitionReq);
  }

  @Override
  public boolean drop_partition_req(final DropPartitionRequest dropPartitionReq) throws TException {
    String dbName = dropPartitionReq.getDbName();
    String catName = dropPartitionReq.getCatName();
    String tbl_name = dropPartitionReq.getTblName();
    List<String> part_vals = dropPartitionReq.getPartVals();
    try {
      Table t = getMS().getTable(catName, dbName, tbl_name, null);
      if (t == null) {
        throw new InvalidObjectException(dbName + "." + tbl_name + " table not found");
      }
      List<String> partNames = new ArrayList<>();
      if (part_vals == null || part_vals.isEmpty()) {
        part_vals = getPartValsFromName(t, dropPartitionReq.getPartName());
      }
      partNames.add(Warehouse.makePartName(t.getPartitionKeys(), part_vals));
      LOG.info("drop_partition_req partition values: {}", part_vals);
      RequestPartsSpec requestPartsSpec = RequestPartsSpec.names(partNames);
      DropPartitionsRequest request = new DropPartitionsRequest(dbName, tbl_name, requestPartsSpec);
      request.setCatName(catName);
      request.setIfExists(false);
      request.setNeedResult(false);
      request.setDeleteData(dropPartitionReq.isDeleteData());
      request.setEnvironmentContext(dropPartitionReq.getEnvironmentContext());
      drop_partitions_req(request);
    } catch (Exception e) {
      handleException(e).convertIfInstance(InvalidObjectException.class, NoSuchObjectException.class)
          .rethrowException(e);
    }
    return true;
  }

  /**
   * Use {@link #get_partition_req(GetPartitionRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public Partition get_partition(final String db_name, final String tbl_name,
                                 final List<String> part_vals) throws MetaException, NoSuchObjectException {
    try {
      String[] parsedDbName = parseDbName(db_name, conf);
      GetPartitionRequest getPartitionRequest = new GetPartitionRequest(parsedDbName[DB_NAME], tbl_name, part_vals);
      getPartitionRequest.setCatName(parsedDbName[CAT_NAME]);
      return get_partition_req(getPartitionRequest).getPartition();
    } catch (TException e) {
      throw handleException(e).throwIfInstance(MetaException.class, NoSuchObjectException.class)
          .defaultMetaException();
    }
  }

  @Override
  public GetPartitionResponse get_partition_req(GetPartitionRequest req)
      throws MetaException, NoSuchObjectException, TException {
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    TableName tableName = new TableName(catName, req.getDbName(), req.getTblName());
    String partName = GetPartitionsHandler.validatePartVals(this, tableName, req.getPartVals());
    GetPartitionsByNamesRequest gpnr = new GetPartitionsByNamesRequest(tableName.getDb(), tableName.getTable());
    gpnr.setNames(List.of(partName));
    List<Partition> partitions = GetPartitionsHandler.getPartitions(
        t -> startTableFunction("get_partition_req", catName, t.getDb(), t.getTable()),
        rex -> endFunction("get_partition_req",
            rex.getLeft() != null && rex.getLeft().success(), rex.getRight(), tableName.toString()),
        this, tableName, gpnr, true);
    return new GetPartitionResponse(partitions.getFirst());
  }

  @Override
  @Deprecated
  public Partition get_partition_with_auth(final String db_name,
                                           final String tbl_name, final List<String> part_vals,
                                           final String user_name, final List<String> group_names)
      throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    TableName tableName = new TableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
    String partName = GetPartitionsHandler.validatePartVals(this, tableName, part_vals);
    GetPartitionsPsWithAuthRequest gpar = new GetPartitionsPsWithAuthRequest(tableName.getDb(), tableName.getTable());
    gpar.setCatName(tableName.getCat());
    gpar.setUserName(user_name);
    gpar.setGroupNames(group_names);
    gpar.setPartNames(List.of(partName));
    List<Partition> partitions = GetPartitionsHandler.getPartitions(
        t ->  startFunction("get_partition_with_auth",
            " : tbl=" + t + samplePartitionValues(part_vals) + getGroupsCountAndUsername(user_name,group_names)),
        rex ->   endFunction("get_partition_with_auth",
            rex.getLeft() != null && rex.getLeft().success(), rex.getRight(), tbl_name),
        this, tableName, gpar, true);
    return partitions.getFirst();
  }

  /**
   * Use {@link #get_partitions_req(PartitionsRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public List<Partition> get_partitions(final String db_name, final String tbl_name,
      final short max_parts) throws NoSuchObjectException, MetaException {
    String[] parsedDbName = parseDbName(db_name, conf);
    TableName tableName = new TableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
    return GetPartitionsHandler.getPartitions(
        t -> startTableFunction("get_partitions", tableName.getCat(), tableName.getDb(), tableName.getTable()),
        rex -> endFunction("get_partitions",
            rex.getLeft() != null && rex.getLeft().success(), rex.getRight(), tableName.toString()),
        this, tableName, GetPartitionsHandler.createPartitionsRequest(tableName, max_parts), false);
  }

  @Override
  public PartitionsResponse get_partitions_req(PartitionsRequest req)
      throws NoSuchObjectException, MetaException, TException {
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    TableName tableName = new TableName(catName, req.getDbName(), req.getTblName());
    List<Partition> partitions = GetPartitionsHandler.getPartitions(
        t -> startTableFunction("get_partitions_req", catName, req.getDbName(), req.getTblName()),
        rex ->  endFunction("get_partitions_req",
            rex.getLeft() != null && rex.getLeft().success(), rex.getRight(),
            TableName.getQualified(catName, req.getDbName(), req.getTblName())),
        this, tableName, req, false);
    return new PartitionsResponse(partitions);
  }

  @Override
  @Deprecated
  public List<Partition> get_partitions_with_auth(final String dbName,
      final String tblName, final short maxParts, final String userName,
      final List<String> groupNames) throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    TableName tableName = new TableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName);
    GetPartitionsPsWithAuthRequest getAuthReq = new GetPartitionsPsWithAuthRequest(tableName.getDb(), tableName.getTable());
    getAuthReq.setCatName(tableName.getCat());
    getAuthReq.setUserName(userName);
    getAuthReq.setGroupNames(groupNames);
    getAuthReq.setMaxParts(maxParts);
    return get_partitions_ps_with_auth_req(getAuthReq).getPartitions();
  }

  @Override
  @Deprecated
  public List<PartitionSpec> get_partitions_pspec(final String db_name, final String tbl_name, final int max_parts)
      throws NoSuchObjectException, MetaException  {

    String[] parsedDbName = parseDbName(db_name, conf);
    String catName = parsedDbName[CAT_NAME];
    String dbName = parsedDbName[DB_NAME];
    String tableName = tbl_name.toLowerCase();

    startPartitionFunction("get_partitions_pspec", catName, dbName, tableName, max_parts);

    List<PartitionSpec> partitionSpecs = null;
    try {
      GetTableRequest getTableRequest = new GetTableRequest(dbName, tableName);
      getTableRequest.setCatName(catName);
      Table table = get_table_core(getTableRequest);
      // get_partitions will parse out the catalog and db names itself
      TableName t = new TableName(catName, dbName, tableName);
      GetPartitionsHandler<PartitionsRequest, Partition> getPartitionsHandler = AbstractRequestHandler.offer(this,
          new GetPartitionsHandler.GetPartitionsRequest<>(GetPartitionsHandler.createPartitionsRequest(t, max_parts), t));
      List<Partition> partitions  = getPartitionsHandler.getResult().result();
      if (is_partition_spec_grouping_enabled(table)) {
        partitionSpecs = MetaStoreServerUtils
            .getPartitionspecsGroupedByStorageDescriptor(table, partitions);
      }
      else {
        PartitionSpec pSpec = new PartitionSpec();
        pSpec.setPartitionList(new PartitionListComposingSpec(partitions));
        pSpec.setCatName(normalizeIdentifier(catName));
        pSpec.setDbName(normalizeIdentifier(dbName));
        pSpec.setTableName(tableName);
        pSpec.setRootPath(table.getSd().getLocation());
        partitionSpecs = Arrays.asList(pSpec);
      }

      return partitionSpecs;
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(NoSuchObjectException.class, MetaException.class).defaultMetaException();
    }
    finally {
      endFunction("get_partitions_pspec", partitionSpecs != null && !partitionSpecs.isEmpty(), null, tbl_name);
    }
  }

  @Override
  public GetPartitionsResponse get_partitions_with_specs(GetPartitionsRequest request)
      throws MetaException, TException {
    String catName = null;
    if (request.isSetCatName()) {
      catName = request.getCatName();
    }
    String[] parsedDbName = parseDbName(request.getDbName(), conf);
    String tableName = request.getTblName();
    if (catName == null) {
      // if catName is not provided in the request use the catName parsed from the dbName
      catName = parsedDbName[CAT_NAME];
    }
    startTableFunction("get_partitions_with_specs", catName, parsedDbName[DB_NAME],
        tableName);
    GetPartitionsResponse response = null;
    Exception ex = null;
    try {
      GetTableRequest getTableRequest = new GetTableRequest(parsedDbName[DB_NAME], tableName);
      getTableRequest.setCatName(catName);
      Table table = get_table_core(getTableRequest);
      List<Partition> partitions = getMS()
          .getPartitionSpecsByFilterAndProjection(table, request.getProjectionSpec(),
              request.getFilterSpec());
      List<String> processorCapabilities = request.getProcessorCapabilities();
      String processorId = request.getProcessorIdentifier();
      if (processorCapabilities == null || processorCapabilities.size() == 0 ||
          processorCapabilities.contains("MANAGERAWMETADATA")) {
        LOG.info("Skipping translation for processor with " + processorId);
      } else {
        if (transformer != null) {
          partitions = transformer.transformPartitions(partitions, table, processorCapabilities, processorId);
        }
      }
      List<PartitionSpec> partitionSpecs =
          MetaStoreServerUtils.getPartitionspecsGroupedByStorageDescriptor(table, partitions);
      response = new GetPartitionsResponse();
      response.setPartitionSpec(partitionSpecs);
    } catch (Exception e) {
      ex = e;
      rethrowException(e);
    } finally {
      endFunction("get_partitions_with_specs", response != null, ex, tableName);
    }
    return response;
  }

  private static boolean is_partition_spec_grouping_enabled(Table table) {

    Map<String, String> parameters = table.getParameters();
    return parameters.containsKey("hive.hcatalog.partition.spec.grouping.enabled")
        && parameters.get("hive.hcatalog.partition.spec.grouping.enabled").equalsIgnoreCase("true");
  }

  @Override
  @Deprecated
  public List<String> get_partition_names(final String db_name, final String tbl_name,
                                          final short max_parts) throws NoSuchObjectException, MetaException {
    String[] parsedDbName = parseDbName(db_name, conf);
    PartitionsRequest partitionReq = new PartitionsRequest(parsedDbName[DB_NAME], tbl_name);
    partitionReq.setCatName(parsedDbName[CAT_NAME]);
    partitionReq.setMaxParts(max_parts);
    return fetch_partition_names_req(partitionReq);
  }

  @Override
  public List<String> fetch_partition_names_req(final PartitionsRequest req)
      throws NoSuchObjectException, MetaException {
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    String dbName = req.getDbName(), tblName = req.getTblName();
    TableName tableName = new TableName(catName, dbName, tblName);
    try {
      return GetPartitionsHandler.getPartitionNames(
          t -> startTableFunction("fetch_partition_names_req", catName, dbName, tblName),
          rex -> endFunction("fetch_partition_names_req",
              rex.getLeft() != null && rex.getLeft().success(), rex.getRight(), tableName.toString()),
          this, tableName, req).result();
    } catch (TException ex) {
      if (ex instanceof NoSuchObjectException e) {
        // Keep it here just because some tests in TestListPartitions assume NoSuchObjectException
        // if the input is invalid at first sight.
        if (StringUtils.isBlank(dbName) || StringUtils.isBlank(tableName.getTable())) {
          throw e;
        }
        return Collections.emptyList();
      }
      throw handleException(ex).defaultMetaException();
    }
  }

  @Override
  public PartitionValuesResponse get_partition_values(PartitionValuesRequest request)
      throws MetaException {
    String catName = request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf);
    TableName tableName = new TableName(catName, request.getDbName(), request.getTblName());
    long maxParts = request.getMaxParts();
    String filter = request.isSetFilter() ? request.getFilter() : "";
    GetPartitionsHandler.GetPartitionsRequest<PartitionValuesRequest> getPartitionsRequest =
        new GetPartitionsHandler.GetPartitionsRequest<>(request, tableName);
    startPartitionFunction("get_partition_values", catName, tableName.getDb(), tableName.getTable(),
        (int) maxParts, filter);
    Exception ex = null;
    try {
      GetPartitionsHandler<PartitionValuesRequest, PartitionValuesResponse> getPartitionsHandler =
          AbstractRequestHandler.offer(this, getPartitionsRequest);
      List<PartitionValuesResponse> resps = getPartitionsHandler.getResult().result();
      if (resps == null || resps.isEmpty()) {
        throw new MetaException(String.format("Unable to get partition for %s", tableName.toString()));
      }
      return resps.getFirst();
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class).defaultMetaException();
    } finally {
      endFunction("get_partition_values", ex == null, ex, tableName.toString());
    }
  }

  @Deprecated
  @Override
  public void alter_partition(final String db_name, final String tbl_name,
                              final Partition new_part)
      throws TException {
    rename_partition(db_name, tbl_name, null, new_part);
  }

  @Deprecated
  @Override
  public void alter_partition_with_environment_context(final String dbName,
                                                       final String tableName, final Partition newPartition,
                                                       final EnvironmentContext envContext)
      throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    alter_partition_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, null,
        newPartition, envContext, null);
  }

  @Deprecated
  @Override
  public void rename_partition(final String db_name, final String tbl_name,
                               final List<String> part_vals, final Partition new_part)
      throws TException {
    // Call alter_partition_core without an environment context.
    String[] parsedDbName = parseDbName(db_name, conf);
    alter_partition_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name, part_vals, new_part,
        null, null);
  }

  @Override
  public RenamePartitionResponse rename_partition_req(RenamePartitionRequest req) throws TException {
    EnvironmentContext context = new EnvironmentContext();
    context.putToProperties(RENAME_PARTITION_MAKE_COPY, String.valueOf(req.isClonePart()));
    context.putToProperties(hive_metastoreConstants.TXN_ID, String.valueOf(req.getTxnId()));
    
    alter_partition_core(req.getCatName(), req.getDbName(), req.getTableName(), req.getPartVals(),
        req.getNewPart(), context, req.getValidWriteIdList());
    return new RenamePartitionResponse();
  };

  private void alter_partition_core(String catName, String db_name, String tbl_name,
                                List<String> part_vals, Partition new_part, EnvironmentContext envContext,
                                String validWriteIds) throws TException {
    startTableFunction("alter_partition", catName, db_name, tbl_name);

    if (LOG.isInfoEnabled()) {
      LOG.info("New partition values:" + new_part.getValues());
      if (part_vals != null && part_vals.size() > 0) {
        LOG.info("Old Partition values:" + part_vals);
      }
    }

    // Adds the missing scheme/authority for the new partition location
    if (new_part.getSd() != null) {
      String newLocation = new_part.getSd().getLocation();
      if (org.apache.commons.lang3.StringUtils.isNotEmpty(newLocation)) {
        Path tblPath = wh.getDnsPath(new Path(newLocation));
        new_part.getSd().setLocation(tblPath.toString());
      }
    }

    // Make sure the new partition has the catalog value set
    if (!new_part.isSetCatName()) {
      new_part.setCatName(catName);
    }

    Partition oldPart = null;
    Exception ex = null;
    try {
      Table table = getMS().getTable(catName, db_name, tbl_name, null);

      firePreEvent(new PreAlterPartitionEvent(db_name, tbl_name, table, part_vals, new_part, this));
      if (part_vals != null && !part_vals.isEmpty()) {
        MetaStoreServerUtils.validatePartitionNameCharacters(new_part.getValues(), getConf());
      }

      oldPart = alterHandler.alterPartition(getMS(), wh, catName, db_name, tbl_name,
          part_vals, new_part, envContext, this, validWriteIds);

      if (!listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners,
            EventType.ALTER_PARTITION,
            new AlterPartitionEvent(oldPart, new_part, table, false,
                true, new_part.getWriteId(), this),
            envContext);
      }
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidOperationException.class)
          .convertIfInstance(InvalidObjectException.class, InvalidOperationException.class)
          .convertIfInstance(AlreadyExistsException.class, InvalidOperationException.class)
          .defaultMetaException();
    } finally {
      endFunction("alter_partition", oldPart != null, ex, tbl_name);
    }
  }

  @Deprecated
  @Override
  public void alter_partitions(final String db_name, final String tbl_name,
                               final List<Partition> new_parts)
      throws TException {
    String[] o = parseDbName(db_name, conf);
    AlterPartitionsRequest req = new AlterPartitionsRequest(o[1], tbl_name, new_parts);
    req.setCatName(o[0]);
    alter_partitions_req(req);
  }

  @Override
  public AlterPartitionsResponse alter_partitions_req(AlterPartitionsRequest req) throws TException {
    alter_partitions_with_environment_context(req.getCatName(),
        req.getDbName(), req.getTableName(), req.getPartitions(), req.getEnvironmentContext(),
        req.isSetValidWriteIdList() ? req.getValidWriteIdList() : null,
        req.isSetWriteId() ? req.getWriteId() : -1);
    return new AlterPartitionsResponse();
  }

  // The old API we are keeping for backward compat. Not used within Hive.
  @Deprecated
  @Override
  public void alter_partitions_with_environment_context(final String db_name, final String tbl_name,
                                                        final List<Partition> new_parts, EnvironmentContext environmentContext)
      throws TException {
    String[] o = parseDbName(db_name, conf);
    AlterPartitionsRequest req = new AlterPartitionsRequest(o[1], tbl_name, new_parts);
    req.setCatName(o[0]);
    req.setEnvironmentContext(environmentContext);
    alter_partitions_req(req);
  }

  private void alter_partitions_with_environment_context(String catName, String db_name, final String tbl_name,
      final List<Partition> new_parts, EnvironmentContext environmentContext,
      String writeIdList, long writeId)
      throws TException {
    if (environmentContext == null) {
      environmentContext = new EnvironmentContext();
    }
    if (catName == null) {
      catName = getDefaultCatalog(conf);
    }

    startTableFunction("alter_partitions", catName, db_name, tbl_name);

    if (LOG.isInfoEnabled()) {
      for (Partition tmpPart : new_parts) {
        LOG.info("New partition values: catalog: {} database: {} table: {} partition: {}",
                catName, db_name, tbl_name, tmpPart.getValues());
      }
    }
    // all partitions are altered atomically
    // all prehooks are fired together followed by all post hooks
    List<Partition> oldParts = null;
    Exception ex = null;
    Lock tableLock = getTableLockFor(db_name, tbl_name);
    tableLock.lock();
    try {

      Table table = null;
      table = getMS().getTable(catName, db_name, tbl_name,  null);

      for (Partition tmpPart : new_parts) {
        // Make sure the catalog name is set in the new partition
        if (!tmpPart.isSetCatName()) {
          tmpPart.setCatName(getDefaultCatalog(conf));
        }
        if (tmpPart.getSd() != null && tmpPart.getSd().getCols() != null && tmpPart.getSd().getCols().isEmpty()) {
          tmpPart.getSd().setCols(table.getSd().getCols());
        }
        // old part values are same as new partition values here
        firePreEvent(new PreAlterPartitionEvent(db_name, tbl_name, table, tmpPart.getValues(), tmpPart, this));
      }
      oldParts = alterHandler.alterPartitions(getMS(), wh, catName, db_name, tbl_name, new_parts,
          environmentContext, writeIdList, writeId, this);
      Iterator<Partition> olditr = oldParts.iterator();

      for (Partition tmpPart : new_parts) {
        Partition oldTmpPart;
        if (olditr.hasNext()) {
          oldTmpPart = olditr.next();
        }
        else {
          throw new InvalidOperationException("failed to alterpartitions");
        }

        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
              EventType.ALTER_PARTITION,
              new AlterPartitionEvent(oldTmpPart, tmpPart, table, false,
                  true, writeId, this));
        }
      }
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidOperationException.class)
          .convertIfInstance(InvalidObjectException.class, InvalidOperationException.class)
          .convertIfInstance(AlreadyExistsException.class, InvalidOperationException.class)
          .defaultMetaException();
    } finally {
      tableLock.unlock();
      endFunction("alter_partitions", oldParts != null, ex, tbl_name);
    }
  }

  @Deprecated
  @Override
  public void alter_table(final String dbname, final String name,
                          final Table newTable)
      throws InvalidOperationException, MetaException {
    // Do not set an environment context.
    String[] parsedDbName = parseDbName(dbname, conf);
    AlterTableRequest req = new AlterTableRequest(parsedDbName[DB_NAME], name, newTable);
    req.setCatName(parsedDbName[CAT_NAME]);
    alter_table_req(req);
  }

  @Deprecated
  @Override
  public void alter_table_with_cascade(final String dbname, final String name,
                                       final Table newTable, final boolean cascade)
      throws InvalidOperationException, MetaException {
    EnvironmentContext envContext = null;
    if (cascade) {
      envContext = new EnvironmentContext();
      envContext.putToProperties(StatsSetupConst.CASCADE, StatsSetupConst.TRUE);
    }
    String[] parsedDbName = parseDbName(dbname, conf);
    AlterTableRequest req = new AlterTableRequest(parsedDbName[DB_NAME], name, newTable);
    req.setCatName(parsedDbName[CAT_NAME]);
    req.setEnvironmentContext(envContext);
    alter_table_req(req);
  }

  @Override
  public AlterTableResponse alter_table_req(AlterTableRequest req)
      throws InvalidOperationException, MetaException {
    alter_table_core(req.getCatName(), req.getDbName(), req.getTableName(),
        req.getTable(), req.getEnvironmentContext(), req.getValidWriteIdList(),
        req.getProcessorCapabilities(), req.getProcessorIdentifier(),
        req.getExpectedParameterKey(), req.getExpectedParameterValue());
    return new AlterTableResponse();
  }

  @Deprecated
  @Override
  public void alter_table_with_environment_context(final String dbname,
                                                   final String name, final Table newTable,
                                                   final EnvironmentContext envContext)
      throws InvalidOperationException, MetaException {
    String[] parsedDbName = parseDbName(dbname, conf);
    AlterTableRequest req = new AlterTableRequest(parsedDbName[DB_NAME], name, newTable);
    req.setCatName(parsedDbName[CAT_NAME]);
    req.setEnvironmentContext(envContext);
    alter_table_req(req);
  }

  private void alter_table_core(String catName, String dbname, String name, Table newTable,
                                EnvironmentContext envContext, String validWriteIdList, List<String> processorCapabilities,
                                String processorId, String expectedPropertyKey, String expectedPropertyValue)
          throws InvalidOperationException, MetaException {
    startFunction("alter_table", ": " + TableName.getQualified(catName, dbname, name)
        + " newtbl=" + newTable.getTableName());
    if (envContext == null) {
      envContext = new EnvironmentContext();
    }
    // Set the values to the envContext, so we do not have to change the HiveAlterHandler API
    if (expectedPropertyKey != null) {
      envContext.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_KEY, expectedPropertyKey);
    }
    if (expectedPropertyValue != null) {
      envContext.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_VALUE, expectedPropertyValue);
    }

    if (catName == null) {
      catName = getDefaultCatalog(conf);
    }

    // HIVE-25282: Drop/Alter table in REMOTE db should fail
    try {
      Database db = get_database_core(catName, dbname);
      if (MetaStoreUtils.isDatabaseRemote(db)) {
        throw new MetaException("Alter table in REMOTE database " + db.getName() + " is not allowed");
      }
    } catch (NoSuchObjectException e) {
      throw new InvalidOperationException("Alter table in REMOTE database is not allowed");
    }

    // Update the time if it hasn't been specified.
    if (newTable.getParameters() == null ||
        newTable.getParameters().get(hive_metastoreConstants.DDL_TIME) == null) {
      newTable.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(System
          .currentTimeMillis() / 1000));
    }

    // Adds the missing scheme/authority for the new table location
    if (newTable.getSd() != null) {
      String newLocation = newTable.getSd().getLocation();
      if (org.apache.commons.lang3.StringUtils.isNotEmpty(newLocation)) {
        Path tblPath = wh.getDnsPath(new Path(newLocation));
        newTable.getSd().setLocation(tblPath.toString());
      }
    }
    // Set the catalog name if it hasn't been set in the new table
    if (!newTable.isSetCatName()) {
      newTable.setCatName(catName);
    }

    boolean success = false;
    Exception ex = null;
    try {
      GetTableRequest request = new GetTableRequest(dbname, name);
      request.setCatName(catName);
      Table oldt = get_table_core(request);
      if (transformer != null) {
        newTable = transformer.transformAlterTable(oldt, newTable, processorCapabilities, processorId);
      }
      firePreEvent(new PreAlterTableEvent(oldt, newTable, this));
      alterHandler.alterTable(getMS(), wh, catName, dbname, name, newTable,
          envContext, this, validWriteIdList);
      success = true;
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidOperationException.class)
          .convertIfInstance(NoSuchObjectException.class, InvalidOperationException.class)
          .defaultMetaException();
    } finally {
      endFunction("alter_table", success, ex, name);
    }
  }

  @Override
  public List<String> get_tables(final String dbname, final String pattern)
      throws MetaException {
    startFunction("get_tables", ": db=" + dbname + " pat=" + pattern);

    List<String> ret = null;
    Exception ex = null;
    String[] parsedDbName = parseDbName(dbname, conf);
    try {
      if (isDatabaseRemote(dbname)) {
        Database db = get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
        return DataConnectorProviderFactory.getDataConnectorProvider(db).getTableNames();
      }
    } catch (Exception e) {
      throw newMetaException(e);
    }

    try {
      ret = getMS().getTables(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], pattern);
      if(ret !=  null && !ret.isEmpty()) {
        List<Table> tableInfo = new ArrayList<>();
        tableInfo = getMS().getTableObjectsByName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], ret);
        tableInfo = FilterUtils.filterTablesIfEnabled(isServerFilterEnabled, filterHook, tableInfo);// tableInfo object has the owner information of the table which is being passed to FilterUtils.
        ret = new ArrayList<>();
        for (Table tbl : tableInfo) {
          ret.add(tbl.getTableName());
        }
      }
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_tables", ret != null, ex);
    }
    return ret;
  }

  @Override
  public List<String> get_tables_by_type(final String dbname, final String pattern, final String tableType)
      throws MetaException {
    startFunction("get_tables_by_type", ": db=" + dbname + " pat=" + pattern + ",type=" + tableType);

    List<String> ret = null;
    Exception ex = null;
    String[] parsedDbName = parseDbName(dbname, conf);
    try {
      ret = getTablesByTypeCore(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], pattern, tableType);
      ret = FilterUtils.filterTableNamesIfEnabled(isServerFilterEnabled, filterHook,
          parsedDbName[CAT_NAME], parsedDbName[DB_NAME], ret);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_tables_by_type", ret != null, ex);
    }
    return ret;
  }

  private List<String> getTablesByTypeCore(final String catName, final String dbname,
                                           final String pattern, final String tableType) throws MetaException {
    startFunction("getTablesByTypeCore", ": catName=" + catName +
        ": db=" + dbname + " pat=" + pattern + ",type=" + tableType);

    List<String> ret = null;
    Exception ex = null;
    Database db = null;
    try {
      db = get_database_core(catName, dbname);
      if (MetaStoreUtils.isDatabaseRemote(db)) {
        return DataConnectorProviderFactory.getDataConnectorProvider(db).getTableNames();
      }
    } catch (Exception e) { /* ignore */ }

    try {
      ret = getMS().getTables(catName, dbname, pattern, TableType.valueOf(tableType), -1);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("getTablesByTypeCore", ret != null, ex);
    }
    return ret;
  }

  @Override
  public List<Table> get_all_materialized_view_objects_for_rewriting()
      throws MetaException {
    startFunction("get_all_materialized_view_objects_for_rewriting");

    List<Table> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getAllMaterializedViewObjectsForRewriting(DEFAULT_CATALOG_NAME);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_all_materialized_view_objects_for_rewriting", ret != null, ex);
    }
    return ret;
  }

  @Override
  public List<String> get_materialized_views_for_rewriting(final String dbname)
      throws MetaException {
    startFunction("get_materialized_views_for_rewriting", ": db=" + dbname);

    List<String> ret = null;
    Exception ex = null;
    String[] parsedDbName = parseDbName(dbname, conf);
    try {
      ret = getMS().getMaterializedViewsForRewriting(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_materialized_views_for_rewriting", ret != null, ex);
    }
    return ret;
  }

  @Override
  public List<String> get_all_tables(final String dbname) throws MetaException {
    startFunction("get_all_tables", ": db=" + dbname);

    List<String> ret = null;
    Exception ex = null;
    String[] parsedDbName = parseDbName(dbname, conf);
    try {
      if (isDatabaseRemote(dbname)) {
        Database db = get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
        return DataConnectorProviderFactory.getDataConnectorProvider(db).getTableNames();
      }
    } catch (Exception e) { /* ignore */ }

    try {
      if (isServerFilterEnabled) {
        List<TableMeta> filteredTableMetas = get_table_meta(dbname, "*", null);
        ret = filteredTableMetas.stream()
            .map(TableMeta::getTableName)
            .collect(Collectors.toList());
      } else {
        ret = getMS().getAllTables(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
      }
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_all_tables", ret != null, ex);
    }
    return ret;
  }

  /**
   * Use {@link #get_fields_req(GetFieldsRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public List<FieldSchema> get_fields(String db, String tableName)
      throws MetaException, UnknownTableException, UnknownDBException {
    return get_fields_with_environment_context(db, tableName, null);
  }

  @Override
  @Deprecated
  public List<FieldSchema> get_fields_with_environment_context(String db, String tableName,
                                                               final EnvironmentContext envContext)
      throws MetaException, UnknownTableException, UnknownDBException {
    String[] parsedDbName = parseDbName(db, conf);
    GetFieldsRequest req = new GetFieldsRequest(parsedDbName[DB_NAME], tableName);
    req.setCatName(parsedDbName[CAT_NAME]);
    req.setEnvContext(envContext);
    return get_fields_req(req).getFields();
  }

  private List<FieldSchema> get_fields_with_environment_context_core(String db, String tableName, final EnvironmentContext envContext)
          throws MetaException, UnknownTableException, UnknownDBException {
    startFunction("get_fields_with_environment_context_core", ": db=" + db + "tbl=" + tableName);
    String[] names = tableName.split("\\.");
    String base_table_name = names[0];
    String[] parsedDbName = parseDbName(db, conf);

    List<FieldSchema> ret = null;
    Exception ex = null;
    try {
      GetTableRequest getTableRequest = new GetTableRequest(parsedDbName[DB_NAME], base_table_name);
      getTableRequest.setCatName(parsedDbName[CAT_NAME]);
      Table tbl = get_table_core(getTableRequest);
      firePreEvent(new PreReadTableEvent(tbl, this));
      String serdeLib = tbl.getSd().getSerdeInfo().getSerializationLib();
      if (serdeLib == null ||
              MetastoreConf.getStringCollection(conf,
                      ConfVars.SERDES_USING_METASTORE_FOR_SCHEMA).contains(serdeLib)) {
        ret = tbl.getSd().getCols();
      } else {
        ret = getFieldsUsingSchemaReader(tbl, db, tableName, serdeLib, envContext);
      }
    } catch (Exception e) {
      ex = e;
      throw handleException(e).convertIfInstance(NoSuchObjectException.class, UnknownTableException.class)
          .defaultMetaException();
    } finally {
      endFunction("get_fields_with_environment_context_core", ret != null, ex, tableName);
    }
    return ret;
  }

  private List<FieldSchema> getFieldsUsingSchemaReader(Table tbl, String db, String tableName, String serdeLib,
      EnvironmentContext envContext) throws MetaException {
    try {
      StorageSchemaReader schemaReader = getStorageSchemaReader();
      return schemaReader.readSchema(tbl, envContext, getConf());
    } catch (UnsupportedOperationException e) {
      // Fallback to metastore for Avro Schema
      if (AVRO_SERDE_CLASS.equals(serdeLib)) {
        LOG.warn(
            "Unable to read schema from storage for AvroSerDe table '{}.{}'. Returning metastore SD columns " + 
                "as fallback; schema may be stale.",
            db, tableName, e);
        return tbl.getSd().getCols();
      }
      throw e;
    }
  }
  
  @Override
  public GetFieldsResponse get_fields_req(GetFieldsRequest req)
      throws MetaException, UnknownTableException, UnknownDBException {
    String dbName = MetaStoreUtils.prependCatalogToDbName(req.getCatName(), req.getDbName(), conf);
    List<FieldSchema> fields = get_fields_with_environment_context_core(
        dbName, req.getTblName(), req.getEnvContext());
    GetFieldsResponse res = new GetFieldsResponse();
    res.setFields(fields);
    return res;
  }

  private StorageSchemaReader getStorageSchemaReader() throws MetaException {
    if (storageSchemaReader == null) {
      String className =
          MetastoreConf.getVar(conf, MetastoreConf.ConfVars.STORAGE_SCHEMA_READER_IMPL);
      Class<? extends StorageSchemaReader> readerClass =
          JavaUtils.getClass(className, StorageSchemaReader.class);
      try {
        storageSchemaReader = readerClass.newInstance();
      } catch (InstantiationException|IllegalAccessException e) {
        LOG.error("Unable to instantiate class " + className, e);
        throw new MetaException(e.getMessage());
      }
    }
    return storageSchemaReader;
  }

  /**
   * Use {@link #get_schema_req(GetSchemaRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public List<FieldSchema> get_schema(String db, String tableName)
      throws MetaException, UnknownTableException, UnknownDBException {
    return get_schema_with_environment_context(db,tableName, null);
  }

  /**
   * Return the schema of the table. This function includes partition columns
   * in addition to the regular columns.
   *
   * @param db
   *          Name of the database
   * @param tableName
   *          Name of the table
   * @param envContext
   *          Store session based properties
   * @return List of columns, each column is a FieldSchema structure
   * @throws MetaException
   * @throws UnknownTableException
   * @throws UnknownDBException
   */
  @Override
  @Deprecated
  public List<FieldSchema> get_schema_with_environment_context(String db, String tableName,
                                                               final EnvironmentContext envContext)
      throws MetaException, UnknownTableException, UnknownDBException {
    String[] parsedDbName = parseDbName(db, conf);
    GetSchemaRequest req = new GetSchemaRequest(parsedDbName[DB_NAME], tableName);
    req.setCatName(parsedDbName[CAT_NAME]);
    req.setEnvContext(envContext);
    return get_schema_req(req).getFields();
  }

  private List<FieldSchema> get_schema_with_environment_context_core(String db, String tableName, final EnvironmentContext envContext)
          throws MetaException, UnknownTableException, UnknownDBException {
    startFunction("get_schema_with_environment_context_core", ": db=" + db + "tbl=" + tableName);
    boolean success = false;
    Exception ex = null;
    try {
      String[] names = tableName.split("\\.");
      String base_table_name = names[0];
      String[] parsedDbName = parseDbName(db, conf);

      Table tbl;
      try {
        GetTableRequest getTableRequest = new GetTableRequest(parsedDbName[DB_NAME], base_table_name);
        getTableRequest.setCatName(parsedDbName[CAT_NAME]);
        tbl = get_table_core(getTableRequest);
      } catch (NoSuchObjectException e) {
        throw new UnknownTableException(e.getMessage());
      }
      // Pass unparsed db name here
      List<FieldSchema> fieldSchemas = get_fields_with_environment_context_core(db, base_table_name,
              envContext);

      if (tbl == null || fieldSchemas == null) {
        throw new UnknownTableException(tableName + " doesn't exist");
      }

      if (tbl.getPartitionKeys() != null) {
        // Combine the column field schemas and the partition keys to create the
        // whole schema
        fieldSchemas.addAll(tbl.getPartitionKeys());
      }
      success = true;
      return fieldSchemas;
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
              .throwIfInstance(UnknownDBException.class, UnknownTableException.class, MetaException.class)
              .defaultMetaException();
    } finally {
      endFunction("get_schema_with_environment_context_core", success, ex, tableName);
    }
  }

  @Override
  public GetSchemaResponse get_schema_req(GetSchemaRequest req)
      throws MetaException, UnknownTableException, UnknownDBException {
    String dbName = MetaStoreUtils.prependCatalogToDbName(req.getCatName(), req.getDbName(), conf);
    List<FieldSchema> fields = get_schema_with_environment_context_core(
        dbName, req.getTblName(), req.getEnvContext());
    GetSchemaResponse res = new GetSchemaResponse();
    res.setFields(fields);
    return res;
  }

  @Override
  @Deprecated
  public Partition get_partition_by_name(final String db_name, final String tbl_name,
                                         final String part_name) throws TException {
    if (StringUtils.isBlank(part_name)) {
      throw new MetaException("The part_name in get_partition_by_name cannot be null or empty");
    }
    String[] parsedDbName = parseDbName(db_name, conf);
    TableName tableName = new TableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
    GetPartitionsByNamesRequest gpnr = new GetPartitionsByNamesRequest(tableName.getDb(), tableName.getTable());
    gpnr.setNames(List.of(part_name));
    List<Partition> partitions = GetPartitionsHandler.getPartitions(
        t ->  startFunction("get_partition_by_name", ": tbl=" + t + " part=" + part_name),
        rex -> endFunction("get_partition_by_name",
            rex.getLeft() != null && rex.getLeft().success(), rex.getRight(), tableName.toString()),
        this, tableName, gpnr, true);
    return partitions.getFirst();
  }

  @Deprecated
  @Override
  public Partition append_partition_by_name(final String db_name, final String tbl_name,
                                            final String part_name) throws TException {
    return append_partition_by_name_with_environment_context(db_name, tbl_name, part_name, null);
  }

  @Deprecated
  @Override
  public Partition append_partition_by_name_with_environment_context(final String db_name,
      final String tbl_name, final String part_name, final EnvironmentContext env_context)
      throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    AppendPartitionsRequest appendPartitionRequest = new AppendPartitionsRequest();
    appendPartitionRequest.setDbName(parsedDbName[DB_NAME]);
    appendPartitionRequest.setTableName(tbl_name);
    appendPartitionRequest.setName(part_name);
    appendPartitionRequest.setCatalogName(parsedDbName[CAT_NAME]);
    appendPartitionRequest.setEnvironmentContext(env_context);
    return append_partition_req(appendPartitionRequest);
  }

  @Deprecated
  @Override
  public boolean drop_partition_by_name(final String db_name, final String tbl_name,
                                        final String part_name, final boolean deleteData) throws TException {
    return drop_partition_by_name_with_environment_context(db_name, tbl_name, part_name,
        deleteData, null);
  }

  @Deprecated
  @Override
  public boolean drop_partition_by_name_with_environment_context(final String db_name,
                                                                 final String tbl_name, final String part_name, final boolean deleteData,
                                                                 final EnvironmentContext envContext) throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    DropPartitionRequest dropPartitionReq = new DropPartitionRequest(parsedDbName[DB_NAME], tbl_name);
    dropPartitionReq.setCatName(parsedDbName[CAT_NAME]);
    dropPartitionReq.setPartName(part_name);
    dropPartitionReq.setDeleteData(deleteData);
    dropPartitionReq.setEnvironmentContext(envContext);
    return drop_partition_req(dropPartitionReq);
  }

  @Override
  @Deprecated
  public List<Partition> get_partitions_ps(final String db_name,
                                           final String tbl_name, final List<String> part_vals,
                                           final short max_parts) throws TException {
    return get_partitions_ps_with_auth(db_name, tbl_name, part_vals, max_parts, null, null);
  }

  /**
   * Use {@link #get_partitions_ps_with_auth_req(GetPartitionsPsWithAuthRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public List<Partition> get_partitions_ps_with_auth(final String db_name,
      final String tbl_name, final List<String> part_vals,
      final short max_parts, final String userName,
      final List<String> groupNames) throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    TableName tableName = new TableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
    GetPartitionsPsWithAuthRequest getAuthReq = new GetPartitionsPsWithAuthRequest(tableName.getDb(), tableName.getTable());
    getAuthReq.setCatName(tableName.getCat());
    getAuthReq.setMaxParts(max_parts);
    getAuthReq.setUserName(userName);
    getAuthReq.setGroupNames(groupNames);
    getAuthReq.setPartVals(part_vals);
    return get_partitions_ps_with_auth_req(getAuthReq).getPartitions();
  }

  @Override
  public GetPartitionsPsWithAuthResponse get_partitions_ps_with_auth_req(GetPartitionsPsWithAuthRequest req)
      throws MetaException, NoSuchObjectException, TException {
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    TableName tableName = new TableName(catName, req.getDbName(), req.getTblName());
    List<Partition> partitions = GetPartitionsHandler.getPartitionsResult(
        t ->  startTableFunction("get_partitions_ps_with_auth_req", catName, t.getDb(), t.getTable()),
        rex -> endFunction("get_partitions_ps_with_auth_req",
            rex.getLeft() != null && rex.getLeft().success(), rex.getRight(), tableName.toString()),
        this, tableName, req).result();
    return new GetPartitionsPsWithAuthResponse(partitions);
  }

  /**
   * Use {@link #get_partition_names_ps_req(GetPartitionNamesPsRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public List<String> get_partition_names_ps(final String db_name,
      final String tbl_name, final List<String> part_vals, final short max_parts)
      throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    startPartitionFunction("get_partitions_names_ps", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name,
        max_parts, part_vals);

    List<String> ret = null;
    Exception ex = null;
    try {
      GetPartitionNamesPsRequest getPartitionNamesPsRequest = new GetPartitionNamesPsRequest(parsedDbName[DB_NAME], tbl_name);
      getPartitionNamesPsRequest.setCatName(parsedDbName[CAT_NAME]);
      getPartitionNamesPsRequest.setPartValues(part_vals);
      getPartitionNamesPsRequest.setMaxParts(max_parts);
      ret = get_partition_names_ps_req(getPartitionNamesPsRequest).getNames();
    } catch (Exception e) {
      ex = e;
      rethrowException(e);
    } finally {
      endFunction("get_partitions_names_ps", ret != null, ex, tbl_name);
    }
    return ret;
  }

  @Override
  public GetPartitionNamesPsResponse get_partition_names_ps_req(GetPartitionNamesPsRequest req)
      throws MetaException, NoSuchObjectException, TException {
    if (req.getPartValues() == null) {
      throw new MetaException("The partValues in GetPartitionNamesPsRequest is null");
    }
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    String dbName = req.getDbName(), tblName = req.getTblName();
    TableName tableName = new TableName(catName, dbName, tblName);
    GetPartitionsPsWithAuthRequest gpar = new GetPartitionsPsWithAuthRequest(tableName.getDb(), tableName.getTable());
    gpar.setMaxParts(req.getMaxParts());
    gpar.setPartVals(req.getPartValues());
    List<String> names = GetPartitionsHandler.getPartitionNames(
        t -> startTableFunction("get_partition_names_ps_req", catName, dbName, tblName),
        rex -> endFunction("get_partition_names_ps_req",
            rex.getLeft() != null && rex.getLeft().success(), rex.getRight(), tableName.toString()),
        this, tableName, gpar).result();
    GetPartitionNamesPsResponse res = new GetPartitionNamesPsResponse();
    res.setNames(names);
    return res;
  }

  @Override
  public List<String> get_partition_names_req(PartitionsByExprRequest req)
      throws MetaException, NoSuchObjectException, TException {
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    String dbName = req.getDbName(), tblName = req.getTblName();
    TableName tableName = new TableName(catName, dbName, tblName);
    return GetPartitionsHandler.getPartitionNames(
        t -> startTableFunction("get_partition_names_req", catName, dbName, tblName),
        rex -> endFunction("get_partition_names_req",
            rex.getLeft() != null && rex.getLeft().success(), rex.getRight(), tableName.toString()),
        this, tableName, req).result();
  }

  @Override
  @Deprecated
  public ColumnStatistics get_table_column_statistics(String dbName, String tableName,
                                                      String colName) throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    parsedDbName[CAT_NAME] = parsedDbName[CAT_NAME].toLowerCase();
    parsedDbName[DB_NAME] = parsedDbName[DB_NAME].toLowerCase();
    tableName = tableName.toLowerCase();
    colName = colName.toLowerCase();
    startFunction("get_column_statistics_by_table", ": table=" +
        TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tableName) + " column=" + colName);
    ColumnStatistics statsObj = null;
    try {
      statsObj = getMS().getTableColumnStatistics(
          parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, Lists.newArrayList(colName),
          "hive", null);
      if (statsObj != null) {
        assert statsObj.getStatsObjSize() <= 1;
      }
      return statsObj;
    } finally {
      endFunction("get_column_statistics_by_table", statsObj != null, null, tableName);
    }
  }

  @Override
  public TableStatsResult get_table_statistics_req(TableStatsRequest request) throws TException {
    String catName = request.isSetCatName() ? request.getCatName().toLowerCase() :
        getDefaultCatalog(conf);
    String dbName = request.getDbName().toLowerCase();
    String tblName = request.getTblName().toLowerCase();
    startFunction("get_table_statistics_req", ": table=" +
        TableName.getQualified(catName, dbName, tblName));
    TableStatsResult result = null;
    List<String> lowerCaseColNames = new ArrayList<>(request.getColNames().size());
    for (String colName : request.getColNames()) {
      lowerCaseColNames.add(colName.toLowerCase());
    }
    try {
      ColumnStatistics cs = getMS().getTableColumnStatistics(
          catName, dbName, tblName, lowerCaseColNames,
          request.getEngine(), request.getValidWriteIdList());
      // Note: stats compliance is not propagated to the client; instead, we just return nothing
      //       if stats are not compliant for now. This won't work for stats merging, but that
      //       is currently only done on metastore size (see set_aggr...).
      //       For some optimizations we might make use of incorrect stats that are "better than
      //       nothing", so this may change in future.
      result = new TableStatsResult((cs == null || cs.getStatsObj() == null
          || (cs.isSetIsStatsCompliant() && !cs.isIsStatsCompliant()))
          ? Lists.newArrayList() : cs.getStatsObj());
    } finally {
      endFunction("get_table_statistics_req", result == null, null, tblName);
    }
    return result;
  }

  @Override
  @Deprecated
  public ColumnStatistics get_partition_column_statistics(String dbName, String tableName,
                                                          String partName, String colName) throws TException {
    // Note: this method appears to be unused within Hive.
    //       It doesn't take txn stats into account.
    dbName = dbName.toLowerCase();
    String[] parsedDbName = parseDbName(dbName, conf);
    tableName = tableName.toLowerCase();
    colName = colName.toLowerCase();
    startFunction("get_column_statistics_by_partition", ": table=" +
        TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tableName) + " partition=" + partName + " column=" + colName);
    ColumnStatistics statsObj = null;

    try {
      List<ColumnStatistics> list = getMS().getPartitionColumnStatistics(
          parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName,
          Lists.newArrayList(partName), Lists.newArrayList(colName),
          "hive");
      if (list.isEmpty()) {
        return null;
      }
      if (list.size() != 1) {
        throw new MetaException(list.size() + " statistics for single column and partition");
      }
      statsObj = list.get(0);
    } finally {
      endFunction("get_column_statistics_by_partition", statsObj != null, null, tableName);
    }
    return statsObj;
  }

  @Override
  public PartitionsStatsResult get_partitions_statistics_req(PartitionsStatsRequest request)
      throws TException {
    String catName = request.isSetCatName() ? request.getCatName().toLowerCase() : getDefaultCatalog(conf);
    String dbName = request.getDbName().toLowerCase();
    String tblName = request.getTblName().toLowerCase();
    startPartitionFunction("get_partitions_statistics_req", catName, dbName, tblName, request.getPartNames());

    PartitionsStatsResult result = null;
    List<String> lowerCaseColNames = new ArrayList<>(request.getColNames().size());
    for (String colName : request.getColNames()) {
      lowerCaseColNames.add(colName.toLowerCase());
    }
    try {
      List<ColumnStatistics> stats = getMS().getPartitionColumnStatistics(
          catName, dbName, tblName, request.getPartNames(), lowerCaseColNames,
          request.getEngine(), request.isSetValidWriteIdList() ? request.getValidWriteIdList() : null);
      Map<String, List<ColumnStatisticsObj>> map = new HashMap<>();
      if (stats != null) {
        for (ColumnStatistics stat : stats) {
          // Note: stats compliance is not propagated to the client; instead, we just return nothing
          //       if stats are not compliant for now. This won't work for stats merging, but that
          //       is currently only done on metastore size (see set_aggr...).
          //       For some optimizations we might make use of incorrect stats that are "better than
          //       nothing", so this may change in future.
          if (stat.isSetIsStatsCompliant() && !stat.isIsStatsCompliant()) {
            continue;
          }
          map.put(stat.getStatsDesc().getPartName(), stat.getStatsObj());
        }
      }
      result = new PartitionsStatsResult(map);
    } finally {
      endFunction("get_partitions_statistics_req", result == null, null, tblName);
    }
    return result;
  }

  @Override
  public boolean update_table_column_statistics(ColumnStatistics colStats) throws TException {
    // Deprecated API, won't work for transactional tables
    colStats.getStatsDesc().setIsTblLevel(true);
    SetPartitionsStatsRequest setStatsRequest =
        new SetPartitionsStatsRequest(List.of(colStats));
    setStatsRequest.setWriteId(-1);
    setStatsRequest.setValidWriteIdList(null);
    setStatsRequest.setNeedMerge(false);
    return set_aggr_stats_for(setStatsRequest);
  }

  @Override
  public SetPartitionsStatsResponse update_table_column_statistics_req(
      SetPartitionsStatsRequest req) throws NoSuchObjectException,
      InvalidObjectException, MetaException, InvalidInputException,
      TException {
    if (req.getColStatsSize() != 1) {
      throw new InvalidInputException("Only one stats object expected");
    }
    if (req.isNeedMerge()) {
      throw new InvalidInputException("Merge is not supported for non-aggregate stats");
    }
    req.getColStats().getFirst().getStatsDesc().setIsTblLevel(true);
    SetPartitionsStatsRequest setStatsRequest = new SetPartitionsStatsRequest(req.getColStats());
    setStatsRequest.setWriteId(req.getWriteId());
    setStatsRequest.setValidWriteIdList(req.getValidWriteIdList());
    setStatsRequest.setNeedMerge(false);
    setStatsRequest.setEngine(req.getEngine());
    boolean ret = set_aggr_stats_for(setStatsRequest);
    return new SetPartitionsStatsResponse(ret);
  }

  @Override
  public boolean update_partition_column_statistics(ColumnStatistics colStats) throws TException {
    // Deprecated API.
    SetPartitionsStatsRequest setStatsRequest =
        new SetPartitionsStatsRequest(Arrays.asList(colStats));
    setStatsRequest.setWriteId(-1);
    setStatsRequest.setValidWriteIdList(null);
    setStatsRequest.setNeedMerge(false);
    return set_aggr_stats_for(setStatsRequest);
  }

  @Override
  public SetPartitionsStatsResponse update_partition_column_statistics_req(
      SetPartitionsStatsRequest req) throws NoSuchObjectException,
      InvalidObjectException, MetaException, InvalidInputException,
      TException {
    if (req.getColStatsSize() != 1) {
      throw new InvalidInputException("Only one stats object expected");
    }
    if (req.isNeedMerge()) {
      throw new InvalidInputException("Merge is not supported for non-aggregate stats");
    }
    SetPartitionsStatsRequest setStatsRequest = new SetPartitionsStatsRequest(req.getColStats());
    setStatsRequest.setWriteId(req.getWriteId());
    setStatsRequest.setValidWriteIdList(req.getValidWriteIdList());
    setStatsRequest.setNeedMerge(false);
    setStatsRequest.setEngine(req.getEngine());
    boolean ret = set_aggr_stats_for(setStatsRequest);
    return new SetPartitionsStatsResponse(ret);
  }

  @Deprecated
  @Override
  public boolean delete_partition_column_statistics(String dbName, String tableName,
                                                    String partName, String colName, String engine) throws TException {
    dbName = dbName.toLowerCase();
    String[] parsedDbName = parseDbName(dbName, conf);
    tableName = tableName.toLowerCase();
    DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(parsedDbName[DB_NAME], tableName);
    if (colName != null) {
      colName = colName.toLowerCase();
      request.addToCol_names(colName);
    }

    request.setEngine(engine);
    request.setCat_name(parsedDbName[CAT_NAME]);
    if (partName != null) {
      request.addToPart_names(partName);
    }
    return delete_column_statistics_req(request);
  }

  @Override
  public boolean delete_column_statistics_req(DeleteColumnStatisticsRequest req) throws TException {
    String dbName = normalizeIdentifier(req.getDb_name());
    String tableName = normalizeIdentifier(req.getTbl_name());
    List<String> colNames = req.getCol_names();
    String engine = req.getEngine();
    String[] parsedDbName = parseDbName(dbName, conf);
    if (req.getCat_name() != null) {
      parsedDbName[CAT_NAME] = normalizeIdentifier(req.getCat_name());
    }
    startFunction("delete_column_statistics_req", ": table=" +
        TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName) +
        " partitions=" + req.getPart_names() + " column=" + colNames + " engine=" + engine);
    boolean ret = false, committed = false;
    List<ListenerEvent> events = new ArrayList<>();
    EventType eventType = null;
    final RawStore rawStore = getMS();
    rawStore.openTransaction();
    try {
      Table table = rawStore.getTable(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName);
      boolean isPartitioned = table.getPartitionKeysSize() > 0;
      if (TxnUtils.isTransactionalTable(table)) {
        throw new MetaException("Cannot delete stats via this API for a transactional table");
      }
      if (!isPartitioned || req.isTableLevel()) {
        ret = rawStore.deleteTableColumnStatistics(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, colNames, engine);
        if (ret) {
          eventType = EventType.DELETE_TABLE_COLUMN_STAT;
          for (String colName : colNames == null || colNames.isEmpty() ?
              table.getSd().getCols().stream().map(FieldSchema::getName).toList() : colNames) {
            if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners, eventType,
                  new DeleteTableColumnStatEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, colName, engine, this));
            }
            events.add(new DeleteTableColumnStatEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, colName, engine, this));
          }
        }
      } else {
        List<String> partNames = new ArrayList<>();
        if (req.getPart_namesSize() > 0) {
          partNames.addAll(req.getPart_names());
        } else {
          partNames.addAll(rawStore.listPartitionNames(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, (short) -1));
        }
        if (partNames.isEmpty()) {
          // no partition found, bail out early
          return true;
        }
        ret = rawStore.deletePartitionColumnStatistics(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName,
                partNames, colNames, engine);
        if (ret) {
          eventType = EventType.DELETE_PARTITION_COLUMN_STAT;
          for (String colName : colNames == null || colNames.isEmpty() ?
              table.getSd().getCols().stream().map(FieldSchema::getName).toList() : colNames) {
            for (String partName : partNames) {
              List<String> partVals = getPartValsFromName(table, partName);
              if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners, eventType,
                    new DeletePartitionColumnStatEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName,
                        partName, partVals, colName, engine, this));
              }
              events.add(new DeletePartitionColumnStatEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName,
                  partName, partVals, colName, engine, this));
            }
          }
        }
      }
      committed = rawStore.commitTransaction();
    } finally {
      if (!committed) {
        rawStore.rollbackTransaction();
      }
      if (!listeners.isEmpty()) {
        for (ListenerEvent event : events) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners, eventType, event);
        }
      }
      endFunction("delete_column_statistics_req", ret, null, tableName);
    }
    return ret;
  }

  @Deprecated
  @Override
  public boolean delete_table_column_statistics(String dbName, String tableName, String colName, String engine)
      throws TException {
    dbName = dbName.toLowerCase();
    tableName = tableName.toLowerCase();
    String[] parsedDbName = parseDbName(dbName, conf);
    DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(parsedDbName[DB_NAME], tableName);
    if (colName != null) {
      colName = colName.toLowerCase();
      request.addToCol_names(colName);
    }
    request.setEngine(engine);
    request.setCat_name(parsedDbName[CAT_NAME]);
    request.setTableLevel(true);
    return delete_column_statistics_req(request);
  }

  @Override
  @Deprecated
  public List<Partition> get_partitions_by_filter(final String dbName, final String tblName,
                                                  final String filter, final short maxParts)
      throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    GetPartitionsByFilterRequest gfr =
        new GetPartitionsByFilterRequest(parsedDbName[DB_NAME], tblName, filter);
    gfr.setMaxParts(maxParts);
    gfr.setCatName(parsedDbName[CAT_NAME]);
    return get_partitions_by_filter_req(gfr);
  }

  @Override
  public List<Partition> get_partitions_by_filter_req(GetPartitionsByFilterRequest req) throws TException {
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    TableName tableName = new TableName(catName, req.getDbName(), req.getTblName());
    return GetPartitionsHandler.getPartitionsResult(
        t -> startTableFunction("get_partitions_by_filter", catName, t.getDb(), t.getTable()),
        rex -> endFunction("get_partitions_by_filter",
            rex.getLeft() != null && rex.getLeft().success(), rex.getRight(), tableName.toString()),
        this, tableName, req).result();
  }

  @Override
  @Deprecated
  public List<PartitionSpec> get_part_specs_by_filter(final String dbName, final String tblName,
                                                      final String filter, final int maxParts)
      throws TException {

    String[] parsedDbName = parseDbName(dbName, conf);
    startPartitionFunction("get_partitions_by_filter_pspec", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName,
            maxParts, filter);
    List<PartitionSpec> partitionSpecs = null;
    try {
      GetTableRequest getTableRequest = new GetTableRequest(parsedDbName[DB_NAME], tblName);
      getTableRequest.setCatName(parsedDbName[CAT_NAME]);
      Table table = get_table_core(getTableRequest);
      // Don't pass the parsed db name, as get_partitions_by_filter will parse it itself
      List<Partition> partitions = get_partitions_by_filter(dbName, tblName, filter, (short) maxParts);

      if (is_partition_spec_grouping_enabled(table)) {
        partitionSpecs = MetaStoreServerUtils
            .getPartitionspecsGroupedByStorageDescriptor(table, partitions);
      }
      else {
        PartitionSpec pSpec = new PartitionSpec();
        pSpec.setPartitionList(new PartitionListComposingSpec(partitions));
        pSpec.setRootPath(table.getSd().getLocation());
        pSpec.setCatName(parsedDbName[CAT_NAME]);
        pSpec.setDbName(parsedDbName[DB_NAME]);
        pSpec.setTableName(tblName);
        partitionSpecs = Arrays.asList(pSpec);
      }

      return partitionSpecs;
    }
    finally {
      endFunction("get_partitions_by_filter_pspec", partitionSpecs != null && !partitionSpecs.isEmpty(), null, tblName);
    }
  }

  @Override
  public PartitionsSpecByExprResult get_partitions_spec_by_expr(
      PartitionsByExprRequest req) throws TException {
    String dbName = req.getDbName(), tblName = req.getTblName();
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    TableName tableName = new TableName(catName, dbName, tblName);
    GetPartitionsHandler.GetPartitionsResult<Partition> result =
        GetPartitionsHandler.getPartitionsResult(
            t -> startTableFunction("get_partitions_spec_by_expr", catName, dbName, req.getTblName()),
            rex ->  endFunction("get_partitions_spec_by_expr",
                rex.getLeft() != null && rex.getLeft().success(), rex.getRight(),
                TableName.getQualified(catName, req.getDbName(), req.getTblName())),
            this, tableName, req);
    GetTableRequest getTableRequest = new GetTableRequest(dbName, tblName);
    getTableRequest.setCatName(catName);
    Table table = get_table_core(getTableRequest);
    List<PartitionSpec> partitionSpecs =
        MetaStoreServerUtils.getPartitionspecsGroupedByStorageDescriptor(table, result.result());
    return new PartitionsSpecByExprResult(partitionSpecs, result.hasUnknownPartitions());
  }

  @Override
  public PartitionsByExprResult get_partitions_by_expr(
      PartitionsByExprRequest req) throws TException {
    String dbName = req.getDbName(), tblName = req.getTblName();
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    TableName tableName = new TableName(catName, dbName, tblName);
    GetPartitionsHandler.GetPartitionsResult<Partition> result =
        GetPartitionsHandler.getPartitionsResult(
            t -> startTableFunction("get_partitions_by_expr", catName, dbName, req.getTblName()),
            rex ->  endFunction("get_partitions_by_expr",
                rex.getLeft() != null && rex.getLeft().success(), rex.getRight(),
                TableName.getQualified(catName, req.getDbName(), req.getTblName())),
            this, tableName, req);
    return new PartitionsByExprResult(result.result(), result.hasUnknownPartitions());
  }

  @Override
  @Deprecated
  public int get_num_partitions_by_filter(final String dbName,
                                          final String tblName, final String filter)
      throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    if (parsedDbName[DB_NAME] == null || tblName == null) {
      throw new MetaException("The DB and table name cannot be null.");
    }
    startFunction("get_num_partitions_by_filter",
        " : tbl=" + TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName) + " Filter="
            + filter);

    int ret = -1;
    Exception ex = null;
    try {
      ret = getMS().getNumPartitionsByFilter(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tblName, filter);
    } catch (Exception e) {
      ex = e;
      rethrowException(e);
    } finally {
      endFunction("get_num_partitions_by_filter", ret != -1, ex, tblName);
    }
    return ret;
  }

  @Override
  @Deprecated
  public List<Partition> get_partitions_by_names(final String dbName, final String tblName,
                                                 final List<String> partNames)
      throws TException {
    if (partNames == null) {
      throw new MetaException("The partNames is null");
    }
    GetPartitionsByNamesRequest request = new GetPartitionsByNamesRequest(dbName, tblName);
    request.setNames(partNames);
    return get_partitions_by_names_req(request).getPartitions();
  }

  @Override
  public GetPartitionsByNamesResult get_partitions_by_names_req(GetPartitionsByNamesRequest gpbnr)
      throws TException {
    if (gpbnr.getNames() == null) {
      throw new MetaException("The names in GetPartitionsByNamesRequest is null");
    }
    String[] dbNameParts = parseDbName(gpbnr.getDb_name(), conf);
    TableName tableName = new TableName(dbNameParts[CAT_NAME], dbNameParts[DB_NAME], gpbnr.getTbl_name());
    List<Partition> partitions = GetPartitionsHandler.getPartitionsResult(
        t ->  startTableFunction("get_partitions_by_names", tableName.getCat(), tableName.getDb(), tableName.getTable()),
        rex ->  endFunction("get_partitions_by_names",
            rex.getLeft() != null && rex.getLeft().success(), rex.getRight(), tableName.toString()),
        this, tableName, gpbnr).result();
    return new GetPartitionsByNamesResult(partitions);
  }

  /**
   * Creates an instance of property manager based on the (declared) namespace.
   * @param ns the namespace
   * @return the manager instance
   * @throws TException
   */
  private PropertyManager getPropertyManager(String ns) throws MetaException, NoSuchObjectException {
    PropertyStore propertyStore = getMS().getPropertyStore();
    PropertyManager mgr = PropertyManager.create(ns, propertyStore);
    return mgr;
  }
  @Override
  public PropertyGetResponse get_properties(PropertyGetRequest req) throws TException {
    try {
      PropertyManager mgr = getPropertyManager(req.getNameSpace());
      Map<String, PropertyMap> selected = mgr.selectProperties(req.getMapPrefix(), req.getMapPredicate(), req.getMapSelection());
      PropertyGetResponse response = new PropertyGetResponse();
      Map<String, Map<String, String>> returned = new TreeMap<>();
      selected.forEach((k, v) -> {
        returned.put(k, v.export());
      });
      response.setProperties(returned);
      return response;
    } catch(PropertyException exception) {
      throw ExceptionHandler.newMetaException(exception);
    }
  }

  @Override
  public boolean set_properties(PropertySetRequest req) throws TException {
    try {
      PropertyManager mgr = getPropertyManager(req.getNameSpace());
      mgr.setProperties((Map<String, Object>) (Map<?, ?>) req.getPropertyMap());
      mgr.commit();
      return true;
    } catch(PropertyException exception) {
      throw ExceptionHandler.newMetaException(exception);
    }
  }

  @Override
  public void markPartitionForEvent(final String db_name, final String tbl_name,
                                    final Map<String, String> partName, final PartitionEventType evtType) throws TException {

    Table tbl = null;
    Exception ex = null;
    RawStore ms  = getMS();
    boolean success = false;
    try {
      String[] parsedDbName = parseDbName(db_name, conf);
      ms.openTransaction();
      startPartitionFunction("markPartitionForEvent", parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tbl_name, partName);
      firePreEvent(new PreLoadPartitionDoneEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tbl_name, partName, this));
      tbl = ms.markPartitionForEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name,
          partName, evtType);
      if (null == tbl) {
        throw new UnknownTableException("Table: " + tbl_name + " not found.");
      }

      if (transactionalListeners.size() > 0) {
        LoadPartitionDoneEvent lpde = new LoadPartitionDoneEvent(true, tbl, partName, this);
        for (MetaStoreEventListener transactionalListener : transactionalListeners) {
          transactionalListener.onLoadPartitionDone(lpde);
        }
      }

      success = ms.commitTransaction();
      for (MetaStoreEventListener listener : listeners) {
        listener.onLoadPartitionDone(new LoadPartitionDoneEvent(true, tbl, partName, this));
      }
    } catch (Exception original) {
      ex = original;
      LOG.error("Exception caught in mark partition event ", original);
      throw handleException(original)
          .throwIfInstance(UnknownTableException.class, InvalidPartitionException.class, MetaException.class)
          .defaultMetaException();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }

      endFunction("markPartitionForEvent", tbl != null, ex, tbl_name);
    }
  }

  @Override
  public boolean isPartitionMarkedForEvent(final String db_name, final String tbl_name,
                                           final Map<String, String> partName, final PartitionEventType evtType) throws TException {

    String[] parsedDbName = parseDbName(db_name, conf);
    startPartitionFunction("isPartitionMarkedForEvent", parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
        tbl_name, partName);
    Boolean ret = null;
    Exception ex = null;
    try {
      ret = getMS().isPartitionMarkedForEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tbl_name, partName, evtType);
    } catch (Exception original) {
      LOG.error("Exception caught for isPartitionMarkedForEvent ", original);
      ex = original;
      throw handleException(original).throwIfInstance(UnknownTableException.class, InvalidPartitionException.class)
          .throwIfInstance(UnknownPartitionException.class,  MetaException.class)
          .defaultMetaException();
    } finally {
      endFunction("isPartitionMarkedForEvent", ret != null, ex, tbl_name);
    }

    return ret;
  }

  private void validateFunctionInfo(Function func) throws InvalidObjectException, MetaException {
    if (func == null) {
      throw new MetaException("Function cannot be null.");
    }
    if (func.getFunctionName() == null) {
      throw new MetaException("Function name cannot be null.");
    }
    if (func.getDbName() == null) {
      throw new MetaException("Database name in Function cannot be null.");
    }
    if (!MetaStoreUtils.validateName(func.getFunctionName(), null)) {
      throw new InvalidObjectException(func.getFunctionName() + " is not a valid object name");
    }
    String className = func.getClassName();
    if (className == null) {
      throw new InvalidObjectException("Function class name cannot be null");
    }
    if (func.getOwnerType() == null) {
      throw new MetaException("Function owner type cannot be null.");
    }
    if (func.getFunctionType() == null) {
      throw new MetaException("Function type cannot be null.");
    }
  }

  @Override
  public void create_function(Function func) throws TException {
    validateFunctionInfo(func);
    boolean success = false;
    RawStore ms = getMS();
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    try {
      String catName = func.isSetCatName() ? func.getCatName() : getDefaultCatalog(conf);
      if (!func.isSetOwnerName()) {
        try {
          func.setOwnerName(SecurityUtils.getUGI().getShortUserName());
        } catch (Exception ex) {
          LOG.error("Cannot obtain username from the session to create a function", ex);
          throw new TException(ex);
        }
      }
      ms.openTransaction();
      Database db = ms.getDatabase(catName, func.getDbName());
      if (db == null) {
        throw new NoSuchObjectException("The database " + func.getDbName() + " does not exist");
      }

      if (MetaStoreUtils.isDatabaseRemote(db)) {
        throw new MetaException("Operation create_function not support for REMOTE database");
      }

      Function existingFunc = ms.getFunction(catName, func.getDbName(), func.getFunctionName());
      if (existingFunc != null) {
        throw new AlreadyExistsException(
            "Function " + func.getFunctionName() + " already exists");
      }
      firePreEvent(new PreCreateFunctionEvent(func, this));
      long time = System.currentTimeMillis() / 1000;
      func.setCreateTime((int) time);
      ms.createFunction(func);
      if (!transactionalListeners.isEmpty()) {
        transactionalListenerResponses =
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                EventType.CREATE_FUNCTION,
                new CreateFunctionEvent(func, true, this));
      }

      success = ms.commitTransaction();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }

      if (!listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners,
            EventType.CREATE_FUNCTION,
            new CreateFunctionEvent(func, success, this),
            null,
            transactionalListenerResponses, ms);
      }
    }
  }

  @Override
  public void drop_function(String dbName, String funcName)
      throws NoSuchObjectException, MetaException,
      InvalidObjectException, InvalidInputException {
    if (funcName == null) {
      throw new MetaException("Function name cannot be null.");
    }
    boolean success = false;
    Function func = null;
    RawStore ms = getMS();
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    String[] parsedDbName = parseDbName(dbName, conf);
    if (parsedDbName[DB_NAME] == null) {
      throw new MetaException("Database name cannot be null.");
    }
    try {
      ms.openTransaction();
      func = ms.getFunction(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], funcName);
      if (func == null) {
        throw new NoSuchObjectException("Function " + funcName + " does not exist");
      }
      Boolean needsCm =
          ReplChangeManager.isSourceOfReplication(get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]));

      // if copy of jar to change management fails we fail the metastore transaction, since the
      // user might delete the jars on HDFS externally after dropping the function, hence having
      // a copy is required to allow incremental replication to work correctly.
      if (func.getResourceUris() != null && !func.getResourceUris().isEmpty()) {
        for (ResourceUri uri : func.getResourceUris()) {
          if (uri.getUri().toLowerCase().startsWith("hdfs:") && needsCm) {
            wh.addToChangeManagement(new Path(uri.getUri()));
          }
        }
      }
      firePreEvent(new PreDropFunctionEvent(func, this));

      // if the operation on metastore fails, we don't do anything in change management, but fail
      // the metastore transaction, as having a copy of the jar in change management is not going
      // to cause any problem, the cleaner thread will remove this when this jar expires.
      ms.dropFunction(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], funcName);
      if (transactionalListeners.size() > 0) {
        transactionalListenerResponses =
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                EventType.DROP_FUNCTION,
                new DropFunctionEvent(func, true, this));
      }
      success = ms.commitTransaction();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }

      if (listeners.size() > 0) {
        MetaStoreListenerNotifier.notifyEvent(listeners,
            EventType.DROP_FUNCTION,
            new DropFunctionEvent(func, success, this),
            null,
            transactionalListenerResponses, ms);
      }
    }
  }

  @Override
  public void alter_function(String dbName, String funcName, Function newFunc) throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    validateForAlterFunction(parsedDbName[DB_NAME], funcName, newFunc);
    boolean success = false;
    RawStore ms = getMS();
    try {
      firePreEvent(new PreCreateFunctionEvent(newFunc, this));
      ms.openTransaction();
      ms.alterFunction(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], funcName, newFunc);
      success = ms.commitTransaction();
    } catch (InvalidObjectException e) {
      // Throwing MetaException instead of InvalidObjectException as the InvalidObjectException
      // is not defined for the alter_function method in the Thrift interface.
      throwMetaException(e);
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }
    }
  }

  private void validateForAlterFunction(String dbName, String funcName, Function newFunc)
      throws MetaException {
    if (dbName == null || funcName == null) {
      throw new MetaException("Database and function name cannot be null.");
    }
    try {
      validateFunctionInfo(newFunc);
    } catch (InvalidObjectException e) {
      // The validateFunctionInfo method is used by the create and alter function methods as well
      // and it can throw InvalidObjectException. But the InvalidObjectException is not defined
      // for the alter_function method in the Thrift interface, therefore a TApplicationException
      // will occur at the caller side. Re-throwing the InvalidObjectException as MetaException
      // would eliminate the TApplicationException at caller side.
      throw newMetaException(e);
    }
  }

  @Override
  public List<String> get_functions(String dbName, String pattern)
      throws MetaException {
    String[] parsedDbName = parseDbName(dbName, conf);
    GetFunctionsRequest request = new GetFunctionsRequest(parsedDbName[DB_NAME]);
    request.setCatalogName(parsedDbName[CAT_NAME]);
    request.setPattern(pattern);
    request.setReturnNames(true);
    GetFunctionsResponse resp = get_functions_req(request);
    return resp.getFunction_names();
  }

  @Override
  public GetFunctionsResponse get_functions_req(GetFunctionsRequest req)
      throws MetaException {
    startFunction("get_functions_req", ": db=" + req.getDbName()
        + " pat=" + req.getPattern());

    RawStore ms = getMS();
    Exception ex = null;
    GetFunctionsResponse response = new GetFunctionsResponse();
    String catName = req.isSetCatalogName() ? req.getCatalogName() : getDefaultCatalog(conf);
    try {
      List result = ms.getFunctionsRequest(catName, req.getDbName(),
          req.getPattern(), req.isReturnNames());
      if (req.isReturnNames()) {
        response.setFunction_names(result);
      } else {
        response.setFunctions(result);
      }
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_functions", ex == null, ex);
    }
    return response;
  }

  @Override
  public GetAllFunctionsResponse get_all_functions()
      throws MetaException {
    GetAllFunctionsResponse response = new GetAllFunctionsResponse();
    startFunction("get_all_functions");
    RawStore ms = getMS();
    List<Function> allFunctions = null;
    Exception ex = null;
    try {
      // Leaving this as the 'hive' catalog (rather than choosing the default from the
      // configuration) because all the default UDFs are in that catalog, and I think that's
      // would people really want here.
      allFunctions = ms.getAllFunctions(DEFAULT_CATALOG_NAME);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_all_functions", allFunctions != null, ex);
    }
    response.setFunctions(allFunctions);
    return response;
  }

  @Override
  public Function get_function(String dbName, String funcName) throws TException {
    if (dbName == null || funcName == null) {
      throw new MetaException("Database and function name cannot be null.");
    }
    startFunction("get_function", ": " + dbName + "." + funcName);

    RawStore ms = getMS();
    Function func = null;
    Exception ex = null;
    String[] parsedDbName = parseDbName(dbName, conf);

    try {
      func = ms.getFunction(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], funcName);
      if (func == null) {
        throw new NoSuchObjectException(
            "Function " + dbName + "." + funcName + " does not exist");
      }
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(NoSuchObjectException.class).defaultMetaException();
    } finally {
      endFunction("get_function", func != null, ex);
    }

    return func;
  }

  @Override
  public void update_table_params(List<TableParamsUpdate> updates) throws TException {
    getMS().updateTableParams(updates);
  }

  public AggrStats get_aggr_stats_for(PartitionsStatsRequest request) throws TException {
    String catName = request.isSetCatName() ? request.getCatName().toLowerCase() :
        getDefaultCatalog(conf);
    String dbName = request.getDbName().toLowerCase();
    String tblName = request.getTblName().toLowerCase();
    startFunction("get_aggr_stats_for", ": table=" +
        TableName.getQualified(catName, dbName, tblName));

    List<String> lowerCaseColNames = new ArrayList<>(request.getColNames().size());
    for (String colName : request.getColNames()) {
      lowerCaseColNames.add(colName.toLowerCase());
    }
    AggrStats aggrStats = null;

    try {
      aggrStats = getMS().get_aggr_stats_for(catName, dbName, tblName,
          request.getPartNames(), lowerCaseColNames, request.getEngine(), request.getValidWriteIdList());
      return aggrStats;
    } finally {
      endFunction("get_aggr_stats_for", aggrStats == null, null, request.getTblName());
    }

  }

  @Override
  public boolean set_aggr_stats_for(SetPartitionsStatsRequest req) throws TException {
    List<ColumnStatistics> columnStatisticsList = req.getColStats();
    if (columnStatisticsList == null || columnStatisticsList.isEmpty()) {
      return true;
    }
    ColumnStatisticsDesc statsDesc = columnStatisticsList.getFirst().getStatsDesc();
    String catName = statsDesc.isSetCatName() ? statsDesc.getCatName() : getDefaultCatalog(conf);
    String dbName = statsDesc.getDbName();
    String tableName = statsDesc.getTableName();
    startFunction("set_aggr_stats_for",
        ": db=" + dbName + " tab=" + tableName + " needMerge=" + req.isNeedMerge() +
            " isTblLevel=" + statsDesc.isIsTblLevel());
    Exception ex = null;
    boolean success = false;
    try {
      success = AbstractRequestHandler.offer(this, req).success();
      return success;
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, NoSuchObjectException.class)
          .convertIfInstance(IOException.class, MetaException.class)
          .defaultMetaException();
    } finally {
      endFunction("set_aggr_stats_for", success, ex,
          TableName.getQualified(catName, dbName, tableName));
    }
  }

  @Override
  public FireEventResponse fire_listener_event(FireEventRequest rqst) throws TException {
    switch (rqst.getData().getSetField()) {
    case INSERT_DATA:
    case INSERT_DATAS:
      String catName =
          rqst.isSetCatName() ? rqst.getCatName() : getDefaultCatalog(conf);
      String dbName = rqst.getDbName();
      String tblName = rqst.getTableName();
      boolean isSuccessful = rqst.isSuccessful();
      List<InsertEvent> events = new ArrayList<>();
      if (rqst.getData().isSetInsertData()) {
        events.add(new InsertEvent(catName, dbName, tblName,
            rqst.getPartitionVals(),
            rqst.getData().getInsertData(), isSuccessful, this));
      } else {
        // this is a bulk fire insert event operation
        // we use the partition values field from the InsertEventRequestData object
        // instead of the FireEventRequest object
        for (InsertEventRequestData insertData : rqst.getData().getInsertDatas()) {
          if (!insertData.isSetPartitionVal()) {
            throw new MetaException(
                "Partition values must be set when firing multiple insert events");
          }
          events.add(new InsertEvent(catName, dbName, tblName,
              insertData.getPartitionVal(),
              insertData, isSuccessful, this));
        }
      }
      FireEventResponse response = new FireEventResponse();
      for (InsertEvent event : events) {
        /*
         * The transactional listener response will be set already on the event, so there is not need
         * to pass the response to the non-transactional listener.
         */
        MetaStoreListenerNotifier
            .notifyEvent(transactionalListeners, EventType.INSERT, event);
        MetaStoreListenerNotifier.notifyEvent(listeners, EventType.INSERT, event);
        if (event.getParameters() != null && event.getParameters()
            .containsKey(
                MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME)) {
          response.addToEventIds(Long.valueOf(event.getParameters()
              .get(MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME)));
        } else {
          String msg = "Insert event id not generated for ";
          if (event.getPartitionObj() != null) {
            msg += "partition " + Arrays
                .toString(event.getPartitionObj().getValues().toArray()) + " of ";
          }
          msg +=
              " of table " + event.getTableObj().getDbName() + "." + event.getTableObj()
                  .getTableName();
          LOG.warn(msg);
        }
      }
      return response;
    case REFRESH_EVENT:
      response = new FireEventResponse();
      catName = rqst.isSetCatName() ? rqst.getCatName() : getDefaultCatalog(conf);
      dbName = rqst.getDbName();
      tblName = rqst.getTableName();
      List<List<String>> partitionVals;
      if (rqst.getPartitionVals() != null && !rqst.getPartitionVals().isEmpty()) {
        partitionVals = Arrays.asList(rqst.getPartitionVals());
      } else {
        partitionVals = rqst.getBatchPartitionValsForRefresh();
      }
      Map<String, String> tableParams = rqst.getTblParams();
      ReloadEvent event = new ReloadEvent(catName, dbName, tblName, partitionVals, rqst.isSuccessful(),
              rqst.getData().getRefreshEvent(), tableParams, this);
      MetaStoreListenerNotifier
              .notifyEvent(transactionalListeners, EventType.RELOAD, event);
      MetaStoreListenerNotifier.notifyEvent(listeners, EventType.RELOAD, event);
      if (event.getParameters() != null && event.getParameters()
              .containsKey(
                      MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME)) {
        response.addToEventIds(Long.valueOf(event.getParameters()
                .get(MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME)));
      } else {
        String msg = "Reload event id not generated for ";
        if (event.getPartitions() != null) {
          msg += "partition(s) " + Arrays
                  .toString(event.getPartitions().toArray()) + " of ";
        }
        msg +=
                " of table " + event.getTableObj().getDbName() + "." + event.getTableObj()
                        .getTableName();
        LOG.warn(msg);
      }
      return response;
    default:
      throw new TException("Event type " + rqst.getData().getSetField().toString()
          + " not currently supported.");
    }

  }

  @Override
  public GetFileMetadataByExprResult get_file_metadata_by_expr(GetFileMetadataByExprRequest req)
      throws TException {
    GetFileMetadataByExprResult result = new GetFileMetadataByExprResult();
    RawStore ms = getMS();
    if (!ms.isFileMetadataSupported()) {
      result.setIsSupported(false);
      result.setMetadata(Collections.emptyMap()); // Set the required field.
      return result;
    }
    result.setIsSupported(true);

    List<Long> fileIds = req.getFileIds();
    boolean needMetadata = !req.isSetDoGetFooters() || req.isDoGetFooters();
    FileMetadataExprType type = req.isSetType() ? req.getType() : FileMetadataExprType.ORC_SARG;

    ByteBuffer[] metadatas = needMetadata ? new ByteBuffer[fileIds.size()] : null;
    ByteBuffer[] ppdResults = new ByteBuffer[fileIds.size()];
    boolean[] eliminated = new boolean[fileIds.size()];

    getMS().getFileMetadataByExpr(fileIds, type, req.getExpr(), metadatas, ppdResults, eliminated);
    for (int i = 0; i < fileIds.size(); ++i) {
      if (!eliminated[i] && ppdResults[i] == null)
      {
        continue; // No metadata => no ppd.
      }
      MetadataPpdResult mpr = new MetadataPpdResult();
      ByteBuffer ppdResult = eliminated[i] ? null : handleReadOnlyBufferForThrift(ppdResults[i]);
      mpr.setIncludeBitset(ppdResult);
      if (needMetadata) {
        ByteBuffer metadata = eliminated[i] ? null : handleReadOnlyBufferForThrift(metadatas[i]);
        mpr.setMetadata(metadata);
      }
      result.putToMetadata(fileIds.get(i), mpr);
    }
    if (!result.isSetMetadata()) {
      result.setMetadata(Collections.emptyMap()); // Set the required field.
    }
    return result;
  }

  @Override
  public GetFileMetadataResult get_file_metadata(GetFileMetadataRequest req) throws TException {
    GetFileMetadataResult result = new GetFileMetadataResult();
    RawStore ms = getMS();
    if (!ms.isFileMetadataSupported()) {
      result.setIsSupported(false);
      result.setMetadata(Collections.emptyMap()); // Set the required field.
      return result;
    }
    result.setIsSupported(true);
    List<Long> fileIds = req.getFileIds();
    ByteBuffer[] metadatas = ms.getFileMetadata(fileIds);
    assert metadatas.length == fileIds.size();
    for (int i = 0; i < metadatas.length; ++i) {
      ByteBuffer bb = metadatas[i];
      if (bb == null) {
        continue;
      }
      bb = handleReadOnlyBufferForThrift(bb);
      result.putToMetadata(fileIds.get(i), bb);
    }
    if (!result.isSetMetadata()) {
      result.setMetadata(Collections.emptyMap()); // Set the required field.
    }
    return result;
  }

  private ByteBuffer handleReadOnlyBufferForThrift(ByteBuffer bb) {
    if (!bb.isReadOnly()) {
      return bb;
    }
    // Thrift cannot write read-only buffers... oh well.
    // TODO: actually thrift never writes to the buffer, so we could use reflection to
    //       unset the unnecessary read-only flag if allocation/copy perf becomes a problem.
    ByteBuffer copy = ByteBuffer.allocate(bb.capacity());
    copy.put(bb);
    copy.flip();
    return copy;
  }

  @Override
  public PutFileMetadataResult put_file_metadata(PutFileMetadataRequest req) throws TException {
    RawStore ms = getMS();
    if (ms.isFileMetadataSupported()) {
      ms.putFileMetadata(req.getFileIds(), req.getMetadata(), req.getType());
    }
    return new PutFileMetadataResult();
  }

  @Override
  public ClearFileMetadataResult clear_file_metadata(ClearFileMetadataRequest req)
      throws TException {
    getMS().putFileMetadata(req.getFileIds(), null, null);
    return new ClearFileMetadataResult();
  }

  @Override
  public CacheFileMetadataResult cache_file_metadata(
      CacheFileMetadataRequest req) throws TException {
    RawStore ms = getMS();
    if (!ms.isFileMetadataSupported()) {
      return new CacheFileMetadataResult(false);
    }
    String dbName = req.getDbName(), tblName = req.getTblName(),
        partName = req.isSetPartName() ? req.getPartName() : null;
    boolean isAllPart = req.isSetIsAllParts() && req.isIsAllParts();
    ms.openTransaction();
    boolean success = false;
    try {
      Table tbl = ms.getTable(getDefaultCatalog(conf), dbName, tblName);
      if (tbl == null) {
        throw new NoSuchObjectException(dbName + "." + tblName + " not found");
      }
      boolean isPartitioned = tbl.isSetPartitionKeys() && tbl.getPartitionKeysSize() > 0;
      String tableInputFormat = tbl.isSetSd() ? tbl.getSd().getInputFormat() : null;
      if (!isPartitioned) {
        if (partName != null || isAllPart) {
          throw new MetaException("Table is not partitioned");
        }
        if (!tbl.isSetSd() || !tbl.getSd().isSetLocation()) {
          throw new MetaException(
              "Table does not have storage location; this operation is not supported on views");
        }
        FileMetadataExprType type = expressionProxy.getMetadataType(tableInputFormat);
        if (type == null) {
          throw new MetaException("The operation is not supported for " + tableInputFormat);
        }
        fileMetadataManager.queueCacheMetadata(tbl.getSd().getLocation(), type);
        success = true;
      } else {
        List<String> partNames;
        if (partName != null) {
          partNames = Lists.newArrayList(partName);
        } else if (isAllPart) {
          partNames = ms.listPartitionNames(getDefaultCatalog(conf), dbName, tblName, (short)-1);
        } else {
          throw new MetaException("Table is partitioned");
        }
        int batchSize = MetastoreConf.getIntVar(
            conf, ConfVars.BATCH_RETRIEVE_OBJECTS_MAX);
        int index = 0;
        int successCount = 0, failCount = 0;
        HashSet<String> failFormats = null;
        while (index < partNames.size()) {
          int currentBatchSize = Math.min(batchSize, partNames.size() - index);
          List<String> nameBatch = partNames.subList(index, index + currentBatchSize);
          index += currentBatchSize;
          List<Partition> parts = ms.getPartitionsByNames(getDefaultCatalog(conf), dbName, tblName, nameBatch);
          for (Partition part : parts) {
            if (!part.isSetSd() || !part.getSd().isSetLocation()) {
              throw new MetaException("Partition does not have storage location;" +
                  " this operation is not supported on views");
            }
            String inputFormat = part.getSd().isSetInputFormat()
                ? part.getSd().getInputFormat() : tableInputFormat;
            FileMetadataExprType type = expressionProxy.getMetadataType(inputFormat);
            if (type == null) {
              ++failCount;
              if (failFormats == null) {
                failFormats = new HashSet<>();
              }
              failFormats.add(inputFormat);
            } else {
              ++successCount;
              fileMetadataManager.queueCacheMetadata(part.getSd().getLocation(), type);
            }
          }
        }
        success = true; // Regardless of the following exception
        if (failCount > 0) {
          String errorMsg = "The operation failed for " + failCount + " partitions and "
              + "succeeded for " + successCount + " partitions; unsupported formats: ";
          boolean isFirst = true;
          for (String s : failFormats) {
            if (!isFirst) {
              errorMsg += ", ";
            }
            isFirst = false;
            errorMsg += s;
          }
          throw new MetaException(errorMsg);
        }
      }
    } finally {
      if (success) {
        if (!ms.commitTransaction()) {
          throw new MetaException("Failed to commit");
        }
      } else {
        ms.rollbackTransaction();
      }
    }
    return new CacheFileMetadataResult(true);
  }

  @Override
  public PrimaryKeysResponse get_primary_keys(PrimaryKeysRequest request) throws TException {
    request.setCatName(request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf));
    startTableFunction("get_primary_keys", request.getCatName(), request.getDb_name(), request.getTbl_name());
    List<SQLPrimaryKey> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getPrimaryKeys(request);
    } catch (Exception e) {
      ex = e;
      throwMetaException(e);
    } finally {
      endFunction("get_primary_keys", ret != null, ex, request.getTbl_name());
    }
    return new PrimaryKeysResponse(ret);
  }

  @Override
  public ForeignKeysResponse get_foreign_keys(ForeignKeysRequest request) throws TException {
    request.setCatName(request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf));
    startFunction("get_foreign_keys",
        " : parentdb=" + request.getParent_db_name() + " parenttbl=" + request.getParent_tbl_name() + " foreigndb="
            + request.getForeign_db_name() + " foreigntbl=" + request.getForeign_tbl_name());
    List<SQLForeignKey> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getForeignKeys(request);
    } catch (Exception e) {
      ex = e;
      throwMetaException(e);
    } finally {
      endFunction("get_foreign_keys", ret != null, ex, request.getForeign_tbl_name());
    }
    return new ForeignKeysResponse(ret);
  }

  @Override
  public UniqueConstraintsResponse get_unique_constraints(UniqueConstraintsRequest request) throws TException {
    request.setCatName(request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf));
    startTableFunction("get_unique_constraints", request.getCatName(), request.getDb_name(), request.getTbl_name());
    List<SQLUniqueConstraint> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getUniqueConstraints(request);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_unique_constraints", ret != null, ex, request.getTbl_name());
    }
    return new UniqueConstraintsResponse(ret);
  }

  @Override
  public NotNullConstraintsResponse get_not_null_constraints(NotNullConstraintsRequest request) throws TException {
    request.setCatName(request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf));
    startTableFunction("get_not_null_constraints", request.getCatName(), request.getDb_name(), request.getTbl_name());
    List<SQLNotNullConstraint> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getNotNullConstraints(request);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_not_null_constraints", ret != null, ex, request.getTbl_name());
    }
    return new NotNullConstraintsResponse(ret);
  }

  @Override
  public DefaultConstraintsResponse get_default_constraints(DefaultConstraintsRequest request) throws TException {
    request.setCatName(request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf));
    startTableFunction("get_default_constraints", request.getCatName(), request.getDb_name(), request.getTbl_name());
    List<SQLDefaultConstraint> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getDefaultConstraints(request);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_default_constraints", ret != null, ex, request.getTbl_name());
    }
    return new DefaultConstraintsResponse(ret);
  }

  @Override
  public CheckConstraintsResponse get_check_constraints(CheckConstraintsRequest request) throws TException {
    request.setCatName(request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf));
    startTableFunction("get_check_constraints", request.getCatName(), request.getDb_name(), request.getTbl_name());
    List<SQLCheckConstraint> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getCheckConstraints(request);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_check_constraints", ret != null, ex, request.getTbl_name());
    }
    return new CheckConstraintsResponse(ret);
  }

  /**
   * Api to fetch all table constraints at once.
   * @param request it consist of catalog name, database name and table name to identify the table in metastore
   * @return all constraints attached to given table
   * @throws TException
   */
  @Override
  public AllTableConstraintsResponse get_all_table_constraints(AllTableConstraintsRequest request)
      throws TException, MetaException, NoSuchObjectException {
    request.setCatName(request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf));
    startTableFunction("get_all_table_constraints", request.getCatName(), request.getDbName(), request.getTblName());
    SQLAllTableConstraints ret = null;
    Exception ex = null;
    try {
      ret = getMS().getAllTableConstraints(request);
    } catch (Exception e) {
      ex = e;
      throwMetaException(e);
    } finally {
      endFunction("get_all_table_constraints", ret != null, ex, request.getTblName());
    }
    return new AllTableConstraintsResponse(ret);
  }

  @Override
  public WMCreateResourcePlanResponse create_resource_plan(WMCreateResourcePlanRequest request)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    int defaultPoolSize = MetastoreConf.getIntVar(
        conf, MetastoreConf.ConfVars.WM_DEFAULT_POOL_SIZE);
    WMResourcePlan plan = request.getResourcePlan();
    if (defaultPoolSize > 0 && plan.isSetQueryParallelism()) {
      // If the default pool is not disabled, override the size with the specified parallelism.
      defaultPoolSize = plan.getQueryParallelism();
    }
    try {
      getMS().createResourcePlan(plan, request.getCopyFrom(), defaultPoolSize);
      return new WMCreateResourcePlanResponse();
    } catch (MetaException e) {
      LOG.error("Exception while trying to persist resource plan", e);
      throw e;
    }
  }

  @Override
  public WMGetResourcePlanResponse get_resource_plan(WMGetResourcePlanRequest request)
      throws NoSuchObjectException, MetaException, TException {
    try {
      WMFullResourcePlan rp = getMS().getResourcePlan(request.getResourcePlanName(), request.getNs());
      WMGetResourcePlanResponse resp = new WMGetResourcePlanResponse();
      resp.setResourcePlan(rp);
      return resp;
    } catch (MetaException e) {
      LOG.error("Exception while trying to retrieve resource plan", e);
      throw e;
    }
  }

  @Override
  public WMGetAllResourcePlanResponse get_all_resource_plans(WMGetAllResourcePlanRequest request)
      throws MetaException, TException {
    try {
      WMGetAllResourcePlanResponse resp = new WMGetAllResourcePlanResponse();
      resp.setResourcePlans(getMS().getAllResourcePlans(request.getNs()));
      return resp;
    } catch (MetaException e) {
      LOG.error("Exception while trying to retrieve resource plans", e);
      throw e;
    }
  }

  @Override
  public WMAlterResourcePlanResponse alter_resource_plan(WMAlterResourcePlanRequest request)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try {
      if (((request.isIsEnableAndActivate() ? 1 : 0) + (request.isIsReplace() ? 1 : 0)
          + (request.isIsForceDeactivate() ? 1 : 0)) > 1) {
        throw new MetaException("Invalid request; multiple flags are set");
      }
      WMAlterResourcePlanResponse response = new WMAlterResourcePlanResponse();
      // This method will only return full resource plan when activating one,
      // to give the caller the result atomically with the activation.
      WMFullResourcePlan fullPlanAfterAlter = getMS().alterResourcePlan(
          request.getResourcePlanName(), request.getNs(), request.getResourcePlan(),
          request.isIsEnableAndActivate(), request.isIsForceDeactivate(), request.isIsReplace());
      if (fullPlanAfterAlter != null) {
        response.setFullResourcePlan(fullPlanAfterAlter);
      }
      return response;
    } catch (MetaException e) {
      LOG.error("Exception while trying to alter resource plan", e);
      throw e;
    }
  }

  @Override
  public WMGetActiveResourcePlanResponse get_active_resource_plan(
      WMGetActiveResourcePlanRequest request) throws MetaException, TException {
    try {
      WMGetActiveResourcePlanResponse response = new WMGetActiveResourcePlanResponse();
      response.setResourcePlan(getMS().getActiveResourcePlan(request.getNs()));
      return response;
    } catch (MetaException e) {
      LOG.error("Exception while trying to get active resource plan", e);
      throw e;
    }
  }

  @Override
  public WMValidateResourcePlanResponse validate_resource_plan(WMValidateResourcePlanRequest request)
      throws NoSuchObjectException, MetaException, TException {
    try {
      return getMS().validateResourcePlan(request.getResourcePlanName(), request.getNs());
    } catch (MetaException e) {
      LOG.error("Exception while trying to validate resource plan", e);
      throw e;
    }
  }

  @Override
  public WMDropResourcePlanResponse drop_resource_plan(WMDropResourcePlanRequest request)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try {
      getMS().dropResourcePlan(request.getResourcePlanName(), request.getNs());
      return new WMDropResourcePlanResponse();
    } catch (MetaException e) {
      LOG.error("Exception while trying to drop resource plan", e);
      throw e;
    }
  }

  @Override
  public WMCreateTriggerResponse create_wm_trigger(WMCreateTriggerRequest request)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    try {
      getMS().createWMTrigger(request.getTrigger());
      return new WMCreateTriggerResponse();
    } catch (MetaException e) {
      LOG.error("Exception while trying to create trigger", e);
      throw e;
    }
  }

  @Override
  public WMAlterTriggerResponse alter_wm_trigger(WMAlterTriggerRequest request)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    try {
      getMS().alterWMTrigger(request.getTrigger());
      return new WMAlterTriggerResponse();
    } catch (MetaException e) {
      LOG.error("Exception while trying to alter trigger", e);
      throw e;
    }
  }

  @Override
  public WMDropTriggerResponse drop_wm_trigger(WMDropTriggerRequest request)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try {
      getMS().dropWMTrigger(request.getResourcePlanName(), request.getTriggerName(), request.getNs());
      return new WMDropTriggerResponse();
    } catch (MetaException e) {
      LOG.error("Exception while trying to drop trigger.", e);
      throw e;
    }
  }

  @Override
  public WMGetTriggersForResourePlanResponse get_triggers_for_resourceplan(
      WMGetTriggersForResourePlanRequest request)
      throws NoSuchObjectException, MetaException, TException {
    try {
      List<WMTrigger> triggers =
          getMS().getTriggersForResourcePlan(request.getResourcePlanName(), request.getNs());
      WMGetTriggersForResourePlanResponse response = new WMGetTriggersForResourePlanResponse();
      response.setTriggers(triggers);
      return response;
    } catch (MetaException e) {
      LOG.error("Exception while trying to retrieve triggers plans", e);
      throw e;
    }
  }

  @Override
  public WMAlterPoolResponse alter_wm_pool(WMAlterPoolRequest request)
      throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException,
      TException {
    try {
      getMS().alterPool(request.getPool(), request.getPoolPath());
      return new WMAlterPoolResponse();
    } catch (MetaException e) {
      LOG.error("Exception while trying to alter WMPool", e);
      throw e;
    }
  }

  @Override
  public WMCreatePoolResponse create_wm_pool(WMCreatePoolRequest request)
      throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException,
      TException {
    try {
      getMS().createPool(request.getPool());
      return new WMCreatePoolResponse();
    } catch (MetaException e) {
      LOG.error("Exception while trying to create WMPool", e);
      throw e;
    }
  }

  @Override
  public WMDropPoolResponse drop_wm_pool(WMDropPoolRequest request)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try {
      getMS().dropWMPool(request.getResourcePlanName(), request.getPoolPath(), request.getNs());
      return new WMDropPoolResponse();
    } catch (MetaException e) {
      LOG.error("Exception while trying to drop WMPool", e);
      throw e;
    }
  }

  @Override
  public WMCreateOrUpdateMappingResponse create_or_update_wm_mapping(
      WMCreateOrUpdateMappingRequest request) throws AlreadyExistsException,
      NoSuchObjectException, InvalidObjectException, MetaException, TException {
    try {
      getMS().createOrUpdateWMMapping(request.getMapping(), request.isUpdate());
      return new WMCreateOrUpdateMappingResponse();
    } catch (MetaException e) {
      LOG.error("Exception while trying to create or update WMMapping", e);
      throw e;
    }
  }

  @Override
  public WMDropMappingResponse drop_wm_mapping(WMDropMappingRequest request)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try {
      getMS().dropWMMapping(request.getMapping());
      return new WMDropMappingResponse();
    } catch (MetaException e) {
      LOG.error("Exception while trying to drop WMMapping", e);
      throw e;
    }
  }

  @Override
  public WMCreateOrDropTriggerToPoolMappingResponse create_or_drop_wm_trigger_to_pool_mapping(
      WMCreateOrDropTriggerToPoolMappingRequest request) throws AlreadyExistsException,
      NoSuchObjectException, InvalidObjectException, MetaException, TException {
    try {
      if (request.isDrop()) {
        getMS().dropWMTriggerToPoolMapping(request.getResourcePlanName(),
            request.getTriggerName(), request.getPoolPath(), request.getNs());
      } else {
        getMS().createWMTriggerToPoolMapping(request.getResourcePlanName(),
            request.getTriggerName(), request.getPoolPath(), request.getNs());
      }
      return new WMCreateOrDropTriggerToPoolMappingResponse();
    } catch (MetaException e) {
      LOG.error("Exception while trying to create or drop pool mappings", e);
      throw e;
    }
  }

  @Override
  public void create_ischema(ISchema schema) throws TException {
    startFunction("create_ischema", ": " + schema.getName());
    boolean success = false;
    Exception ex = null;
    RawStore ms = getMS();
    try {
      firePreEvent(new PreCreateISchemaEvent(this, schema));
      Map<String, String> transactionalListenersResponses = Collections.emptyMap();
      ms.openTransaction();
      try {
        ms.createISchema(schema);

        if (!transactionalListeners.isEmpty()) {
          transactionalListenersResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventType.CREATE_ISCHEMA, new CreateISchemaEvent(true, this, schema));
        }
        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }
        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners, EventType.CREATE_ISCHEMA,
              new CreateISchemaEvent(success, this, schema), null,
              transactionalListenersResponses, ms);
        }
      }
    } catch (MetaException|AlreadyExistsException e) {
      LOG.error("Caught exception creating schema", e);
      ex = e;
      throw e;
    } finally {
      endFunction("create_ischema", success, ex);
    }
  }

  @Override
  public void alter_ischema(AlterISchemaRequest rqst) throws TException {
    startFunction("alter_ischema", ": " + rqst);
    boolean success = false;
    Exception ex = null;
    RawStore ms = getMS();
    try {
      ISchema oldSchema = ms.getISchema(rqst.getName());
      if (oldSchema == null) {
        throw new NoSuchObjectException("Could not find schema " + rqst.getName());
      }
      firePreEvent(new PreAlterISchemaEvent(this, oldSchema, rqst.getNewSchema()));
      Map<String, String> transactionalListenersResponses = Collections.emptyMap();
      ms.openTransaction();
      try {
        ms.alterISchema(rqst.getName(), rqst.getNewSchema());
        if (!transactionalListeners.isEmpty()) {
          transactionalListenersResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventType.ALTER_ISCHEMA, new AlterISchemaEvent(true, this, oldSchema, rqst.getNewSchema()));
        }
        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }
        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ALTER_ISCHEMA,
              new AlterISchemaEvent(success, this, oldSchema, rqst.getNewSchema()), null,
              transactionalListenersResponses, ms);
        }
      }
    } catch (MetaException|NoSuchObjectException e) {
      LOG.error("Caught exception altering schema", e);
      ex = e;
      throw e;
    } finally {
      endFunction("alter_ischema", success, ex);
    }
  }

  @Override
  public ISchema get_ischema(ISchemaName schemaName) throws TException {
    startFunction("get_ischema", ": " + schemaName);
    Exception ex = null;
    ISchema schema = null;
    try {
      schema = getMS().getISchema(schemaName);
      if (schema == null) {
        throw new NoSuchObjectException("No schema named " + schemaName + " exists");
      }
      firePreEvent(new PreReadISchemaEvent(this, schema));
      return schema;
    } catch (MetaException e) {
      LOG.error("Caught exception getting schema", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_ischema", schema != null, ex);
    }
  }

  @Override
  public void drop_ischema(ISchemaName schemaName) throws TException {
    startFunction("drop_ischema", ": " + schemaName);
    Exception ex = null;
    boolean success = false;
    RawStore ms = getMS();
    try {
      // look for any valid versions.  This will also throw NoSuchObjectException if the schema
      // itself doesn't exist, which is what we want.
      SchemaVersion latest = ms.getLatestSchemaVersion(schemaName);
      if (latest != null) {
        ex = new InvalidOperationException("Schema " + schemaName + " cannot be dropped, it has" +
            " at least one valid version");
        throw (InvalidObjectException)ex;
      }
      ISchema schema = ms.getISchema(schemaName);
      firePreEvent(new PreDropISchemaEvent(this, schema));
      Map<String, String> transactionalListenersResponses = Collections.emptyMap();
      ms.openTransaction();
      try {
        ms.dropISchema(schemaName);
        if (!transactionalListeners.isEmpty()) {
          transactionalListenersResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventType.DROP_ISCHEMA, new DropISchemaEvent(true, this, schema));
        }
        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }
        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners, EventType.DROP_ISCHEMA,
              new DropISchemaEvent(success, this, schema), null,
              transactionalListenersResponses, ms);
        }
      }
    } catch (MetaException|NoSuchObjectException e) {
      LOG.error("Caught exception dropping schema", e);
      ex = e;
      throw e;
    } finally {
      endFunction("drop_ischema", success, ex);
    }
  }

  @Override
  public void add_schema_version(SchemaVersion schemaVersion) throws TException {
    startFunction("add_schema_version", ": " + schemaVersion);
    boolean success = false;
    Exception ex = null;
    RawStore ms = getMS();
    try {
      // Make sure the referenced schema exists
      if (ms.getISchema(schemaVersion.getSchema()) == null) {
        throw new NoSuchObjectException("No schema named " + schemaVersion.getSchema());
      }
      firePreEvent(new PreAddSchemaVersionEvent(this, schemaVersion));
      Map<String, String> transactionalListenersResponses = Collections.emptyMap();
      ms.openTransaction();
      try {
        ms.addSchemaVersion(schemaVersion);

        if (!transactionalListeners.isEmpty()) {
          transactionalListenersResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventType.ADD_SCHEMA_VERSION, new AddSchemaVersionEvent(true, this, schemaVersion));
        }
        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }
        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ADD_SCHEMA_VERSION,
              new AddSchemaVersionEvent(success, this, schemaVersion), null,
              transactionalListenersResponses, ms);
        }
      }
    } catch (MetaException|AlreadyExistsException e) {
      LOG.error("Caught exception adding schema version", e);
      ex = e;
      throw e;
    } finally {
      endFunction("add_schema_version", success, ex);
    }
  }

  @Override
  public SchemaVersion get_schema_version(SchemaVersionDescriptor version) throws TException {
    startFunction("get_schema_version", ": " + version);
    Exception ex = null;
    SchemaVersion schemaVersion = null;
    try {
      schemaVersion = getMS().getSchemaVersion(version);
      if (schemaVersion == null) {
        throw new NoSuchObjectException("No schema version " + version + "exists");
      }
      firePreEvent(new PreReadhSchemaVersionEvent(this, Collections.singletonList(schemaVersion)));
      return schemaVersion;
    } catch (MetaException e) {
      LOG.error("Caught exception getting schema version", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_schema_version", schemaVersion != null, ex);
    }
  }

  @Override
  public SchemaVersion get_schema_latest_version(ISchemaName schemaName) throws TException {
    startFunction("get_latest_schema_version", ": " + schemaName);
    Exception ex = null;
    SchemaVersion schemaVersion = null;
    try {
      schemaVersion = getMS().getLatestSchemaVersion(schemaName);
      if (schemaVersion == null) {
        throw new NoSuchObjectException("No versions of schema " + schemaName + "exist");
      }
      firePreEvent(new PreReadhSchemaVersionEvent(this, Collections.singletonList(schemaVersion)));
      return schemaVersion;
    } catch (MetaException e) {
      LOG.error("Caught exception getting latest schema version", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_latest_schema_version", schemaVersion != null, ex);
    }
  }

  @Override
  public List<SchemaVersion> get_schema_all_versions(ISchemaName schemaName) throws TException {
    startFunction("get_all_schema_versions", ": " + schemaName);
    Exception ex = null;
    List<SchemaVersion> schemaVersions = null;
    try {
      schemaVersions = getMS().getAllSchemaVersion(schemaName);
      if (schemaVersions == null) {
        throw new NoSuchObjectException("No versions of schema " + schemaName + "exist");
      }
      firePreEvent(new PreReadhSchemaVersionEvent(this, schemaVersions));
      return schemaVersions;
    } catch (MetaException e) {
      LOG.error("Caught exception getting all schema versions", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_all_schema_versions", schemaVersions != null, ex);
    }
  }

  @Override
  public void drop_schema_version(SchemaVersionDescriptor version) throws TException {
    startFunction("drop_schema_version", ": " + version);
    Exception ex = null;
    boolean success = false;
    RawStore ms = getMS();
    try {
      SchemaVersion schemaVersion = ms.getSchemaVersion(version);
      if (schemaVersion == null) {
        throw new NoSuchObjectException("No schema version " + version);
      }
      firePreEvent(new PreDropSchemaVersionEvent(this, schemaVersion));
      Map<String, String> transactionalListenersResponses = Collections.emptyMap();
      ms.openTransaction();
      try {
        ms.dropSchemaVersion(version);
        if (!transactionalListeners.isEmpty()) {
          transactionalListenersResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventType.DROP_SCHEMA_VERSION, new DropSchemaVersionEvent(true, this, schemaVersion));
        }
        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }
        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners, EventType.DROP_SCHEMA_VERSION,
              new DropSchemaVersionEvent(success, this, schemaVersion), null,
              transactionalListenersResponses, ms);
        }
      }
    } catch (MetaException|NoSuchObjectException e) {
      LOG.error("Caught exception dropping schema version", e);
      ex = e;
      throw e;
    } finally {
      endFunction("drop_schema_version", success, ex);
    }
  }

  @Override
  public FindSchemasByColsResp get_schemas_by_cols(FindSchemasByColsRqst rqst) throws TException {
    startFunction("get_schemas_by_cols");
    Exception ex = null;
    List<SchemaVersion> schemaVersions = Collections.emptyList();
    try {
      schemaVersions = getMS().getSchemaVersionsByColumns(rqst.getColName(),
          rqst.getColNamespace(), rqst.getType());
      firePreEvent(new PreReadhSchemaVersionEvent(this, schemaVersions));
      final List<SchemaVersionDescriptor> entries = new ArrayList<>(schemaVersions.size());
      schemaVersions.forEach(schemaVersion -> entries.add(
          new SchemaVersionDescriptor(schemaVersion.getSchema(), schemaVersion.getVersion())));
      return new FindSchemasByColsResp(entries);
    } catch (MetaException e) {
      LOG.error("Caught exception doing schema version query", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_schemas_by_cols", !schemaVersions.isEmpty(), ex);
    }
  }

  @Override
  public void map_schema_version_to_serde(MapSchemaVersionToSerdeRequest rqst)
      throws TException {
    startFunction("map_schema_version_to_serde, :" + rqst);
    boolean success = false;
    Exception ex = null;
    RawStore ms = getMS();
    try {
      SchemaVersion oldSchemaVersion = ms.getSchemaVersion(rqst.getSchemaVersion());
      if (oldSchemaVersion == null) {
        throw new NoSuchObjectException("No schema version " + rqst.getSchemaVersion());
      }
      SerDeInfo serde = ms.getSerDeInfo(rqst.getSerdeName());
      if (serde == null) {
        throw new NoSuchObjectException("No SerDe named " + rqst.getSerdeName());
      }
      SchemaVersion newSchemaVersion = new SchemaVersion(oldSchemaVersion);
      newSchemaVersion.setSerDe(serde);
      firePreEvent(new PreAlterSchemaVersionEvent(this, oldSchemaVersion, newSchemaVersion));
      Map<String, String> transactionalListenersResponses = Collections.emptyMap();
      ms.openTransaction();
      try {
        ms.alterSchemaVersion(rqst.getSchemaVersion(), newSchemaVersion);
        if (!transactionalListeners.isEmpty()) {
          transactionalListenersResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventType.ALTER_SCHEMA_VERSION, new AlterSchemaVersionEvent(true, this,
                      oldSchemaVersion, newSchemaVersion));
        }
        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }
        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ALTER_SCHEMA_VERSION,
              new AlterSchemaVersionEvent(success, this, oldSchemaVersion, newSchemaVersion), null,
              transactionalListenersResponses, ms);
        }
      }
    } catch (MetaException|NoSuchObjectException e) {
      LOG.error("Caught exception mapping schema version to serde", e);
      ex = e;
      throw e;
    } finally {
      endFunction("map_schema_version_to_serde", success, ex);
    }
  }

  @Override
  public void set_schema_version_state(SetSchemaVersionStateRequest rqst) throws TException {
    startFunction("set_schema_version_state, :" + rqst);
    boolean success = false;
    Exception ex = null;
    RawStore ms = getMS();
    try {
      SchemaVersion oldSchemaVersion = ms.getSchemaVersion(rqst.getSchemaVersion());
      if (oldSchemaVersion == null) {
        throw new NoSuchObjectException("No schema version " + rqst.getSchemaVersion());
      }
      SchemaVersion newSchemaVersion = new SchemaVersion(oldSchemaVersion);
      newSchemaVersion.setState(rqst.getState());
      firePreEvent(new PreAlterSchemaVersionEvent(this, oldSchemaVersion, newSchemaVersion));
      Map<String, String> transactionalListenersResponses = Collections.emptyMap();
      ms.openTransaction();
      try {
        ms.alterSchemaVersion(rqst.getSchemaVersion(), newSchemaVersion);
        if (!transactionalListeners.isEmpty()) {
          transactionalListenersResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventType.ALTER_SCHEMA_VERSION, new AlterSchemaVersionEvent(true, this,
                      oldSchemaVersion, newSchemaVersion));
        }
        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }
        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ALTER_SCHEMA_VERSION,
              new AlterSchemaVersionEvent(success, this, oldSchemaVersion, newSchemaVersion), null,
              transactionalListenersResponses, ms);
        }
      }
    } catch (MetaException|NoSuchObjectException e) {
      LOG.error("Caught exception changing schema version state", e);
      ex = e;
      throw e;
    } finally {
      endFunction("set_schema_version_state", success, ex);
    }
  }

  @Override
  public void add_serde(SerDeInfo serde) throws TException {
    startFunction("create_serde", ": " + serde.getName());
    Exception ex = null;
    boolean success = false;
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      ms.addSerde(serde);
      success = ms.commitTransaction();
    } catch (MetaException|AlreadyExistsException e) {
      LOG.error("Caught exception creating serde", e);
      ex = e;
      throw e;
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }
      endFunction("create_serde", success, ex);
    }
  }

  @Override
  public SerDeInfo get_serde(GetSerdeRequest rqst) throws TException {
    startFunction("get_serde", ": " + rqst);
    Exception ex = null;
    SerDeInfo serde = null;
    try {
      serde = getMS().getSerDeInfo(rqst.getSerdeName());
      if (serde == null) {
        throw new NoSuchObjectException("No serde named " + rqst.getSerdeName() + " exists");
      }
      return serde;
    } catch (MetaException e) {
      LOG.error("Caught exception getting serde", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_serde", serde != null, ex);
    }
  }

  public void add_runtime_stats(RuntimeStat stat) throws TException {
    startFunction("store_runtime_stats");
    Exception ex = null;
    boolean success = false;
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      ms.addRuntimeStat(stat);
      success = ms.commitTransaction();
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }
      endFunction("store_runtime_stats", success, ex);
    }
  }

  @Override
  public List<RuntimeStat> get_runtime_stats(GetRuntimeStatsRequest rqst) throws TException {
    startFunction("get_runtime_stats");
    Exception ex = null;
    try {
      List<RuntimeStat> res = getMS().getRuntimeStats(rqst.getMaxWeight(), rqst.getMaxCreateTime());
      return res;
    } catch (MetaException e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_runtime_stats", ex == null, ex);
    }
  }

  @Override
  public ScheduledQueryPollResponse scheduled_query_poll(ScheduledQueryPollRequest request)
      throws MetaException, TException {
    startFunction("scheduled_query_poll");
    Exception ex = null;
    try {
      RawStore ms = getMS();
      return ms.scheduledQueryPoll(request);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("scheduled_query_poll", ex == null, ex);
    }
  }

  @Override
  public void scheduled_query_maintenance(ScheduledQueryMaintenanceRequest request) throws MetaException, TException {
    startFunction("scheduled_query_poll");
    Exception ex = null;
    try {
      String query = request.getScheduledQuery().getQuery();
      ScheduledQueryMaintenanceRequestType requestType = request.getType();
      RawStore ms = getMS();
      ms.scheduledQueryMaintenance(request);
      if (requestType == ScheduledQueryMaintenanceRequestType.DROP) {
        abortReplCreatedOpenTxnsForDatabase(query);
      }
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("scheduled_query_poll", ex == null, ex);
    }
  }

  private void abortReplCreatedOpenTxnsForDatabase(String query) throws TException {
    List<Long> toBeAbortedTxns = null;
    List<TxnType> txnListExcludingReplCreated = new ArrayList<>();
    String pattern = "(?<=REPL LOAD )\\w+(?= INTO \\w+)";
    Pattern regex = Pattern.compile(pattern);
    Matcher matcher = regex.matcher(query);
    String dbName;
    if (matcher.find()) {
      dbName = matcher.group();
      String replPolicy = dbName + ".*";
      for (TxnType type : TxnType.values()) {
        // exclude REPL_CREATED txn
        if (type != TxnType.REPL_CREATED) {
          txnListExcludingReplCreated.add(type);
        }
      }
      List<Long> openTxnList = null;
      GetOpenTxnsResponse openTxnsResponse = null;
      try {
        openTxnsResponse = getTxnHandler()
                .getOpenTxns(txnListExcludingReplCreated);
      } catch (Exception e) {
        LOG.error("Got an error : " + e);
      }
      if (openTxnsResponse != null) {
        openTxnList = openTxnsResponse.getOpen_txns();
        if (openTxnList != null) {
          toBeAbortedTxns = getTxnHandler()
                  .getOpenTxnForPolicy(openTxnList, replPolicy);
          if (!toBeAbortedTxns.isEmpty()) {
            LOG.info("Aborting Repl created open transactions");
            abort_txns(new AbortTxnsRequest(toBeAbortedTxns));
          }
        }
      }
    }
  }

  @Override
  public void scheduled_query_progress(ScheduledQueryProgressInfo info) throws MetaException, TException {
    startFunction("scheduled_query_poll");
    Exception ex = null;
    try {
      RawStore ms = getMS();
      ms.scheduledQueryProgress(info);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("scheduled_query_poll", ex == null, ex);
    }
  }

  @Override
  public ScheduledQuery get_scheduled_query(ScheduledQueryKey scheduleKey) throws TException {
    startFunction("get_scheduled_query");
    Exception ex = null;
    try {
      return getMS().getScheduledQuery(scheduleKey);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_scheduled_query", ex == null, ex);
    }
  }

  @Override
  public void add_replication_metrics(ReplicationMetricList replicationMetricList) throws MetaException{
    startFunction("add_replication_metrics");
    Exception ex = null;
    try {
      getMS().addReplicationMetrics(replicationMetricList);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("add_replication_metrics", ex == null, ex);
    }
  }

  @Override
  public ReplicationMetricList get_replication_metrics(GetReplicationMetricsRequest
                                                           getReplicationMetricsRequest) throws MetaException{
    startFunction("get_replication_metrics");
    Exception ex = null;
    try {
      return getMS().getReplicationMetrics(getReplicationMetricsRequest);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_replication_metrics", ex == null, ex);
    }
  }

  @Override
  public void create_stored_procedure(StoredProcedure proc) throws NoSuchObjectException, MetaException {
    startFunction("create_stored_procedure");
    Exception ex = null;

    throwUnsupportedExceptionIfRemoteDB(proc.getDbName(), "create_stored_procedure");
    try {
      getMS().createOrUpdateStoredProcedure(proc);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("create_stored_procedure", ex == null, ex);
    }
  }

  public StoredProcedure get_stored_procedure(StoredProcedureRequest request) throws MetaException, NoSuchObjectException {
    startFunction("get_stored_procedure");
    Exception ex = null;
    try {
      StoredProcedure proc = getMS().getStoredProcedure(request.getCatName(), request.getDbName(), request.getProcName());
        if (proc == null) {
          throw new NoSuchObjectException(
                  "HPL/SQL StoredProcedure " + request.getDbName() + "." + request.getProcName() + " does not exist");
        }
        return proc;
    } catch (Exception e) {
      if (!(e instanceof NoSuchObjectException)) {
        LOG.error("Caught exception", e);
      }
      ex = e;
      throw e;
    } finally {
      endFunction("get_stored_procedure", ex == null, ex);
    }
  }

  @Override
  public void drop_stored_procedure(StoredProcedureRequest request) throws MetaException {
    startFunction("drop_stored_procedure");
    Exception ex = null;
    try {
      getMS().dropStoredProcedure(request.getCatName(), request.getDbName(), request.getProcName());
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("drop_stored_procedure", ex == null, ex);
    }
  }

  @Override
  public List<String> get_all_stored_procedures(ListStoredProcedureRequest request) throws MetaException {
    startFunction("get_all_stored_procedures");
    Exception ex = null;
    try {
      return getMS().getAllStoredProcedures(request);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_all_stored_procedures", ex == null, ex);
    }
  }

public Package find_package(GetPackageRequest request) throws MetaException, NoSuchObjectException {
    startFunction("find_package");
    Exception ex = null;
    try {
      Package pkg = getMS().findPackage(request);
      if (pkg == null) {
        throw new NoSuchObjectException(
                "HPL/SQL package " + request.getDbName() + "." + request.getPackageName() + " does not exist");
      }
      return pkg;
    } catch (Exception e) {
      if (!(e instanceof NoSuchObjectException)) {
        LOG.error("Caught exception", e);
      }
      ex = e;
      throw e;
    } finally {
      endFunction("find_package", ex == null, ex);
    }
  }

  public void add_package(AddPackageRequest request) throws MetaException, NoSuchObjectException {
    startFunction("add_package");
    Exception ex = null;
    try {
      getMS().addPackage(request);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("add_package", ex == null, ex);
    }
  }

  public List<String> get_all_packages(ListPackageRequest request) throws MetaException {
    startFunction("get_all_packages");
    Exception ex = null;
    try {
      return getMS().listPackages(request);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_all_packages", ex == null, ex);
    }
  }

  public void drop_package(DropPackageRequest request) throws MetaException {
    startFunction("drop_package");
    Exception ex = null;
    try {
      getMS().dropPackage(request);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("drop_package", ex == null, ex);
    }
  }

  @Override
  public List<WriteEventInfo> get_all_write_event_info(GetAllWriteEventInfoRequest request)
      throws MetaException {
    startFunction("get_all_write_event_info");
    Exception ex = null;
    try {
      List<WriteEventInfo> writeEventInfoList =
          getMS().getAllWriteEventInfo(request.getTxnId(), request.getDbName(), request.getTableName());
      return writeEventInfoList == null ? Collections.emptyList() : writeEventInfoList;
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_all_write_event_info", ex == null, ex);
    }
  }
}
