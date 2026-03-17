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

package org.apache.hadoop.hive.metastore.handler;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CreateDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_IN_TEST;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.isDbReplicationTarget;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

@SuppressWarnings("unused")
@RequestHandler(requestBody = CreateDatabaseRequest.class)
public class CreateDatabaseHandler
    extends AbstractRequestHandler<CreateDatabaseRequest, CreateDatabaseHandler.CreateDatabaseResult> {
  private static final Logger LOG = LoggerFactory.getLogger(CreateDatabaseHandler.class);
  private RawStore ms;
  private Warehouse wh;
  private Database db;
  private boolean skipAuthorization;
  private String name;

  CreateDatabaseHandler(IHMSHandler handler, CreateDatabaseRequest request) {
    super(handler, false, request);
  }

  @Override
  protected CreateDatabaseResult execute() throws TException, IOException {
    boolean success = false;
    boolean madeManagedDir = false;
    boolean madeExternalDir = false;
    boolean isReplicated = isDbReplicationTarget(db);
    Map<String, String> transactionalListenersResponses = Collections.emptyMap();
    Path dbExtPath = new Path(db.getLocationUri());
    Path dbMgdPath = db.getManagedLocationUri() != null ? new Path(db.getManagedLocationUri()) : null;
    boolean isInTest = MetastoreConf.getBoolVar(handler.getConf(), HIVE_IN_TEST);
    try {
      Database authDb = new Database(db);
      if (skipAuthorization) {
        // @TODO could it move to authorization layer?
        //null out to skip authorizer URI check
        authDb.setManagedLocationUri(null);
        authDb.setLocationUri(null);
      }

      ((HMSHandler) handler).firePreEvent(new PreCreateDatabaseEvent(authDb, handler));
      if (db.getCatalogName() != null && !db.getCatalogName().equals(Warehouse.DEFAULT_CATALOG_NAME)) {
        if (!wh.isDir(dbExtPath)) {
          LOG.debug("Creating database path {}", dbExtPath);
          if (!wh.mkdirs(dbExtPath)) {
            throw new MetaException("Unable to create database path " + dbExtPath +
                ", failed to create database " + db.getName());
          }
          madeExternalDir = true;
        }
      } else {
        if (dbMgdPath != null) {
          try {
            // Since this may be done as random user (if doAs=true) he may not have access
            // to the managed directory. We run this as an admin user
            madeManagedDir = UserGroupInformation.getLoginUser().doAs((PrivilegedExceptionAction<Boolean>) () -> {
              if (!wh.isDir(dbMgdPath)) {
                LOG.info("Creating database path in managed directory {}", dbMgdPath);
                if (!wh.mkdirs(dbMgdPath)) {
                  throw new MetaException("Unable to create database managed path " + dbMgdPath +
                      ", failed to create database " + db.getName());
                }
                return true;
              }
              return false;
            });
            if (madeManagedDir) {
              LOG.info("Created database path in managed directory {}", dbMgdPath);
            } else if (!isInTest || !isDbReplicationTarget(db)) { // Hive replication tests doesn't drop the db after each test
              throw new MetaException("Unable to create database managed directory " + dbMgdPath +
                  ", failed to create database " + db.getName());
            }
          } catch (IOException | InterruptedException e) {
            throw new MetaException(
                "Unable to create database managed directory " + dbMgdPath + ", failed to create database " +
                    db.getName() + ":" + e.getMessage());
          }
        }
        try {
          madeExternalDir = UserGroupInformation.getCurrentUser().doAs((PrivilegedExceptionAction<Boolean>) () -> {
            if (!wh.isDir(dbExtPath)) {
              LOG.info("Creating database path in external directory {}", dbExtPath);
              return wh.mkdirs(dbExtPath);
            }
            return false;
          });
          if (madeExternalDir) {
            LOG.info("Created database path in external directory {}", dbExtPath);
          } else {
            LOG.warn(
                "Failed to create external path {} for database {}. " +
                    "This may result in access not being allowed if the StorageBasedAuthorizationProvider is enabled",
                dbExtPath, db.getName());
          }
        } catch (IOException | InterruptedException | UndeclaredThrowableException e) {
          throw new MetaException("Failed to create external path " + dbExtPath + " for database " + db.getName() +
                  ". This may result in access not being allowed if the " +
              "StorageBasedAuthorizationProvider is enabled: " + e.getMessage());
        }
      }

      ms.openTransaction();
      ms.createDatabase(db);

      if (!handler.getTransactionalListeners().isEmpty()) {
        transactionalListenersResponses =
            MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(),
                EventMessage.EventType.CREATE_DATABASE,
                new CreateDatabaseEvent(db, true, handler, isReplicated));
      }

      success = ms.commitTransaction();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
        if (db.getCatalogName() != null && !db.getCatalogName().equals(Warehouse.DEFAULT_CATALOG_NAME)) {
          if (madeManagedDir && dbMgdPath != null) {
            wh.deleteDir(dbMgdPath, true, db);
          }
        } else {
          if (madeManagedDir && dbMgdPath != null) {
            try {
              UserGroupInformation.getLoginUser().doAs((PrivilegedExceptionAction<Void>) () -> {
                wh.deleteDir(dbMgdPath, true, db);
                return null;
              });
            } catch (IOException | InterruptedException e) {
              LOG.error("Couldn't delete managed directory {} after it was created for database {} {}",
                  dbMgdPath, db.getName(), e.getMessage());
            }
          }

          if (madeExternalDir) {
            try {
              UserGroupInformation.getCurrentUser().doAs((PrivilegedExceptionAction<Void>) () -> {
                wh.deleteDir(dbExtPath, true, db);
                return null;
              });
            } catch (IOException | InterruptedException e) {
              LOG.error("Couldn't delete external directory {} after it was created for database {} {}",
                  dbExtPath, db.getName(), e.getMessage());
            }
          }
        }
      }
    }
    return new CreateDatabaseResult(success, transactionalListenersResponses);
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    this.name = request.getDatabaseName();
    if (!MetaStoreUtils.validateName(name, handler.getConf())) {
      throw new InvalidObjectException(name + " is not a valid database name");
    }
    this.ms = handler.getMS();
    String catalogName =
        request.isSetCatalogName() ? request.getCatalogName() : getDefaultCatalog(handler.getConf());
    Catalog cat;
    try {
      cat = ms.getCatalog(catalogName);
    } catch (NoSuchObjectException e) {
      LOG.error("No such catalog {}", catalogName);
      throw new InvalidObjectException("No such catalog " + catalogName);
    }

    db = new Database(name, request.getDescription(), request.getLocationUri(), request.getParameters());
    db.setPrivileges(request.getPrivileges());
    db.setOwnerName(request.getOwnerName());
    db.setOwnerType(request.getOwnerType());
    db.setCatalogName(catalogName);
    db.setCreateTime((int)(System.currentTimeMillis() / 1000));
    db.setManagedLocationUri(request.getManagedLocationUri());
    db.setType(request.getType());
    db.setConnector_name(request.getDataConnectorName());
    db.setRemote_dbname(request.getRemote_dbname());
    this.wh = handler.getWh();

    String passedInURI = db.getLocationUri();
    String passedInManagedURI = db.getManagedLocationUri();
    Path defaultDbExtPath = wh.getDefaultDatabasePath(db.getName(), true);
    Path defaultDbMgdPath = wh.getDefaultDatabasePath(db.getName(), false);
    Path dbExtPath = (passedInURI != null) ?
        wh.getDnsPath(new Path(passedInURI)) : wh.determineDatabasePath(cat, db);
    Path dbMgdPath = (passedInManagedURI != null) ? wh.getDnsPath(new Path(passedInManagedURI)) : null;

    skipAuthorization = ((passedInURI == null && passedInManagedURI == null) ||
        (defaultDbExtPath.equals(dbExtPath) &&
            (dbMgdPath == null || defaultDbMgdPath.equals(dbMgdPath))));

    db.setLocationUri(dbExtPath.toString());
    if (dbMgdPath != null) {
      db.setManagedLocationUri(dbMgdPath.toString());
    }

    if (db.getOwnerName() == null){
      try {
        db.setOwnerName(SecurityUtils.getUGI().getShortUserName());
      } catch (Exception e) {
        LOG.warn("Failed to get owner name for create database operation.", e);
      }
    }
  }

  @Override
  protected void afterExecute(CreateDatabaseResult result) throws TException, IOException {
    boolean success = result != null && result.success();
    if (!handler.getListeners().isEmpty()) {
      MetaStoreListenerNotifier.notifyEvent(handler.getListeners(), EventMessage.EventType.CREATE_DATABASE,
          new CreateDatabaseEvent(db, success, handler, isDbReplicationTarget(db)),
          null, result != null ? result.transactionalListenersResponses : Collections.emptyMap(), ms);
    }
  }

  @Override
  protected String getMessagePrefix() {
    return "CreateDatabaseHandler [" + id + "] -  Create database " + name + ":";
  }

  public record CreateDatabaseResult(boolean success,
                                     Map<String, String> transactionalListenersResponses) implements Result {

  }
}
