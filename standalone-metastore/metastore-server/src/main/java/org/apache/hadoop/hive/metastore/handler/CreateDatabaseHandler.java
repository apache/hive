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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

    // HIVE-28820: create the default managed database directory even when MANAGEDLOCATION
    // is not explicitly specified. Do not persist this default path into Database.managedLocationUri.
    Path managedPathToCreate = dbMgdPath != null ? dbMgdPath : wh.getDefaultDatabasePath(db.getName(), false);
    boolean explicitManagedLocation = dbMgdPath != null;
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
        madeManagedDir = createDbDirectory(managedPathToCreate, true, "managed", true);
        if (madeManagedDir) {
          LOG.info("Created database path in managed directory {}", managedPathToCreate);
        } else if (explicitManagedLocation && (!isInTest || !isDbReplicationTarget(db))) {
          throw new MetaException("Unable to create database managed directory " + managedPathToCreate +
              ", failed to create database " + db.getName());
        }
        madeExternalDir = createDbDirectory(dbExtPath, false, "external", false);
        if (madeExternalDir) {
          LOG.info("Created database path in external directory {}", dbExtPath);
        } else {
          LOG.warn("Failed to create external path {} for database {}. " +
                  "This may result in access not being allowed if the StorageBasedAuthorizationProvider is enabled",
              dbExtPath, db.getName());
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
          if (madeManagedDir) {
            try {
              UserGroupInformation.getLoginUser().doAs((PrivilegedExceptionAction<Void>) () -> {
                wh.deleteDir(managedPathToCreate, true, db);
                return null;
              });
            } catch (IOException | InterruptedException e) {
              LOG.error("Couldn't delete managed directory {} after it was created for database {} {}",
                  managedPathToCreate, db.getName(), e.getMessage());
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
  public String toString() {
    return "CreateDatabaseHandler [" + id + "] -  Create database " + name + ":";
  }

  public record CreateDatabaseResult(boolean success,
                                     Map<String, String> transactionalListenersResponses) implements Result {

  }

  /**
   * Creates the given database directory (managed or external) as the given user,
   * running the actual mkdir as an admin (login) or current user depending on runAsLoginUser.
   *
   * @param path the directory path to create
   * @param runAsLoginUser true to run as the login (admin) user (used for managed dir,
   *                        since the calling user may not have access to it),
   *                        false to run as the current user (used for external dir)
   * @param dirLabel a short label ("managed"/"external") used only for log/error messages
   * @param throwOnMkdirFailure true to throw the exception about create database dir
   * @return true if the directory was created by this call, false if it already existed
   * @throws MetaException if directory creation fails
   */
  private boolean createDbDirectory(Path path, boolean runAsLoginUser, String dirLabel,
                                    boolean throwOnMkdirFailure) throws MetaException {
    try {
      UserGroupInformation ugi = runAsLoginUser
          ? UserGroupInformation.getLoginUser()
          : UserGroupInformation.getCurrentUser();
      return ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> {
        if (!wh.isDir(path)) {
          LOG.info("Creating database path in {} directory {}", dirLabel, path);
          if (!wh.mkdirs(path)) {
            if (throwOnMkdirFailure) {
              throw new MetaException("Unable to create database " + dirLabel + " path " + path +
                  ", failed to create database " + db.getName());
            }
            return false;
          }
          return true;
        }
        return false;
      });
    } catch (IOException | InterruptedException | UndeclaredThrowableException e) {
      throw new MetaException("Unable to create database " + dirLabel + " directory " + path +
          ", failed to create database " + db.getName() + ": " + e.getMessage());
    }
  }

}
