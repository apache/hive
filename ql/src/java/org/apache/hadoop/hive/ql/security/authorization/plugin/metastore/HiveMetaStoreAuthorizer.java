/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreDropTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.security.HiveMetastoreAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.events.*;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactoryImpl;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.List;

/**
 * HiveMetaStoreAuthorizer :  Do authorization checks on MetaStore Events in MetaStorePreEventListener
 */

public class HiveMetaStoreAuthorizer extends MetaStorePreEventListener {
  private static final Log    LOG              = LogFactory.getLog(HiveMetaStoreAuthorizer.class);

  private static final ThreadLocal<Configuration> tConfig = new ThreadLocal<Configuration>() {
    @Override
    protected Configuration initialValue() {
      return new HiveConf(HiveMetaStoreAuthorizer.class);
    }
  };

  private static final ThreadLocal<HiveMetastoreAuthenticationProvider> tAuthenticator = new ThreadLocal<HiveMetastoreAuthenticationProvider>() {
    @Override
    protected HiveMetastoreAuthenticationProvider initialValue() {
      try {
        return (HiveMetastoreAuthenticationProvider) HiveUtils.getAuthenticator(tConfig.get(), HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER);
      } catch (HiveException excp) {
        throw new IllegalStateException("Authentication provider instantiation failure", excp);
      }
    }
  };

  public HiveMetaStoreAuthorizer(Configuration config) {
    super(config);
  }

  @Override
  public final void onEvent(PreEventContext preEventContext) throws MetaException, NoSuchObjectException, InvalidOperationException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("==> HiveMetaStoreAuthorizer.onEvent(): EventType=" + preEventContext.getEventType());
    }

    HiveMetaStoreAuthzInfo authzContext = buildAuthzContext(preEventContext);

    if (!skipAuthorization(authzContext)) {
      try {
        HiveConf              hiveConf          = new HiveConf(super.getConf(), HiveConf.class);
        HiveAuthorizerFactory authorizerFactory = HiveUtils.getAuthorizerFactory(hiveConf, HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER);

        if (authorizerFactory != null) {
          HiveMetastoreAuthenticationProvider authenticator = tAuthenticator.get();

          authenticator.setConf(hiveConf);

          HiveAuthzSessionContext.Builder authzContextBuilder = new HiveAuthzSessionContext.Builder();

          authzContextBuilder.setClientType(HiveAuthzSessionContext.CLIENT_TYPE.HIVEMETASTORE);
          authzContextBuilder.setSessionString("HiveMetaStore");

          HiveAuthzSessionContext authzSessionContext = authzContextBuilder.build();

          HiveAuthorizer hiveAuthorizer = authorizerFactory.createHiveAuthorizer(new HiveMetastoreClientFactoryImpl(), hiveConf, authenticator, authzSessionContext);

          checkPrivileges(authzContext, hiveAuthorizer);
        }
      } catch (Exception e) {
        LOG.error("HiveMetaStoreAuthorizer.onEvent(): failed", e);
        throw new MetaException(e.getMessage());
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("<== HiveMetaStoreAuthorizer.onEvent(): EventType=" + preEventContext.getEventType());
    }
  }

  HiveMetaStoreAuthzInfo buildAuthzContext(PreEventContext preEventContext) throws MetaException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("==> HiveMetaStoreAuthorizer.buildAuthzContext(): EventType=" + preEventContext.getEventType());
    }

    HiveMetaStoreAuthorizableEvent authzEvent = null;

    if (preEventContext != null) {

      switch (preEventContext.getEventType()) {
        case CREATE_DATABASE:
          authzEvent = new CreateDatabaseEvent(preEventContext);
          break;
        case ALTER_DATABASE:
          authzEvent = new AlterDatabaseEvent(preEventContext);
          break;
        case DROP_DATABASE:
          authzEvent = new DropDatabaseEvent(preEventContext);
          break;
        case CREATE_TABLE:
          authzEvent = new CreateTableEvent(preEventContext);
          if (isViewOperation(preEventContext) && (!isSuperUser(getCurrentUser(authzEvent)))) {
            throw new MetaException(getErrorMessage("CREATE_VIEW", getCurrentUser(authzEvent)));
          }
          break;
        case ALTER_TABLE:
          authzEvent = new AlterTableEvent(preEventContext);
          if (isViewOperation(preEventContext) && (!isSuperUser(getCurrentUser(authzEvent)))) {
            throw new MetaException(getErrorMessage("ALTER_VIEW", getCurrentUser(authzEvent)));
          }
          break;
        case DROP_TABLE:
          authzEvent = new DropTableEvent(preEventContext);
          if (isViewOperation(preEventContext) && (!isSuperUser(getCurrentUser(authzEvent)))) {
            throw new MetaException(getErrorMessage("DROP_VIEW", getCurrentUser(authzEvent)));
          }
          break;
        case ADD_PARTITION:
          authzEvent = new AddPartitionEvent(preEventContext);
          break;
        case ALTER_PARTITION:
          authzEvent = new AlterPartitionEvent(preEventContext);
          break;
        case LOAD_PARTITION_DONE:
          authzEvent = new LoadPartitionDoneEvent(preEventContext);
          break;
        case DROP_PARTITION:
          authzEvent = new DropPartitionEvent(preEventContext);
          break;
        case AUTHORIZATION_API_CALL:
        case READ_ISCHEMA:
        case CREATE_ISCHEMA:
        case DROP_ISCHEMA:
        case ALTER_ISCHEMA:
        case ADD_SCHEMA_VERSION:
        case ALTER_SCHEMA_VERSION:
        case DROP_SCHEMA_VERSION:
        case READ_SCHEMA_VERSION:
        case CREATE_CATALOG:
        case ALTER_CATALOG:
        case DROP_CATALOG:
          if (!isSuperUser(getCurrentUser())) {
            throw new MetaException(getErrorMessage(preEventContext, getCurrentUser()));
          }
          break;
        default:
          break;
       }
    }

    HiveMetaStoreAuthzInfo ret = authzEvent != null ? authzEvent.getAuthzContext() : null;

    if (LOG.isDebugEnabled()) {
      LOG.debug("<== HiveMetaStoreAuthorizer.buildAuthzContext(): EventType=" + preEventContext.getEventType() + "; ret=" + ret);
    }

    return ret;
  }

  boolean isSuperUser(String userName) {
    Configuration conf      = getConf();
    String        ipAddress = HiveMetaStore.HMSHandler.getIPAddress();
    return (MetaStoreServerUtils.checkUserHasHostProxyPrivileges(userName, conf, ipAddress));
  }

  boolean isViewOperation(PreEventContext preEventContext) {
    boolean ret = false;

    PreEventContext.PreEventType  preEventType = preEventContext.getEventType();

    switch (preEventType) {
      case CREATE_TABLE:
        PreCreateTableEvent preCreateTableEvent = (PreCreateTableEvent) preEventContext;
        Table table = preCreateTableEvent.getTable();
        ret         = isViewType(table);
        break;
      case ALTER_TABLE:
        PreAlterTableEvent preAlterTableEvent  = (PreAlterTableEvent) preEventContext;
        Table inTable  = preAlterTableEvent.getOldTable();
        Table outTable = preAlterTableEvent.getNewTable();
        ret            = (isViewType(inTable) || isViewType(outTable));
        break;
      case  DROP_TABLE:
        PreDropTableEvent preDropTableEvent = (PreDropTableEvent) preEventContext;
        Table droppedTable = preDropTableEvent.getTable();
        ret                = isViewType(droppedTable);
        break;
    }

    return ret;
  }

  private void checkPrivileges(final HiveMetaStoreAuthzInfo authzContext, HiveAuthorizer authorizer) throws MetaException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("==> HiveMetaStoreAuthorizer.checkPrivileges(): authzContext=" + authzContext + ", authorizer=" + authorizer);
    }

    HiveOperationType         hiveOpType       = authzContext.getOperationType();
    List<HivePrivilegeObject> inputHObjs       = authzContext.getInputHObjs();
    List<HivePrivilegeObject> outputHObjs      = authzContext.getOutputHObjs();
    HiveAuthzContext          hiveAuthzContext = authzContext.getHiveAuthzContext();

    try {
      authorizer.checkPrivileges(hiveOpType, inputHObjs, outputHObjs, hiveAuthzContext);
    } catch (Exception e) {
      throw new MetaException(e.getMessage());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("<== HiveMetaStoreAuthorizer.checkPrivileges(): authzContext=" + authzContext + ", authorizer=" + authorizer);
    }
  }

  private boolean skipAuthorization(HiveMetaStoreAuthzInfo authzContext) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("==> HiveMetaStoreAuthorizer.skipAuthorization(): authzContext=" + authzContext);
    }

    boolean ret = false;

    if (authzContext == null) {
      ret = true;
    } else {

      UserGroupInformation ugi = authzContext.getUGI();

      if (ugi == null) {
        ret = true;
      } else {
        ret = isSuperUser(ugi.getShortUserName());
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("<== HiveMetaStoreAuthorizer.skipAuthorization(): authzContext=" + authzContext + "; ret=" + ret);
    }

    return ret;
  }

  private  boolean isViewType(Table table) {
    boolean ret = false;

    String tableType = table.getTableType();

    if (TableType.MATERIALIZED_VIEW.name().equals(tableType) || TableType.VIRTUAL_VIEW.name().equals(tableType)) {
      ret = true;
    }

    return ret;
  }

  private String getErrorMessage(PreEventContext preEventContext, String user) {
    String err = "Operation type " + preEventContext.getEventType().name() + " not allowed for user:" + user;
    return err;
  }

  private String getErrorMessage(String eventType, String user) {
    String err = "Operation type " + eventType + " not allowed for user:" + user;
    return err;
  }

  private String getCurrentUser() {
    try {
      return UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException excp) {
    }
    return null;
  }

  private String getCurrentUser(HiveMetaStoreAuthorizableEvent authorizableEvent) {
    return authorizableEvent.getAuthzContext().getUGI().getShortUserName();
  }
}

