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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.security.authorization;

import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.events.PreAddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreDropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreDropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreDropTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.events.PreReadDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreReadTableEvent;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.HiveMetastoreAuthenticationProvider;

/**
 * AuthorizationPreEventListener : A MetaStorePreEventListener that
 * performs authorization/authentication checks on the metastore-side.
 *
 * Note that this can only perform authorization checks on defined
 * metastore PreEventContexts, such as the adding/dropping and altering
 * of databases, tables and partitions.
 */
@Private
public class AuthorizationPreEventListener extends MetaStorePreEventListener {

  public static final Logger LOG = LoggerFactory.getLogger(
      AuthorizationPreEventListener.class);

  private static final ThreadLocal<Configuration> tConfig = new ThreadLocal<Configuration>() {
    @Override
    protected Configuration initialValue() {
      return new HiveConf(AuthorizationPreEventListener.class);
    }
  };

  private static final ThreadLocal<HiveMetastoreAuthenticationProvider> tAuthenticator
      = new ThreadLocal<HiveMetastoreAuthenticationProvider>() {
    @Override
    protected HiveMetastoreAuthenticationProvider initialValue() {
      try {
        return  (HiveMetastoreAuthenticationProvider) HiveUtils.getAuthenticator(
            tConfig.get(), HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER);
      } catch (HiveException he) {
        throw new IllegalStateException("Authentication provider instantiation failure",he);
      }
    }
  };

  private static final ThreadLocal<List<HiveMetastoreAuthorizationProvider>> tAuthorizers
      = new ThreadLocal<List<HiveMetastoreAuthorizationProvider>>() {
    @Override
    protected List<HiveMetastoreAuthorizationProvider> initialValue() {
      try {
        return  HiveUtils.getMetaStoreAuthorizeProviderManagers(
            tConfig.get(), HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_MANAGER, tAuthenticator.get());
      } catch (HiveException he) {
        throw new IllegalStateException("Authorization provider instantiation failure",he);
      }
    }
  };

  private static final ThreadLocal<Boolean> tConfigSetOnAuths = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return false;
    }
  };

  public AuthorizationPreEventListener(Configuration config) throws HiveException {
    super(config);
  }

  @Override
  public void onEvent(PreEventContext context) throws MetaException, NoSuchObjectException,
      InvalidOperationException {

    if (!tConfigSetOnAuths.get()){
      // The reason we do this guard is because when we do not have a good way of initializing
      // the config to the handler's thread local config until this call, so we do it then.
      // Once done, though, we need not repeat this linking, we simply call setMetaStoreHandler
      // and let the AuthorizationProvider and AuthenticationProvider do what they want.
      tConfig.set(context.getHandler().getConf());
      // Warning note : HMSHandler.getHiveConf() is not thread-unique, .getConf() is.
      tAuthenticator.get().setConf(tConfig.get());
      for(HiveMetastoreAuthorizationProvider authorizer : tAuthorizers.get()){
        authorizer.setConf(tConfig.get());
      }
      tConfigSetOnAuths.set(true); // set so we don't repeat this initialization
    }

    tAuthenticator.get().setMetaStoreHandler(context.getHandler());
    for(HiveMetastoreAuthorizationProvider authorizer : tAuthorizers.get()){
      authorizer.setMetaStoreHandler(context.getHandler());
    }

    switch (context.getEventType()) {
    case CREATE_TABLE:
      authorizeCreateTable((PreCreateTableEvent)context);
      break;
    case DROP_TABLE:
      authorizeDropTable((PreDropTableEvent)context);
      break;
    case ALTER_TABLE:
      authorizeAlterTable((PreAlterTableEvent)context);
      break;
    case READ_TABLE:
      authorizeReadTable((PreReadTableEvent)context);
      break;
    case READ_DATABASE:
      authorizeReadDatabase((PreReadDatabaseEvent)context);
      break;
    case ADD_PARTITION:
      authorizeAddPartition((PreAddPartitionEvent)context);
      break;
    case DROP_PARTITION:
      authorizeDropPartition((PreDropPartitionEvent)context);
      break;
    case ALTER_PARTITION:
      authorizeAlterPartition((PreAlterPartitionEvent)context);
      break;
    case CREATE_DATABASE:
      authorizeCreateDatabase((PreCreateDatabaseEvent)context);
      break;
    case DROP_DATABASE:
      authorizeDropDatabase((PreDropDatabaseEvent)context);
      break;
    case LOAD_PARTITION_DONE:
      // noop for now
      break;
    case AUTHORIZATION_API_CALL:
      authorizeAuthorizationAPICall();
    default:
      break;
    }

  }

  private void authorizeReadTable(PreReadTableEvent context) throws InvalidOperationException,
      MetaException {
    if (!isReadAuthzEnabled()) {
      return;
    }
    try {
      org.apache.hadoop.hive.ql.metadata.Table wrappedTable = new TableWrapper(context.getTable());
      for (HiveMetastoreAuthorizationProvider authorizer : tAuthorizers.get()) {
        authorizer.authorize(wrappedTable, new Privilege[] { Privilege.SELECT }, null);
      }
    } catch (AuthorizationException e) {
      throw invalidOperationException(e);
    } catch (HiveException e) {
      throw metaException(e);
    }
  }

  private void authorizeReadDatabase(PreReadDatabaseEvent context)
      throws InvalidOperationException, MetaException {
    if (!isReadAuthzEnabled()) {
      return;
    }
    try {
      for (HiveMetastoreAuthorizationProvider authorizer : tAuthorizers.get()) {
        authorizer.authorize(new Database(context.getDatabase()),
            new Privilege[] { Privilege.SELECT }, null);
      }
    } catch (AuthorizationException e) {
      throw invalidOperationException(e);
    } catch (HiveException e) {
      throw metaException(e);
    }
  }

  private boolean isReadAuthzEnabled() {
    return tConfig.get().getBoolean(ConfVars.HIVE_METASTORE_AUTHORIZATION_AUTH_READS.varname, true);
  }

  private void authorizeAuthorizationAPICall() throws InvalidOperationException, MetaException {
    for (HiveMetastoreAuthorizationProvider authorizer : tAuthorizers.get()) {
      try {
        authorizer.authorizeAuthorizationApiInvocation();
      } catch (AuthorizationException e) {
        throw invalidOperationException(e);
      } catch (HiveException e) {
        throw metaException(e);
      }
    }
  }

  private void authorizeCreateDatabase(PreCreateDatabaseEvent context)
      throws InvalidOperationException, MetaException {
    try {
      for (HiveMetastoreAuthorizationProvider authorizer : tAuthorizers.get()) {
        authorizer.authorize(new Database(context.getDatabase()),
            HiveOperation.CREATEDATABASE.getInputRequiredPrivileges(),
            HiveOperation.CREATEDATABASE.getOutputRequiredPrivileges());
      }
    } catch (AuthorizationException e) {
      throw invalidOperationException(e);
    } catch (HiveException e) {
      throw metaException(e);
    }
  }

  private void authorizeDropDatabase(PreDropDatabaseEvent context)
      throws InvalidOperationException, MetaException {
    try {
      for (HiveMetastoreAuthorizationProvider authorizer : tAuthorizers.get()) {
        authorizer.authorize(new Database(context.getDatabase()),
            HiveOperation.DROPDATABASE.getInputRequiredPrivileges(),
            HiveOperation.DROPDATABASE.getOutputRequiredPrivileges());
      }
    } catch (AuthorizationException e) {
      throw invalidOperationException(e);
    } catch (HiveException e) {
      throw metaException(e);
    }
  }

  private void authorizeCreateTable(PreCreateTableEvent context)
      throws InvalidOperationException, MetaException {
    try {
      org.apache.hadoop.hive.ql.metadata.Table wrappedTable = new TableWrapper(context.getTable());
      for (HiveMetastoreAuthorizationProvider authorizer : tAuthorizers.get()) {
        authorizer.authorize(wrappedTable,
            HiveOperation.CREATETABLE.getInputRequiredPrivileges(),
            HiveOperation.CREATETABLE.getOutputRequiredPrivileges());
      }
    } catch (AuthorizationException e) {
      throw invalidOperationException(e);
    } catch (HiveException e) {
      throw metaException(e);
    }
  }

  private void authorizeDropTable(PreDropTableEvent context)
      throws InvalidOperationException, MetaException {
    try {
      org.apache.hadoop.hive.ql.metadata.Table wrappedTable = new TableWrapper(context.getTable());
      for (HiveMetastoreAuthorizationProvider authorizer : tAuthorizers.get()) {
        authorizer.authorize(wrappedTable,
            HiveOperation.DROPTABLE.getInputRequiredPrivileges(),
            HiveOperation.DROPTABLE.getOutputRequiredPrivileges());
      }
    } catch (AuthorizationException e) {
      throw invalidOperationException(e);
    } catch (HiveException e) {
      throw metaException(e);
    }
  }

  private void authorizeAlterTable(PreAlterTableEvent context)
      throws InvalidOperationException, MetaException {

    try {
      org.apache.hadoop.hive.ql.metadata.Table wrappedTable = new TableWrapper(context.getOldTable());
      for (HiveMetastoreAuthorizationProvider authorizer : tAuthorizers.get()) {
        authorizer.authorize(wrappedTable,
            null,
            new Privilege[]{Privilege.ALTER_METADATA});
      }
    } catch (AuthorizationException e) {
      throw invalidOperationException(e);
    } catch (HiveException e) {
      throw metaException(e);
    }
  }

  private void authorizeAddPartition(PreAddPartitionEvent context)
      throws InvalidOperationException, MetaException {
    try {
      for (org.apache.hadoop.hive.metastore.api.Partition mapiPart : context.getPartitions()) {
        org.apache.hadoop.hive.ql.metadata.Partition wrappedPartiton = new PartitionWrapper(
            mapiPart, context);
    for(HiveMetastoreAuthorizationProvider authorizer : tAuthorizers.get()){
          authorizer.authorize(wrappedPartiton,
              HiveOperation.ALTERTABLE_ADDPARTS.getInputRequiredPrivileges(),
              HiveOperation.ALTERTABLE_ADDPARTS.getOutputRequiredPrivileges());
        }
      }
    } catch (AuthorizationException | NoSuchObjectException e) {
      throw invalidOperationException(e);
    } catch (HiveException e) {
      throw metaException(e);
    }
  }

  private void authorizeDropMultiPartition(HiveMultiPartitionAuthorizationProviderBase authorizer,
                                           final PreDropPartitionEvent context)
      throws AuthorizationException, HiveException {
    Iterator<Partition> partitionIterator = context.getPartitionIterator();

    final TableWrapper table = new TableWrapper(context.getTable());
    final Iterator<org.apache.hadoop.hive.ql.metadata.Partition> qlPartitionIterator =
        Iterators.transform(partitionIterator, new Function<Partition, org.apache.hadoop.hive.ql.metadata.Partition>() {
          @Override
          public org.apache.hadoop.hive.ql.metadata.Partition apply(Partition partition) {
            try {
              return new PartitionWrapper(table, partition);
            } catch (Exception exception) {
              LOG.error("Could not construct partition-object for: " + partition, exception);
              throw new RuntimeException(exception);
            }
          }
        });

    authorizer.authorize(new TableWrapper(context.getTable()),
                         new Iterable<org.apache.hadoop.hive.ql.metadata.Partition>() {
                           @Override
                           public Iterator<org.apache.hadoop.hive.ql.metadata.Partition> iterator() {
                             return qlPartitionIterator;
                           }
                         },
                         HiveOperation.ALTERTABLE_DROPPARTS.getInputRequiredPrivileges(),
                         HiveOperation.ALTERTABLE_DROPPARTS.getOutputRequiredPrivileges());
  }

  private void authorizeDropPartition(PreDropPartitionEvent context)
      throws InvalidOperationException, MetaException {
    try {
      for (HiveMetastoreAuthorizationProvider authorizer : tAuthorizers.get()) {
        if (authorizer instanceof HiveMultiPartitionAuthorizationProviderBase) {
          // Authorize all dropped-partitions in one shot.
          authorizeDropMultiPartition((HiveMultiPartitionAuthorizationProviderBase)authorizer, context);
        }
        else {
          // Authorize individually.
          TableWrapper table = new TableWrapper(context.getTable());
          Iterator<Partition> partitionIterator = context.getPartitionIterator();
          while (partitionIterator.hasNext()) {
            authorizer.authorize(
                new PartitionWrapper(table, partitionIterator.next()),
                HiveOperation.ALTERTABLE_DROPPARTS.getInputRequiredPrivileges(),
                HiveOperation.ALTERTABLE_DROPPARTS.getOutputRequiredPrivileges()
            );
          }
        }
      }
    } catch (AuthorizationException e) {
      throw invalidOperationException(e);
    } catch (HiveException e) {
      throw metaException(e);
    }
  }

  private void authorizeAlterPartition(PreAlterPartitionEvent context)
      throws InvalidOperationException, MetaException {
    try {
      org.apache.hadoop.hive.metastore.api.Partition mapiPart = context.getNewPartition();
      org.apache.hadoop.hive.ql.metadata.Partition wrappedPartition = new PartitionWrapper(
          mapiPart, context);
    for (HiveMetastoreAuthorizationProvider authorizer : tAuthorizers.get()) {
       authorizer.authorize(wrappedPartition,
            null,
            new Privilege[]{Privilege.ALTER_METADATA});
      }
    } catch (AuthorizationException | NoSuchObjectException e) {
      throw invalidOperationException(e);
    } catch (HiveException e) {
      throw metaException(e);
    }
  }

  private InvalidOperationException invalidOperationException(Exception e) {
    InvalidOperationException ex = new InvalidOperationException(e.getMessage());
    ex.initCause(e.getCause());
    return ex;
  }

  private MetaException metaException(HiveException e) {
    MetaException ex =  new MetaException(e.getMessage());
    ex.initCause(e);
    return ex;
  }

  // Wrapper extends ql.metadata.Table for easy construction syntax
  public static class TableWrapper extends org.apache.hadoop.hive.ql.metadata.Table {

    public TableWrapper(org.apache.hadoop.hive.metastore.api.Table apiTable) {
      org.apache.hadoop.hive.metastore.api.Table wrapperApiTable = apiTable.deepCopy();
      if (wrapperApiTable.getTableType() == null){
        // TableType specified was null, we need to figure out what type it was.
        if (MetaStoreUtils.isExternalTable(wrapperApiTable)){
          wrapperApiTable.setTableType(TableType.EXTERNAL_TABLE.toString());
        } else if (MetaStoreUtils.isIndexTable(wrapperApiTable)) {
          wrapperApiTable.setTableType(TableType.INDEX_TABLE.toString());
        } else if (MetaStoreUtils.isMaterializedViewTable(wrapperApiTable)) {
          wrapperApiTable.setTableType(TableType.MATERIALIZED_VIEW.toString());
        } else if ((wrapperApiTable.getSd() == null) || (wrapperApiTable.getSd().getLocation() == null)) {
          wrapperApiTable.setTableType(TableType.VIRTUAL_VIEW.toString());
        } else {
          wrapperApiTable.setTableType(TableType.MANAGED_TABLE.toString());
        }
      }
      initialize(wrapperApiTable);
    }
  }

  // Wrapper extends ql.metadata.Partition for easy construction syntax
  public static class PartitionWrapper extends org.apache.hadoop.hive.ql.metadata.Partition {

    public PartitionWrapper(org.apache.hadoop.hive.ql.metadata.Table table,
        org.apache.hadoop.hive.metastore.api.Partition mapiPart) throws HiveException {
      initialize(table,mapiPart);
    }

    public PartitionWrapper(org.apache.hadoop.hive.metastore.api.Partition mapiPart,
        PreEventContext context) throws HiveException, NoSuchObjectException, MetaException {
      org.apache.hadoop.hive.metastore.api.Partition wrapperApiPart = mapiPart.deepCopy();
      org.apache.hadoop.hive.metastore.api.Table t = context.getHandler().get_table_core(
          mapiPart.getDbName(), mapiPart.getTableName());
      if (wrapperApiPart.getSd() == null){
        // In the cases of create partition, by the time this event fires, the partition
        // object has not yet come into existence, and thus will not yet have a
        // location or an SD, but these are needed to create a ql.metadata.Partition,
        // so we use the table's SD. The only place this is used is by the
        // authorization hooks, so we will not affect code flow in the metastore itself.
        wrapperApiPart.setSd(t.getSd().deepCopy());
      }
      initialize(new TableWrapper(t),wrapperApiPart);
    }
  }

}
