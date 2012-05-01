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

package org.apache.hadoop.hive.metastore.hooks;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.apache.hadoop.hive.ql.hooks.BaseReplicationHook;
import org.apache.hadoop.hive.ql.hooks.ConnectionUrlFactory;
import org.apache.hadoop.hive.ql.hooks.HookUtils;
import org.apache.hadoop.hive.ql.hooks.ReplicationHook;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.json.JSONException;
import org.json.JSONObject;

/*
 * MetaStoreEventListener that logs metastore operations to the audit log.
 * The operations that this listener logs are only those from the thrift server,
 * and not the CLI, because ReplicationHook currently logs queries from the CLI.
 */
public class AuditMetaStoreEventListener extends MetaStoreEventListener {
  public static final Log LOG = LogFactory.getLog(AuditMetaStoreEventListener.class);

  private static final String COMMAND_TYPE = "METASTORE_API";
  private static final String COMMAND_NAME = "name";
  private static final String ADD_PARTITION_COMMAND = "ADD_PARTITION";
  private static final String ALTER_PARTITION_COMMAND = "ALTER_PARTITION";
  private static final String ALTER_TABLE_COMMAND = "ALTER_TABLE";
  private static final String CREATE_TABLE_COMMAND = "CREATE_TABLE";
  private static final String DROP_PARTITION_COMMAND = "DROP_PARTITION";
  private static final String DROP_DATABASE_COMMAND = "DROP_DATABASE";
  private static final String DROP_TABLE_COMMAND = "DROP_TABLE";
  private static final String NEW_TABLE = "new_table";
  private static final String OLD_TABLE = "old_table";
  private static final String NEW_PARTITION = "new_partition";
  private static final String OLD_PARTITION = "old_partition";

  private final TSerializer jsonSerializer;


  protected ConnectionUrlFactory urlFactory = null;

  public AuditMetaStoreEventListener(Configuration config) throws Exception{
    super(config);
    urlFactory = BaseReplicationHook.getReplicationMySqlUrl();
    jsonSerializer = new TSerializer(new TSimpleJSONProtocol.Factory());
  }

  private void insertToDB(Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, ListenerEvent event, String command) throws MetaException {
    HiveConf conf = event.getHandler().getHiveConf();
    //if HIVEQUERYID is set, then this command came from a CLI
    // (and will execute posthooks).  We don't want to log such a command
    if (conf.getVar(HiveConf.ConfVars.HIVEQUERYID) == null ||
        conf.getVar(HiveConf.ConfVars.HIVEQUERYID).isEmpty()) {
      try {
        ArrayList<Object> sqlParams = new ArrayList<Object>();
        sqlParams.add(command);
        sqlParams.add(StringEscapeUtils.escapeJava(ReplicationHook.entitiesToString(inputs)));
        sqlParams.add(ReplicationHook.entitiesToString(outputs));
        sqlParams.add(COMMAND_TYPE);

        // Assertion at beginning of method guarantees this string will remain empty
        String sql = "insert into snc1_command_log " +
                     " set command = ?, inputs = ?, outputs = ?, command_type = ?";

        String ipAddress = HMSHandler.getIpAddress();
        if (ipAddress != null) {
          if (ipAddress.startsWith("/")) {
            ipAddress = ipAddress.replaceFirst("/", "");
          }
          sql += ", user_info = ?";
          sqlParams.add(ipAddress);
        }

        HookUtils.runInsert(conf, urlFactory, sql, sqlParams, HookUtils
            .getSqlNumRetry(conf));
      } catch (Exception e) {
        throw new MetaException(e.getMessage());
      }
    }
  }

  private Table getTableFromPart(Partition p, ListenerEvent event) throws MetaException {
    try {
      return event.getHandler().get_table(p.getDbName(), p.getTableName());
    } catch (Exception e) {
      throw new MetaException(e.getMessage());
    }
  }

  private org.apache.hadoop.hive.ql.metadata.Table getQlTable(Table t) {
    return new org.apache.hadoop.hive.ql.metadata.Table(t);
  }

  private org.apache.hadoop.hive.ql.metadata.Partition getQlPartition(Table t, Partition p)
      throws MetaException{
    try {
      org.apache.hadoop.hive.ql.metadata.Table qlTable = getQlTable(t);
      return new org.apache.hadoop.hive.ql.metadata.Partition(qlTable, p);
    } catch (HiveException e) {
      throw new MetaException(e.getMessage());
    }
  }

  private ReadEntity getPartitionInput(Partition p, ListenerEvent event)  throws MetaException {
    Table mTable = getTableFromPart(p, event);
    ReadEntity input = new ReadEntity(getQlPartition(mTable, p));
    return input;
  }

  private WriteEntity getPartitionOutput(Partition p, ListenerEvent event)  throws MetaException {
    try {
      Table mTable = event.getHandler().get_table(p.getDbName(), p.getTableName());
      WriteEntity output = new WriteEntity(getQlPartition(mTable, p));
      return output;
    } catch (Exception e) {
      throw new MetaException(e.getMessage());
    }
  }

  private void logNoSuccess() {
    LOG.info("ListenerEvent success is false");
  }

  private void addCommandNameToCommand(JSONObject command, String name) {
    try {
      command.put(COMMAND_NAME, name);
    } catch (JSONException e) {
      LOG.error("Could not add command name to JSON object", e);
    }
  }

  private void addTBaseToCommand(JSONObject command, TBase object, String objectName) {
    try {
      command.put(objectName, new JSONObject(jsonSerializer.toString(object)));
    } catch (JSONException e) {
      LOG.error("Could not add " + objectName + " to JSON object", e);
    } catch (TException e) {
      LOG.error("Could not serialize " + objectName + " to JSON", e);
    }
  }

  @Override
  public void onAddPartition(AddPartitionEvent event) throws MetaException {
    if (!event.getStatus()) {
      logNoSuccess();
      return;
    }

    Set<ReadEntity> inputs = new HashSet<ReadEntity>();
    Set<WriteEntity> outputs = new HashSet<WriteEntity>();
    JSONObject command = new JSONObject();
    inputs.add(new ReadEntity(getQlTable(getTableFromPart(event.getPartition(), event))));
    outputs.add(getPartitionOutput(event.getPartition(), event));
    addCommandNameToCommand(command, ADD_PARTITION_COMMAND);

    insertToDB(inputs, outputs, event, command.toString());
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent event) throws MetaException {
    if (!event.getStatus()) {
      logNoSuccess();
      return;
    }

    Set<ReadEntity> inputs = new HashSet<ReadEntity>();
    Set<WriteEntity> outputs = new HashSet<WriteEntity>();
    JSONObject command = new JSONObject();
    inputs.add(getPartitionInput(event.getOldPartition(), event));
    outputs.add(getPartitionOutput(event.getNewPartition(), event));
    addCommandNameToCommand(command, ALTER_PARTITION_COMMAND);
    addTBaseToCommand(command, event.getOldPartition(), OLD_PARTITION);
    addTBaseToCommand(command, event.getNewPartition(), NEW_PARTITION);

    insertToDB(inputs, outputs, event, command.toString());
  }

  @Override
  public void onDropPartition(DropPartitionEvent event) throws MetaException {
    if (!event.getStatus()) {
      logNoSuccess();
      return;
    }

    Set<ReadEntity> inputs = new HashSet<ReadEntity>();
    Set<WriteEntity> outputs = new HashSet<WriteEntity>();
    JSONObject command = new JSONObject();
    inputs.add(new ReadEntity(getQlTable(getTableFromPart(event.getPartition(), event))));
    outputs.add(getPartitionOutput(event.getPartition(), event));
    addCommandNameToCommand(command, DROP_PARTITION_COMMAND);

    insertToDB(inputs, outputs, event, command.toString());
  }

  @Override
  /*
   * Currently, on the create database CLI command, nothing gets logged.
   */
  public void onCreateDatabase(CreateDatabaseEvent event) throws MetaException {
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent event) throws MetaException {
    if (!event.getStatus()) {
      logNoSuccess();
      return;
    }

    Set<ReadEntity> inputs = new HashSet<ReadEntity>();
    Set<WriteEntity> outputs = new HashSet<WriteEntity>();
    JSONObject command = new JSONObject();
    addCommandNameToCommand(command, DROP_DATABASE_COMMAND);

    insertToDB(inputs, outputs, event, command.toString());
  }

  @Override
  public void onCreateTable(CreateTableEvent event) throws MetaException {
    if (!event.getStatus()) {
      logNoSuccess();
      return;
    }

    Set<ReadEntity> inputs = new HashSet<ReadEntity>();
    Set<WriteEntity> outputs = new HashSet<WriteEntity>();
    JSONObject command = new JSONObject();
    outputs.add(new WriteEntity(getQlTable(event.getTable())));
    addCommandNameToCommand(command, CREATE_TABLE_COMMAND);

    insertToDB(inputs, outputs, event, command.toString());
  }

  @Override
  public void onDropTable(DropTableEvent event) throws MetaException {
    if (!event.getStatus()) {
      logNoSuccess();
      return;
    }

    Set<ReadEntity> inputs = new HashSet<ReadEntity>();
    Set<WriteEntity> outputs = new HashSet<WriteEntity>();
    JSONObject command = new JSONObject();
    inputs.add(new ReadEntity(getQlTable(event.getTable())));
    outputs.add(new WriteEntity(getQlTable(event.getTable())));
    addCommandNameToCommand(command, DROP_TABLE_COMMAND);

    insertToDB(inputs, outputs, event, command.toString());
  }

  @Override
  public void onAlterTable(AlterTableEvent event) throws MetaException {
    if (!event.getStatus()) {
      logNoSuccess();
      return;
    }

    Set<ReadEntity> inputs = new HashSet<ReadEntity>();
    Set<WriteEntity> outputs = new HashSet<WriteEntity>();
    JSONObject command = new JSONObject();
    inputs.add(new ReadEntity(getQlTable(event.getOldTable())));
    outputs.add(new WriteEntity(getQlTable(event.getOldTable())));
    outputs.add(new WriteEntity(getQlTable(event.getNewTable())));
    addCommandNameToCommand(command, ALTER_TABLE_COMMAND);
    addTBaseToCommand(command, event.getOldTable(), OLD_TABLE);
    addTBaseToCommand(command, event.getNewTable(), NEW_TABLE);

    insertToDB(inputs, outputs, event, command.toString());
  }

  @Override
  public void onLoadPartitionDone(LoadPartitionDoneEvent lpe) throws MetaException {
  }

}
