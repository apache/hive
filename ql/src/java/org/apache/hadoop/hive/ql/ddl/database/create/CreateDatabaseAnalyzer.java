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

package org.apache.hadoop.hive.ql.ddl.database.create;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DatabaseType;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for database creation commands.
 */
@DDLType(types = HiveParser.TOK_CREATEDATABASE)
public class CreateDatabaseAnalyzer extends BaseSemanticAnalyzer {
  public CreateDatabaseAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    String databaseName = unescapeIdentifier(root.getChild(0).getText());

    boolean ifNotExists = false;
    String comment = null;
    String locationUri = null;
    String managedLocationUri = null;
    String type = DatabaseType.NATIVE.name();
    String connectorName = null;
    Map<String, String> props = null;

    for (int i = 1; i < root.getChildCount(); i++) {
      ASTNode childNode = (ASTNode) root.getChild(i);
      switch (childNode.getToken().getType()) {
      case HiveParser.TOK_IFNOTEXISTS:
        ifNotExists = true;
        break;
      case HiveParser.TOK_DATABASECOMMENT:
        comment = unescapeSQLString(childNode.getChild(0).getText());
        break;
      case HiveParser.TOK_DATABASEPROPERTIES:
        props = getProps((ASTNode) childNode.getChild(0));
        break;
      case HiveParser.TOK_DATABASELOCATION:
        locationUri = unescapeSQLString(childNode.getChild(0).getText());
        outputs.add(toWriteEntity(locationUri));
        break;
      case HiveParser.TOK_DATABASE_MANAGEDLOCATION:
        managedLocationUri = unescapeSQLString(childNode.getChild(0).getText());
        outputs.add(toWriteEntity(managedLocationUri));
        break;
      case HiveParser.TOK_DATACONNECTOR:
        type = DatabaseType.REMOTE.name();
        ASTNode nextNode = (ASTNode) root.getChild(i);
        connectorName = ((ASTNode)nextNode).getChild(0).getText();
        DataConnector connector = getDataConnector(connectorName, true);
        if (connector == null) {
          throw new SemanticException("Cannot retrieve connector with name: " + connectorName);
        }
        inputs.add(new ReadEntity(connector));
        break;
      default:
        throw new SemanticException("Unrecognized token in CREATE DATABASE statement");
      }
    }

    CreateDatabaseDesc desc = null;
    Database database = new Database(databaseName, comment, locationUri, props);
    if (type.equalsIgnoreCase(DatabaseType.NATIVE.name())) {
      desc = new CreateDatabaseDesc(databaseName, comment, locationUri, managedLocationUri, ifNotExists, props);
      database.setType(DatabaseType.NATIVE);
      // database = new Database(databaseName, comment, locationUri, props);
      if (managedLocationUri != null) {
        database.setManagedLocationUri(managedLocationUri);
      }
    } else {
      String remoteDbName = databaseName;
      if (props != null && props.get("connector.remoteDbName") != null) // TODO finalize the property name
        remoteDbName = props.get("connector.remoteDbName");
      desc = new CreateDatabaseDesc(databaseName, comment, locationUri, null, ifNotExists, props, type,
          connectorName, remoteDbName);
      database.setConnector_name(connectorName);
      database.setType(DatabaseType.REMOTE);
      database.setRemote_dbname(remoteDbName);
    }
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
    outputs.add(new WriteEntity(database, WriteEntity.WriteType.DDL_NO_LOCK));
  }
}
