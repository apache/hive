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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.cli.SemanticAnalysis;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.StorageFormat;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;

final class CreateTableHook extends HCatSemanticAnalyzerBase {

  private String tableName;

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context,
                ASTNode ast) throws SemanticException {

    Hive db;
    try {
      db = context.getHive();
    } catch (HiveException e) {
      throw new SemanticException(
        "Couldn't get Hive DB instance in semantic analysis phase.",
        e);
    }

    // Analyze and create tbl properties object
    int numCh = ast.getChildCount();

    tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast
      .getChild(0));
    boolean likeTable = false;
    StorageFormat format = new StorageFormat(context.getConf());

    for (int num = 1; num < numCh; num++) {
      ASTNode child = (ASTNode) ast.getChild(num);
      if (format.fillStorageFormat(child)) {
        if (org.apache.commons.lang.StringUtils
            .isNotEmpty(format.getStorageHandler())) {
            return ast;
        }
        continue;
      }
      switch (child.getToken().getType()) {

      case HiveParser.TOK_QUERY: // CTAS
        throw new SemanticException(
          "Operation not supported. Create table as " +
            "Select is not a valid operation.");

      case HiveParser.TOK_ALTERTABLE_BUCKETS:
        break;

      case HiveParser.TOK_LIKETABLE:
        likeTable = true;
        break;

      case HiveParser.TOK_IFNOTEXISTS:
        try {
          List<String> tables = db.getTablesByPattern(tableName);
          if (tables != null && tables.size() > 0) { // table
            // exists
            return ast;
          }
        } catch (HiveException e) {
          throw new SemanticException(e);
        }
        break;

      case HiveParser.TOK_TABLEPARTCOLS:
        List<FieldSchema> partCols = BaseSemanticAnalyzer
          .getColumns((ASTNode) child.getChild(0), false);
        for (FieldSchema fs : partCols) {
          if (!fs.getType().equalsIgnoreCase("string")) {
            throw new SemanticException(
              "Operation not supported. HCatalog only " +
                "supports partition columns of type string. "
                + "For column: "
                + fs.getName()
                + " Found type: " + fs.getType());
          }
        }
        break;
      }
    }

    if (!likeTable && (format.getInputFormat() == null || format.getOutputFormat() == null)) {
      throw new SemanticException(
        "STORED AS specification is either incomplete or incorrect.");
    }

    return ast;
  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
              List<Task<? extends Serializable>> rootTasks)
    throws SemanticException {

    if (rootTasks.size() == 0) {
      // There will be no DDL task created in case if its CREATE TABLE IF
      // NOT EXISTS
      return;
    }
    CreateTableDesc desc = ((DDLTask) rootTasks.get(rootTasks.size() - 1))
      .getWork().getCreateTblDesc();
    if (desc == null) {
      // Desc will be null if its CREATE TABLE LIKE. Desc will be
      // contained in CreateTableLikeDesc. Currently, HCat disallows CTLT in
      // pre-hook. So, desc can never be null.
      return;
    }
    Map<String, String> tblProps = desc.getTblProps();
    if (tblProps == null) {
      // tblProps will be null if user didnt use tblprops in his CREATE
      // TABLE cmd.
      tblProps = new HashMap<String, String>();

    }

    // first check if we will allow the user to create table.
    String storageHandler = desc.getStorageHandler();
    if (StringUtils.isEmpty(storageHandler)) {
    } else {
      try {
        HiveStorageHandler storageHandlerInst = HCatUtil
          .getStorageHandler(context.getConf(),
            desc.getStorageHandler(),
            desc.getSerName(),
            desc.getInputFormat(),
            desc.getOutputFormat());
        //Authorization checks are performed by the storageHandler.getAuthorizationProvider(), if
        //StorageDelegationAuthorizationProvider is used.
      } catch (IOException e) {
        throw new SemanticException(e);
      }
    }

    if (desc != null) {
      try {
        Table table = context.getHive().newTable(desc.getTableName());
        if (desc.getLocation() != null) {
          table.setDataLocation(new Path(desc.getLocation()));
        }
        if (desc.getStorageHandler() != null) {
          table.setProperty(
            org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE,
            desc.getStorageHandler());
        }
        for (Map.Entry<String, String> prop : tblProps.entrySet()) {
          table.setProperty(prop.getKey(), prop.getValue());
        }
        for (Map.Entry<String, String> prop : desc.getSerdeProps().entrySet()) {
          table.setSerdeParam(prop.getKey(), prop.getValue());
        }
        //TODO: set other Table properties as needed

        //authorize against the table operation so that location permissions can be checked if any

        if (HCatAuthUtil.isAuthorizationEnabled(context.getConf())) {
          authorize(table, Privilege.CREATE);
        }
      } catch (HiveException ex) {
        throw new SemanticException(ex);
      }
    }

    desc.setTblProps(tblProps);
    context.getConf().set(HCatConstants.HCAT_CREATE_TBL_NAME, tableName);
  }
}
