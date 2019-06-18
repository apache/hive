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

package org.apache.hadoop.hive.ql.parse;

import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_IFEXISTS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.ScheduledQuery;
import org.apache.hadoop.hive.metastore.api.ScheduledQuery._Fields;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequestType;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.plan.CreateMacroDesc;
import org.apache.hadoop.hive.ql.plan.DropMacroDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FunctionWork;
import org.apache.hadoop.hive.ql.schq.ScheduledQueryMaintWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduledQueryAnalyzer extends BaseSemanticAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduledQueryAnalyzer.class);
  private UnparseTranslator unparseTranslator;

  public ScheduledQueryAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
    this.unparseTranslator = new UnparseTranslator(conf);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    ScheduledQueryMaintWork work;
    ScheduledQueryMaintenanceRequestType type = translateAstType(ast.getToken().getType());
    ScheduledQuery schq = interpretAstNode(ast);
    fillScheduledQuery(type, schq);
    try {
      schq.validate();
    } catch (TException e) {
      throw new SemanticException("ScheduledQuery is invalid", e);
    }
    work = new ScheduledQueryMaintWork(type, schq);
    rootTasks.add(TaskFactory.get(work));
  }

  private void fillScheduledQuery(ScheduledQueryMaintenanceRequestType type, ScheduledQuery schq)
      throws SemanticException {
    if (type == ScheduledQueryMaintenanceRequestType.INSERT) {
      populateUnfilled(schq, buildEmptySchq());
    } else {
      try {
        ScheduledQuery oldSchq = db.getMSC().getScheduledQuery(schq.getScheduleKey());
        populateUnfilled(schq, oldSchq);
      } catch (TException e) {
        throw new SemanticException("unable to get Scheduled query" + e);
      }
    }
  }

  private ScheduledQuery buildEmptySchq() {
    ScheduledQuery ret = new ScheduledQuery();
    // ret.setScheduleKey() -- not populated
    // ret.setSchedule(schedule);
    // ret.setQuery(query);
    ret.setEnabled(true);
    ret.setUser(SessionState.get().getUserName());
    return ret;
  }

  private void populateUnfilled(ScheduledQuery schq, ScheduledQuery def) {
    _Fields[] q = ScheduledQuery._Fields.values();
    for (_Fields field : q) {
      if (!schq.isSet(field) && def.isSet(field)) {
        schq.setFieldValue(field, def.getFieldValue(field));
      }
    }
  }

  private ScheduledQueryMaintenanceRequestType translateAstType(int type) throws SemanticException {
    switch (type) {
    case HiveParser.TOK_CREATE_SCHEDULED_QUERY:
      return ScheduledQueryMaintenanceRequestType.INSERT;
    case HiveParser.TOK_ALTER_SCHEDULED_QUERY:
      return ScheduledQueryMaintenanceRequestType.UPDATE;
    case HiveParser.TOK_DROP_SCHEDULED_QUERY:
      return ScheduledQueryMaintenanceRequestType.DELETE;
    default:
      throw new SemanticException("Can't handle: " + type);
    }

  }

  private ScheduledQuery interpretAstNode(ASTNode ast) throws SemanticException {
    // child0 is the schedule name
    String scheduleName = ast.getChild(0).getText();
    String clusterNamespace = conf.getVar(ConfVars.HIVE_SCHEDULED_QUERIES_NAMESPACE);
    ScheduledQueryKey key = new ScheduledQueryKey(scheduleName, clusterNamespace);
    ScheduledQuery ret = new ScheduledQuery(key);

    // child 1..n are arguments/options/etc
    for (int i = 1; i < ast.getChildCount(); i++) {
      processScheduledQueryAstNode(ret, (ASTNode) ast.getChild(i));
    }
    return ret;
  }

  private void processScheduledQueryAstNode(ScheduledQuery schq, ASTNode node) throws SemanticException {
    switch (node.getType()) {
    case HiveParser.TOK_ENABLE:
      schq.setEnabled(true);
      return;
    case HiveParser.TOK_DISABLE:
      schq.setEnabled(false);
      return;
    case HiveParser.TOK_CRON:
      schq.setSchedule(unescapeSQLString(node.getChild(0).getText()));
      return;
    case HiveParser.TOK_EXECUTED_AS:
      schq.setUser(unescapeSQLString(node.getChild(0).getText()));
      return;
    case HiveParser.TOK_QUERY:
      schq.setQuery(unparseQuery(node.getChild(0)));
      return;
    default:
      throw new SemanticException("Unexpected token: " + node.getType());
    }
  }

  private String unparseQuery(Tree child) throws SemanticException {
    ASTNode input = (ASTNode) child;
    BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(queryState, input);
    sem.analyze(input, ctx);
    sem.validate();

    unparseTranslator.applyTranslations(ctx.getTokenRewriteStream());
    String expandedText = ctx.getTokenRewriteStream().toString(input.getTokenStartIndex(), input.getTokenStopIndex());

    return expandedText;
  }

}
