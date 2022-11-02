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

import com.cronutils.builder.CronBuilder;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.field.expression.FieldExpression;
import com.google.common.base.Objects;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.ScheduledQuery;
import org.apache.hadoop.hive.metastore.api.ScheduledQuery._Fields;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequestType;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.scheduled.ScheduledQueryMaintenanceWork;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.TimestampParser;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.cronutils.model.field.expression.FieldExpressionFactory.always;
import static com.cronutils.model.field.expression.FieldExpressionFactory.on;
import static com.cronutils.model.field.expression.FieldExpressionFactory.every;
import static com.cronutils.model.field.expression.FieldExpressionFactory.questionMark;

import java.util.ArrayList;
import java.util.List;

public class ScheduledQueryAnalyzer extends BaseSemanticAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduledQueryAnalyzer.class);

  public ScheduledQueryAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void initCtx(Context ctx) {
    ctx.setScheduledQuery(true);
    super.initCtx(ctx);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    ScheduledQueryMaintenanceWork work;
    ScheduledQueryMaintenanceRequestType type = translateAstType(ast.getToken().getType());
    ScheduledQuery parsedSchq = interpretAstNode(ast);
    ScheduledQuery schq = fillScheduledQuery(type, parsedSchq);
    checkAuthorization(type, schq);
    LOG.info("scheduled query operation: " + type + " " + schq);
    try {
      schq.validate();
    } catch (TException e) {
      throw new SemanticException("ScheduledQuery is invalid", e);
    }
    work = new ScheduledQueryMaintenanceWork(type, schq);
    rootTasks.add(TaskFactory.get(work));
    queryState.setCommandType(toHiveOperation(type));
  }

  private ScheduledQuery  fillScheduledQuery(ScheduledQueryMaintenanceRequestType type, ScheduledQuery schqChanges)
      throws SemanticException {
    if (type == ScheduledQueryMaintenanceRequestType.CREATE) {
      return composeOverlayObject(schqChanges, buildEmptySchq());
    } else {
      try {
        ScheduledQuery schqStored = db.getMSC().getScheduledQuery(schqChanges.getScheduleKey());
        if (schqChanges.isSetUser()) {
          // in case the user will change; we have to run an authorization check beforehand
          checkAuthorization(type, schqStored);
        }
        // clear the next execution time
        schqStored.setNextExecutionIsSet(false);
        return composeOverlayObject(schqChanges, schqStored);
      } catch (TException e) {
        throw new SemanticException("unable to get Scheduled query" + e);
      }
    }
  }

  private ScheduledQuery buildEmptySchq() {
    ScheduledQuery ret = new ScheduledQuery();
    ret.setEnabled(conf.getBoolVar(ConfVars.HIVE_SCHEDULED_QUERIES_CREATE_AS_ENABLED));
    ret.setUser(getUserName());
    return ret;
  }

  private String getUserName() {
    SessionState sessionState = SessionState.get();
    if (sessionState.getAuthenticator() != null && sessionState.getAuthenticator().getUserName() != null) {
      return sessionState.getAuthenticator().getUserName();
    }
    String userName = sessionState.getUserName();
    if(userName == null) {
     throw new RuntimeException("userName is unset; this is unexpected");
    }
    return userName;
  }

  /**
   * Composes an overlay object.
   *
   * Output is a flattened view of the input objects.
   * having the value from the first one which has it defined from the overlays.
   */
  private ScheduledQuery composeOverlayObject(ScheduledQuery ...overlays) {
    ScheduledQuery ret = new ScheduledQuery();
    _Fields[] q = ScheduledQuery._Fields.values();
    for (_Fields field : q) {
      for (ScheduledQuery o : overlays) {
        if (o.isSet(field)) {
          ret.setFieldValue(field, o.getFieldValue(field));
          break;
        }
      }
    }
    return ret;
  }

  private ScheduledQueryMaintenanceRequestType translateAstType(int type) throws SemanticException {
    switch (type) {
    case HiveParser.TOK_CREATE_SCHEDULED_QUERY:
      return ScheduledQueryMaintenanceRequestType.CREATE;
    case HiveParser.TOK_ALTER_SCHEDULED_QUERY:
      return ScheduledQueryMaintenanceRequestType.ALTER;
    case HiveParser.TOK_DROP_SCHEDULED_QUERY:
      return ScheduledQueryMaintenanceRequestType.DROP;
    default:
      throw new SemanticException("Can't handle: " + type);
    }

  }

  private ScheduledQuery interpretAstNode(ASTNode ast) throws SemanticException {
    // child0 is the schedule name
    String scheduleName = ast.getChild(0).getText();
    String clusterNamespace = conf.getVar(ConfVars.HIVE_SCHEDULED_QUERIES_NAMESPACE);
    LOG.info("scheduled query namespace:" + clusterNamespace);
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
    case HiveParser.TOK_SCHEDULE:
      schq.setSchedule(interpretEveryNode(parseInteger(node.getChild(0).getChild(0), 1), node.getChild(1).getType(), parseTimeStamp(node.getChild(2))));
      return;
    case HiveParser.TOK_EXECUTED_AS:
      schq.setUser(unescapeSQLString(node.getChild(0).getText()));
      return;
    case HiveParser.TOK_QUERY:
      schq.setQuery(unparseTree(node.getChild(0)));
      return;
    case HiveParser.TOK_EXECUTE:
      int now = (int) (System.currentTimeMillis() / 1000);
      schq.setNextExecution(now);
      return;
    default:
      throw new SemanticException("Unexpected token: " + node.getType());
    }
  }

  private String interpretEveryNode(int every, int intervalToken, Timestamp ts) throws SemanticException {
    CronBuilder b = getDefaultCronBuilder();
    switch (intervalToken) {
    case HiveParser.TOK_INTERVAL_DAY_LITERAL:
      if (every != 1) {
        throw new SemanticException("EVERY " + every + " DAY is not supported; only EVERY DAY is supported");
      }
      b.withSecond(on(ts.getSeconds()));
      b.withMinute(on(ts.getMinutes()));
      b.withHour(on(ts.getHours()));
      break;
    case HiveParser.TOK_INTERVAL_HOUR_LITERAL:
      b.withSecond(on(ts.getSeconds()));
      b.withMinute(on(ts.getMinutes()));
      b.withHour(every(on0(ts.getHours()), every));
      break;
    case HiveParser.TOK_INTERVAL_MINUTE_LITERAL:
      b.withSecond(on(ts.getSeconds()));
      b.withMinute(every(on0(ts.getMinutes()), every));
      break;
    case HiveParser.TOK_INTERVAL_SECOND_LITERAL:
      b.withSecond(every(on0(ts.getSeconds()), every));
      break;
    default:
      throw new SemanticException("not supported schedule interval(only HOUR/MINUTE/SECOND is supported)");
    }

    return b.instance().asString();
  }

  private FieldExpression on0(int n) {
    if (n == 0) {
      return always();
    } else {
      return on(n);
    }
  }

  private int parseInteger(Tree node, int def) {
    if (node == null) {
      return def;
    } else {
      return Integer.parseInt(node.getText());
    }
  }

  private Timestamp parseTimeStamp(Tree offsetNode) {
    if (offsetNode == null) {
      return new Timestamp();
    }
    List<String> s = new ArrayList<>();
    s.add(TimestampParser.ISO_8601_FORMAT_STR);
    s.add(TimestampParser.RFC_1123_FORMAT_STR);
    s.add("HH:mm:ss");
    s.add("H:mm:ss");
    s.add("HH:mm");

    TimestampParser p = new TimestampParser(s);
    return p.parseTimestamp(unescapeSQLString(offsetNode.getText()));
  }


  private CronBuilder getDefaultCronBuilder() {
    CronDefinition definition = CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ);
    CronBuilder b = CronBuilder.cron(definition)
        .withYear(always())
        .withDoM(always())
        .withMonth(always())
        .withDoW(questionMark())
        .withHour(always())
        .withMinute(always())
        .withMinute(always())
        .withSecond(always());
    return b;
  }

  private void checkAuthorization(ScheduledQueryMaintenanceRequestType type, ScheduledQuery schq)
      throws SemanticException {
    boolean schqAuthorization = (SessionState.get().getAuthorizerV2() != null)
        && conf.getBoolVar(ConfVars.HIVE_SECURITY_AUTHORIZATION_SCHEDULED_QUERIES_SUPPORTED);

    try {
      if (!schqAuthorization) {
        String currentUser = getUserName();
        if (!Objects.equal(currentUser, schq.getUser())) {
          throw new HiveAccessControlException(
              "Authorization of scheduled queries is not enabled - only owners may change scheduled queries (currentUser: "
                  + currentUser + ", owner: " + schq.getUser() + ")");
        }
      } else {
        HiveOperationType opType = toHiveOpType(type);
        List<HivePrivilegeObject> privObjects = new ArrayList<HivePrivilegeObject>();
        ScheduledQueryKey key = schq.getScheduleKey();
        privObjects.add(
            HivePrivilegeObject.forScheduledQuery(schq.getUser(), key.getClusterNamespace(), key.getScheduleName()));

        SessionState.get().getAuthorizerV2().checkPrivileges(opType, privObjects, privObjects,
            new HiveAuthzContext.Builder().build());
      }

    } catch (Exception e) {
      throw new SemanticException(e.getMessage(), e);
    }
  }

  private HiveOperationType toHiveOpType(ScheduledQueryMaintenanceRequestType type) throws SemanticException {
    switch (type) {
    case CREATE:
      return HiveOperationType.CREATE_SCHEDULED_QUERY;
    case ALTER:
      return HiveOperationType.ALTER_SCHEDULED_QUERY;
    case DROP:
      return HiveOperationType.DROP_SCHEDULED_QUERY;
    default:
      throw new SemanticException("Unexpected type: " + type);
    }
  }

  private HiveOperation toHiveOperation(ScheduledQueryMaintenanceRequestType type) throws SemanticException {
    switch (type) {
    case CREATE:
      return HiveOperation.CREATE_SCHEDULED_QUERY;
    case ALTER:
      return HiveOperation.ALTER_SCHEDULED_QUERY;
    case DROP:
      return HiveOperation.DROP_SCHEDULED_QUERY;
    default:
      throw new SemanticException("Unexpected type: " + type);
    }
  }

  /**
   * Unparses the input AST node into correctly quoted sql string.
   */
  private String unparseTree(Tree child) throws SemanticException {
    ASTNode input = (ASTNode) child;
    ((HiveConf) (ctx.getConf())).setBoolVar(HiveConf.ConfVars.HIVE_CBO_ENABLED, false);
    BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(queryState, input);

    sem.analyze(input, ctx);
    sem.validate();
    sem.executeUnParseTranslations();

    String expandedText = ctx.getTokenRewriteStream().toString(input.getTokenStartIndex(), input.getTokenStopIndex());

    return expandedText;
  }

}
