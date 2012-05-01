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

package org.apache.hadoop.hive.ql.hooks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * A hook which is used to prevent people from querying dropped partitions in
 * silver. The list of wrongly dropped partitions is in cdb.datahawk - if the
 * query uses any of these partitions, it should fail.
 */
public class QueryDroppedPartitionsHook implements ExecuteWithHookContext {
  static final private Log LOG =
    LogFactory.getLog(QueryDroppedPartitionsHook.class);

//  private static final String SMC_DATABASE_NAME = "cdb.datahawk";

  @Override
  public void run(HookContext hookContext) throws Exception {

    assert(hookContext.getHookType() == HookContext.HookType.PRE_EXEC_HOOK);
    SessionState sess = SessionState.get();
    HiveConf conf = sess.getConf();
    String commandType = StringEscapeUtils.escapeJava(sess.getCommandType());

    // Only check for queries
    if ((commandType == null) ||
        (!commandType.equals("QUERY") &&
         !commandType.equals("CREATETABLE_AS_SELECT"))) {
      return;
    }

    Set<ReadEntity> inputs = hookContext.getInputs();

    // Nothing to check
    if ((inputs == null) || (inputs.isEmpty())) {
      return;
    }

    String inputString = getInputs(inputs);
    if ((inputString == null) || (inputString.isEmpty())) {
      return;
    }

    ConnectionUrlFactory urlFactory =
        HookUtils.getUrlFactory(conf,
            FBHiveConf.CONNECTION_FACTORY,
            FBHiveConf.QUERYDROPPED_PARTITIONS_CONNECTION_FACTORY,
            FBHiveConf.QUERYDROPPED_PARTITIONS_MYSQL_TIER_VAR_NAME,
            FBHiveConf.QUERYPLAN_HOST_DATABASE_VAR_NAME);

    // Return silently if you cannot connect for some reason
    if ((FBHiveConf.QUERYDROPPED_PARTITIONS_MYSQL_TIER_VAR_NAME == null) ||
        FBHiveConf.QUERYDROPPED_PARTITIONS_MYSQL_TIER_VAR_NAME.isEmpty()) {
      LOG.warn(FBHiveConf.QUERYPLAN_MYSQL_TIER_VAR_NAME + " is null");
      return;
    }

    if (urlFactory == null) {
      LOG.warn("unable to access " + conf.get(FBHiveConf.QUERYPLAN_MYSQL_TIER_VAR_NAME));
      return;
    }

    List<Object> sqlParams = new ArrayList<Object>();
    sqlParams.add(inputString);

    LOG.info("QueryDroppedPartitionsHook input string: " + inputString);

    // Does the query reference a dropped partition
    String sql = "select count(*) from 0114_dropped_parts3 " +
      "where (recovered is null or recovered != 1) and ?";

    List<List<Object>> result =
      HookUtils.runInsertSelect(conf, urlFactory, sql, sqlParams, false);

    Long numberDroppedPartitions = null;

    if (!result.isEmpty() && result.get(0).get(0) != null) {
      numberDroppedPartitions = (Long)result.get(0).get(0);
    }

    if ((numberDroppedPartitions != null) &&
        (numberDroppedPartitions > 0)) {
      String exception = "You cannot select from " + inputString + ".";
      exception += "Look at ";
      exception +=
   "https://our.intern.facebook.com/intern/sevmanager/prod/sev/137261279725248";
      exception += " for details ";
      throw new Exception(exception);
    }

  }

  private String getInputs(Set<ReadEntity> inputs) {
    StringBuilder sb = new StringBuilder();

    Map<String, Set<String>> inputMap = new HashMap<String, Set<String>>();

    for (ReadEntity input : inputs) {
      Partition inputPartition = input.getP();
      if (inputPartition == null) {
        continue;
      }

      if (!inputMap.containsKey(inputPartition.getTable().getTableName())) {
        inputMap.put(inputPartition.getTable().getTableName(), new HashSet<String>());
      }
      inputMap.get(
        inputPartition.getTable().getTableName()).add(inputPartition.getName().split("/")[0]);
    }

    if (inputMap.isEmpty()) {
      return "";
    }

    sb.append("(");
    boolean firstTable = true;

    for (Entry<String, Set<String>> entry : inputMap.entrySet()) {
      if (!firstTable) {
        sb.append(" OR ");
      } else {
        firstTable = false;
      }

      sb.append("(table_name = '" + entry.getKey() + "' AND ds IN (");

      boolean firstPartition = true;
      for (String part : entry.getValue()) {
        if (!firstPartition) {
          sb.append(", ");
        } else {
          firstPartition = false;
        }

        sb.append("'" + part + "'");
      }
      sb.append("))");
    }
    sb.append(")");

    return sb.toString();
  }

}
