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

package org.apache.hadoop.hive.ql;

import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.LineageState;

/**
 * The class to store query level info such as queryId. Multiple queries can run
 * in the same session, so SessionState is to hold common session related info, and
 * each QueryState is to hold query related info.
 */
public class QueryState {
  /**
   * current configuration.
   */
  private final HiveConf queryConf;
  /**
   * type of the command.
   */
  private HiveOperation commandType;

  /**
   * Per-query Lineage state to track what happens in the query
   */
  private LineageState lineageState = new LineageState();

  /**
   * transaction manager used in the query.
   */
  private HiveTxnManager txnManager;

  /**
   * Private constructor, use QueryState.Builder instead.
   * @param conf The query specific configuration object
   */
  private QueryState(HiveConf conf) {
    this.queryConf = conf;
  }

  public String getQueryId() {
    return (queryConf.getVar(HiveConf.ConfVars.HIVEQUERYID));
  }

  public String getQueryString() {
    return queryConf.getQueryString();
  }

  public String getCommandType() {
    if (commandType == null) {
      return null;
    }
    return commandType.getOperationName();
  }

  public HiveOperation getHiveOperation() {
    return commandType;
  }

  public void setCommandType(HiveOperation commandType) {
    this.commandType = commandType;
  }

  public HiveConf getConf() {
    return queryConf;
  }

  public LineageState getLineageState() {
    return lineageState;
  }

  public void setLineageState(LineageState lineageState) {
    this.lineageState = lineageState;
  }

  public HiveTxnManager getTxnManager() {
    return txnManager;
  }

  public void setTxnManager(HiveTxnManager txnManager) {
    this.txnManager = txnManager;
  }

  /**
   * Builder to instantiate the QueryState object.
   */
  public static class Builder {
    private Map<String, String> confOverlay = null;
    private boolean isolated = true;
    private boolean generateNewQueryId = false;
    private HiveConf hiveConf = null;
    private LineageState lineageState = null;

    /**
     * Default constructor - use this builder to create a QueryState object.
     */
    public Builder() {
    }

    /**
     * Set this if there are specific configuration values which should be added to the original
     * config. If at least one value is set, then the configuration will be detached from the
     * original one.
     * @param confOverlay The query specific parameters
     * @return The builder
     */
    public Builder withConfOverlay(Map<String, String> confOverlay) {
      this.confOverlay = confOverlay;
      return this;
    }

    /**
     * Disable configuration isolation.
     *
     * For internal use / testing purposes only.
     */
    public Builder nonIsolated() {
      isolated = false;
      return this;
    }

    /**
     * Set this to true if new queryId should be generated, otherwise the original one will be kept.
     * If not set the default value is false.
     * @param generateNewQueryId If new queryId should be generated
     * @return The builder
     */
    public Builder withGenerateNewQueryId(boolean generateNewQueryId) {
      this.generateNewQueryId = generateNewQueryId;
      return this;
    }

    /**
     * The source HiveConf object used to create the QueryState. If runAsync is false, and the
     * confOverLay is empty then we will reuse the conf object as a backing datastore for the
     * QueryState. We will create a clone of the conf object otherwise.
     * @param hiveConf The source HiveConf
     * @return The builder
     */
    public Builder withHiveConf(HiveConf hiveConf) {
      this.hiveConf = hiveConf;
      return this;
    }

    /**
     * add a LineageState that will be set in the built QueryState
     * @param lineageState the source lineageState
     * @return the builder
     */
    public Builder withLineageState(LineageState lineageState) {
      this.lineageState = lineageState;
      return this;
    }

    /**
     * Creates the QueryState object. The default values are:
     * - runAsync false
     * - confOverlay null
     * - generateNewQueryId false
     * - conf null
     * @return The generated QueryState object
     */
    public QueryState build() {
      HiveConf queryConf;

      if (isolated) {
        // isolate query conf
        if (hiveConf == null) {
          queryConf = new HiveConf();
        } else {
          queryConf = new HiveConf(hiveConf);
        }
      } else {
        queryConf = hiveConf;
      }

      // Set the specific parameters if needed
      if (confOverlay != null && !confOverlay.isEmpty()) {
        // apply overlay query specific settings, if any
        for (Map.Entry<String, String> confEntry : confOverlay.entrySet()) {
          try {
            queryConf.verifyAndSet(confEntry.getKey(), confEntry.getValue());
          } catch (IllegalArgumentException e) {
            throw new RuntimeException("Error applying statement specific settings", e);
          }
        }
      }

      // Generate the new queryId if needed
      if (generateNewQueryId) {
        String queryId = QueryPlan.makeQueryId();
        queryConf.setVar(HiveConf.ConfVars.HIVEQUERYID, queryId);
        // FIXME: druid storage handler relies on query.id to maintain some staging directories
        // expose queryid to session level
        if (hiveConf != null) {
          hiveConf.setVar(HiveConf.ConfVars.HIVEQUERYID, queryId);
        }
      }

      QueryState queryState = new QueryState(queryConf);
      if (lineageState != null) {
        queryState.setLineageState(lineageState);
      }
      return queryState;
    }
  }
}
