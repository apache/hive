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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.LineageState;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.tez.dag.api.TezConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class to store query level info such as queryId. Multiple queries can run
 * in the same session, so SessionState is to hold common session related info, and
 * each QueryState is to hold query related info.
 */
public class QueryState {
  private static final Logger LOG = LoggerFactory.getLogger(QueryState.class);

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
   * validTxnList supplier
   */
  private Supplier<String> validTxnList;

  /**
   * Holds the number of rows affected for insert queries.
   */
  private long numModifiedRows = 0;

  static public final String USERID_TAG = "userid";

  /**
   * map of resources involved in the query.
   */
  private final Map<String, Object> resourceMap = new HashMap<>();

  /**
   * Cache of HMS requests/responses utilized by SessionHiveMetaStoreClient.
   */
  private Map<Object, Object> hmsCache;

  /**
   * Tracks if HMS cache should be used to answer metadata requests.
   * In some sections, it makes sense to disable the cache to get fresh responses.
   */
  private boolean hmsCacheEnabled;

  /**
   * query level lock for ConditionalTask#resolveTask.
   */
  private final ReentrantLock resolveConditionalTaskLock = new ReentrantLock(true);

  /**
   * Private constructor, use QueryState.Builder instead.
   * @param conf The query specific configuration object
   */
  private QueryState(HiveConf conf) {
    this.queryConf = conf;
    this.validTxnList = () -> conf.get(ValidTxnList.VALID_TXNS_KEY);
  }

  // Get the query id stored in query specific config.
  public String getQueryId() {
    return queryConf.getVar(HiveConf.ConfVars.HIVE_QUERY_ID);
  }

  public String getQueryString() {
    return queryConf.getQueryString();
  }

  // Returns the HMS cache if it is currently enabled
  public Map<Object, Object> getHMSCache() {
    return hmsCacheEnabled ? hmsCache : null;
  }

  /**
   * Disable the HMS cache. Useful in situations when you
   * must not get cached metadata responses.
   */
  public void disableHMSCache() {
    hmsCacheEnabled = false;
  }

  public void enableHMSCache() {
    hmsCacheEnabled = true;
  }

  public void createHMSCache() {
    LOG.info("Query-level HMS cache created for {}", getQueryId());
    hmsCache = new HashMap<>();
    hmsCacheEnabled = true;
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

  public String getValidTxnList() {
    return validTxnList.get();
  }
  
  public void setValidTxnList(Supplier<String> validTxnList) {
    this.validTxnList = validTxnList;
  }

  public long getNumModifiedRows() {
    return numModifiedRows;
  }

  public void setNumModifiedRows(long numModifiedRows) {
    this.numModifiedRows = numModifiedRows;
  }

  public String getQueryTag() {
    return HiveConf.getVar(this.queryConf, HiveConf.ConfVars.HIVE_QUERY_TAG);
  }

  public void setQueryTag(String queryTag) {
    HiveConf.setVar(this.queryConf, HiveConf.ConfVars.HIVE_QUERY_TAG, queryTag);
  }

  public static void setApplicationTag(HiveConf queryConf, String queryTag) {
    String jobTag = HiveConf.getVar(queryConf, HiveConf.ConfVars.HIVE_QUERY_TAG);
    if (jobTag == null || jobTag.isEmpty()) {
      jobTag = queryTag;
    } else {
      jobTag = jobTag.concat("," + queryTag);
    }
    if (SessionState.get() != null) {
      jobTag = jobTag.concat("," + USERID_TAG + "=" + SessionState.get().getUserName());
    }
    queryConf.set(MRJobConfig.JOB_TAGS, jobTag);
    queryConf.set(TezConfiguration.TEZ_APPLICATION_TAGS, jobTag);
  }

  public void addResource(String resourceIdentifier, Object resource) {
    resourceMap.put(resourceIdentifier, resource);
  }

  public Object getResource(String resourceIdentifier) {
    return resourceMap.get(resourceIdentifier);
  }

  public ReentrantLock getResolveConditionalTaskLock() {
    return resolveConditionalTaskLock;
  }

  /**
   * Generating the new QueryState object. Making sure, that the new queryId is generated.
   * @param conf The HiveConf which should be used
   * @param lineageState a LineageState to be set in the new QueryState object
   * @return The new QueryState object
   */
  public static QueryState getNewQueryState(HiveConf conf, LineageState lineageState) {
    return new QueryState.Builder()
        .withGenerateNewQueryId(true)
        .withHiveConf(conf)
        .withLineageState(lineageState)
        .build();
  }

  /**
   * Builder to instantiate the QueryState object.
   */
  public static class Builder {
    private Map<String, String> confOverlay = null;
    private boolean isolated = true;
    private boolean generateNewQueryId = false;
    private HiveConf hiveConf = null;
    private Supplier<String> validTxnList;
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
    
    public Builder withValidTxnList(Supplier<String> validTxnList) {
      this.validTxnList = validTxnList;
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
        queryConf.setVar(HiveConf.ConfVars.HIVE_QUERY_ID, queryId);
        setApplicationTag(queryConf, queryId);

        // FIXME: druid storage handler relies on query.id to maintain some staging directories
        // expose queryid to session level
        if (hiveConf != null) {
          hiveConf.setVar(HiveConf.ConfVars.HIVE_QUERY_ID, queryId);
        }
      }

      QueryState queryState = new QueryState(queryConf);
      if (lineageState != null) {
        queryState.setLineageState(lineageState);
      }
      if (validTxnList != null) {
        queryState.setValidTxnList(validTxnList);
      }
      return queryState;
    }
  }
}
