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

package org.apache.hadoop.hive.ql;

import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

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
   * Private constructor, use QueryState.Builder instead
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

  /**
   * Builder to instantiate the QueryState object.
   */
  public static class Builder {
    private Map<String, String> confOverlay = null;
    private boolean runAsync = false;
    private boolean generateNewQueryId = false;
    private HiveConf hiveConf = null;

    /**
     * Default constructor - use this builder to create a QueryState object
     */
    public Builder() {
    }

    /**
     * Set this to true if the configuration should be detached from the original config. If not
     * set the default value is false.
     * @param runAsync If the configuration should be detached
     * @return The builder
     */
    public Builder withRunAsync(boolean runAsync) {
      this.runAsync = runAsync;
      return this;
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
     * confOverLay is empty then we will reuse the hiveConf object as a backing datastore for the
     * QueryState. We will create a clone of the hiveConf object otherwise.
     * @param hiveConf The source HiveConf
     * @return The builder
     */
    public Builder withHiveConf(HiveConf hiveConf) {
      this.hiveConf = hiveConf;
      return this;
    }

    /**
     * Creates the QueryState object. The default values are:
     * - runAsync false
     * - confOverlay null
     * - generateNewQueryId false
     * - hiveConf null
     * @return The generated QueryState object
     */
    public QueryState build() {
      HiveConf queryConf = hiveConf;

      if (queryConf == null) {
        // Generate a new conf if necessary
        queryConf = new HiveConf();
      } else if (runAsync || (confOverlay != null && !confOverlay.isEmpty())) {
        // Detach the original conf if necessary
        queryConf = new HiveConf(queryConf);
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
        queryConf.setVar(HiveConf.ConfVars.HIVEQUERYID, QueryPlan.makeQueryId());
      }

      return new QueryState(queryConf);
    }
  }
}
