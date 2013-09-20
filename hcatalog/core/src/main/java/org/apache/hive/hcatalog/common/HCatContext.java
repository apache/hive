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

package org.apache.hive.hcatalog.common;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;

import java.util.Map;

/**
 * HCatContext is a singleton that provides global access to configuration data.
 *
 * <p>HCatalog provides a variety of functionality that users can configure at runtime through
 * configuration properties. Available configuration properties are defined in
 * {@link HCatConstants}. HCatContext allows users to enable optional functionality by
 * setting properties in a provided configuration.</p>
 *
 * <p>HCatalog <em>users</em> (MR apps, processing framework adapters) should set properties
 * in a configuration that has been provided to
 * {@link #setConf(org.apache.hadoop.conf.Configuration)} to enable optional functionality.
 * The job configuration must be used to ensure properties are passed to the backend MR tasks.</p>
 *
 * <p>HCatalog <em>developers</em> should enable optional functionality by checking properties
 * from {@link #getConf()}. Since users are not obligated to set a configuration, optional
 * functionality must provide a sensible default.</p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum HCatContext {
  INSTANCE;

  private Configuration conf = null;

  /**
   * Use the given configuration for optional behavior. Keys exclusive to an existing config
   * are set in the new conf. The job conf must be used to ensure properties are passed to
   * backend MR tasks.
   */
  public synchronized HCatContext setConf(Configuration newConf) {
    Preconditions.checkNotNull(newConf, "Required parameter 'newConf' must not be null.");

    if (conf == null) {
      conf = newConf;
      return this;
    }

    if (conf != newConf) {
      synchronized (conf) {
        for (Map.Entry<String, String> entry : conf) {
          if ((entry.getKey().matches("hcat.*")) && (newConf.get(entry.getKey()) == null)) {
            newConf.set(entry.getKey(), entry.getValue());
          }
        }        
      }
      conf = newConf;
    }
    return this;
  }

  /**
   * Get the configuration, if there is one. Users are not required to setup HCatContext
   * unless they wish to override default behavior, so the configuration may not be present.
   *
   * @return an Optional that might contain a Configuration
   */
  public Optional<Configuration> getConf() {
    return Optional.fromNullable(conf);
  }
}
