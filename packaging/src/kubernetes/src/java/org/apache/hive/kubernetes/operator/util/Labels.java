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

package org.apache.hive.kubernetes.operator.util;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hive.kubernetes.operator.model.HiveCluster;

/** Standard Kubernetes label and selector helpers following recommended label conventions. */
public final class Labels {

  public static final String APP_NAME = "app.kubernetes.io/name";
  public static final String APP_INSTANCE = "app.kubernetes.io/instance";
  public static final String APP_COMPONENT = "app.kubernetes.io/component";
  public static final String MANAGED_BY = "app.kubernetes.io/managed-by";
  public static final String MANAGED_BY_VALUE = "hive-kubernetes-operator";

  private Labels() {
  }

  /**
   * Returns the full set of labels for a component's Kubernetes resource.
   *
   * @param hc the HiveCluster resource
   * @param component component name (metastore, hiveserver2, llap, tezam, schema-init)
   * @return label map
   */
  public static Map<String, String> forComponent(HiveCluster hc,
      String component) {
    Map<String, String> labels = new LinkedHashMap<>();
    labels.put(APP_NAME, "apache-hive");
    labels.put(APP_INSTANCE, hc.getMetadata().getName());
    labels.put(APP_COMPONENT, component);
    labels.put(MANAGED_BY, MANAGED_BY_VALUE);
    return labels;
  }

  /**
   * Returns the minimal selector labels for matching pods of a component.
   *
   * @param hc the HiveCluster resource
   * @param component component name
   * @return selector map
   */
  public static Map<String, String> selectorForComponent(HiveCluster hc,
      String component) {
    Map<String, String> selector = new LinkedHashMap<>();
    selector.put(APP_INSTANCE, hc.getMetadata().getName());
    selector.put(APP_COMPONENT, component);
    return selector;
  }
}
