/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.split.SplitLocationProvider;
import org.slf4j.Logger;

public class Utils {

  public static SplitLocationProvider getSplitLocationProvider(Configuration conf, Logger LOG)
      throws IOException {
    // fall back to checking confs
    return getSplitLocationProvider(conf, true, LOG);
  }

  public static SplitLocationProvider getSplitLocationProvider(Configuration conf, boolean useCacheAffinity, Logger LOG) throws
      IOException {
    boolean useCustomLocations =
        HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_MODE).equals("llap")
        && HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_CLIENT_CONSISTENT_SPLITS) 
        && useCacheAffinity;
    SplitLocationProvider splitLocationProvider;
    LOG.info("SplitGenerator using llap affinitized locations: " + useCustomLocations);
    if (useCustomLocations) {
      LlapRegistryService serviceRegistry = LlapRegistryService.getClient(conf);
      LOG.info("Using LLAP instance " + serviceRegistry.getApplicationId());

      Collection<LlapServiceInstance> serviceInstances =
          serviceRegistry.getInstances().getAllInstancesOrdered(true);
      Preconditions.checkArgument(!serviceInstances.isEmpty(),
          "No running LLAP daemons! Please check LLAP service status and zookeeper configuration");
      ArrayList<String> locations = new ArrayList<>(serviceInstances.size());
      for (LlapServiceInstance serviceInstance : serviceInstances) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding " + serviceInstance.getWorkerIdentity() + " with hostname=" +
              serviceInstance.getHost() + " to list for split locations");
        }
        locations.add(serviceInstance.getHost());
      }
      splitLocationProvider = new HostAffinitySplitLocationProvider(locations);
    } else {
      splitLocationProvider = new SplitLocationProvider() {
        @Override
        public String[] getLocations(InputSplit split) throws IOException {
          if (split == null) {
            return null;
          }
          String[] locations = split.getLocations();
          if (locations != null && locations.length == 1) {
            if ("localhost".equals(locations[0])) {
              return ArrayUtils.EMPTY_STRING_ARRAY;
            }
          }
          return locations;
        }
      };
    }
    return splitLocationProvider;
  }
}
