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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.split.SplitLocationProvider;
import org.apache.tez.common.counters.TezCounters;
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
    final String locationProviderClass = HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_SPLIT_LOCATION_PROVIDER_CLASS);
    final boolean customLocationProvider =
      !HostAffinitySplitLocationProvider.class.getName().equals(locationProviderClass);
    LOG.info("SplitGenerator using llap affinitized locations: {} locationProviderClass: {}", useCustomLocations, locationProviderClass);
    if (customLocationProvider) {
      SplitLocationProvider locationProviderImpl;
      try {
        // the implementation of SplitLocationProvider may have Configuration as a single arg constructor, so we try
        // invoking that constructor first. If that does not exist, the fallback will use no-arg constructor.
        locationProviderImpl = JavaUtils
          .newInstance(JavaUtils.getClass(locationProviderClass, SplitLocationProvider.class),
            new Class<?>[]{Configuration.class}, new Object[]{conf});
      } catch (Exception e) {
        LOG.warn("Unable to instantiate {} class. Will try no-arg constructor invocation..", locationProviderClass, e);
        try {
          locationProviderImpl = JavaUtils.newInstance(JavaUtils.getClass(locationProviderClass,
            SplitLocationProvider.class));
        } catch (Exception ex) {
          throw new IOException(ex);
        }
      }
      return locationProviderImpl;
    } else if (useCustomLocations) {
      LlapRegistryService serviceRegistry = LlapRegistryService.getClient(conf);
      return getCustomSplitLocationProvider(serviceRegistry, LOG);
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

  @VisibleForTesting
  static SplitLocationProvider getCustomSplitLocationProvider(LlapRegistryService serviceRegistry, Logger LOG) throws
      IOException {
    LOG.info("Using LLAP instance " + serviceRegistry.getApplicationId());

    Collection<LlapServiceInstance> serviceInstances =
        serviceRegistry.getInstances().getAllInstancesOrdered(true);
    Preconditions.checkArgument(!serviceInstances.isEmpty(),
        "No running LLAP daemons! Please check LLAP service status and zookeeper configuration");
    ArrayList<String> locations = new ArrayList<>(serviceInstances.size());
    for (LlapServiceInstance serviceInstance : serviceInstances) {
      String executors =
          serviceInstance.getProperties().get(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS);
      if (executors != null && Integer.parseInt(executors) == 0) {
        // If the executors set to 0 we should not consider this location for affinity
        locations.add(null);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Not adding " + serviceInstance.getWorkerIdentity() + " with hostname=" +
                        serviceInstance.getHost() + " since executor number is 0");
        }
      } else {
        locations.add(serviceInstance.getHost());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding " + serviceInstance.getWorkerIdentity() + " with hostname=" +
                        serviceInstance.getHost() + " to list for split locations");
        }
      }
    }
    return new HostAffinitySplitLocationProvider(locations);
  }


  /**
   * Merges two different tez counters into one
   *
   * @param counter1 - tez counter 1
   * @param counter2 - tez counter 2
   * @return - merged tez counter
   */
  public static TezCounters mergeTezCounters(final TezCounters counter1, final TezCounters counter2) {
    TezCounters merged = new TezCounters();
    if (counter1 != null) {
      for (String counterGroup : counter1.getGroupNames()) {
        merged.addGroup(counter1.getGroup(counterGroup));
      }
    }

    if (counter2 != null) {
      for (String counterGroup : counter2.getGroupNames()) {
        merged.addGroup(counter2.getGroup(counterGroup));
      }
    }

    return merged;
  }
}
