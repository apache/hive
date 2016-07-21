/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.log;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.routing.PurgePolicy;
import org.apache.logging.log4j.core.appender.routing.RoutingAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.status.StatusLogger;

/**
 * A purge policy for the {@link RoutingAppender} which awaits a notification from the application
 * about a key no longer being required, before it purges it.
 */
@Plugin(name = "LlapRoutingAppenderPurgePolicy", category = "Core", printObject = true)
public class LlapRoutingAppenderPurgePolicy implements PurgePolicy {

  private static final Logger LOGGER = StatusLogger.getLogger();

  private static final ConcurrentMap<String, LlapRoutingAppenderPurgePolicy> INSTANCES =
      new ConcurrentHashMap<>();

  private final Set<String> knownAppenders =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  private final String name;

  // The Routing appender, which manages underlying appenders
  private RoutingAppender routingAppender;

  public LlapRoutingAppenderPurgePolicy(String name) {
    LOGGER.trace("Created " + LlapRoutingAppenderPurgePolicy.class.getName() + " with name=" + name);
    this.name=name;
  }

  private LlapRoutingAppenderPurgePolicy() {
    this("_NOOP_");
  }

  @Override
  public void initialize(RoutingAppender routingAppender) {
    this.routingAppender = routingAppender;
  }

  @Override
  public void purge() {
    // Nothing to do here. This is not invoked by the log4j framework. Should likely not be in
    // the log4j interface
  }

  @Override
  public void update(String key, LogEvent event) {
    Marker marker = event.getMarker();
    if (marker != null && marker.getName() != null && marker.getName().equals(Log4jQueryCompleteMarker.EOF_MARKER)) {
      LOGGER.debug("Received " + Log4jQueryCompleteMarker.EOF_MARKER + " for key. Attempting cleanup.");
      keyComplete(key);
    }
    else {
      if (knownAppenders.add(key)) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Registered key: [" + key + "] on purgePolicyWithName=" + name +
              ", thisAddress=" + System.identityHashCode(this));
        }
      }
    }
  }

  /**
   * Indicate that the specified key is no longer used.
   * @param key
   */
  private void keyComplete(String key) {
    Preconditions.checkNotNull(key, "Key must be specified");
    boolean removed = knownAppenders.remove(key);
    if (removed) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Deleting Appender for key: " + key);
      }
      routingAppender.deleteAppender(key);
    } else {
      LOGGER.trace("Ignoring call to remove unknown key: " + key);
    }
  }


  @PluginFactory
  public static PurgePolicy createPurgePolicy(
      @PluginAttribute("name") final String name) {

    // Name required for routing. Error out if it is not set.
    Preconditions.checkNotNull(name,
        "Name must be specified for " + LlapRoutingAppenderPurgePolicy.class.getName());
    LlapRoutingAppenderPurgePolicy llapRoutingAppenderPurgePolicy =
        new LlapRoutingAppenderPurgePolicy(name);
    LlapRoutingAppenderPurgePolicy old = INSTANCES.putIfAbsent(name, llapRoutingAppenderPurgePolicy);
    if (old != null) {
      LOGGER.debug("Attempt to create multiple instances of " +
          LlapRoutingAppenderPurgePolicy.class.getName() + " with the name " + name +
          ". Using original instance");
      llapRoutingAppenderPurgePolicy = old;
    }
    return llapRoutingAppenderPurgePolicy;
  }
}
