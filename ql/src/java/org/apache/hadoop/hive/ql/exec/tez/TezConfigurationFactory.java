/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.hadoop.hive.ql.exec.tez;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.dag.api.Scope;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.security.ssl.SSLFactory.SSL_CLIENT_CONF_KEY;

public class TezConfigurationFactory {
  private static TezConfiguration defaultConf = new TezConfiguration();
  private static final Field updatingResource;

  private static final Logger LOG = LoggerFactory.getLogger(TezConfigurationFactory.class.getName());

  static {
    //SSL configs are added as needed
    String sslConf = defaultConf.get(SSL_CLIENT_CONF_KEY, "ssl-client.xml");
    defaultConf.addResource(sslConf);
    LOG.info("SSL conf : " + sslConf);
    try {
      //Cache the field handle so that we can avoid expensive conf.getPropertySources(key) later
      updatingResource = Configuration.class.getDeclaredField("updatingResource");
    } catch (NoSuchFieldException | SecurityException e) {
      throw new RuntimeException(e);
    }
    updatingResource.setAccessible(true);

  }

  public static Configuration copyInto(Configuration target, Configuration src,
      Predicate<String> sourceFilter) {
    Iterator<Map.Entry<String, String>> iter = src.iterator();
    while (iter.hasNext()) {
      Map.Entry<String, String> entry = iter.next();
      String name = entry.getKey();
      String value = entry.getValue();
      String[] sources = getPropertyUpdatingResources(src, name);
      final String source = resolveFirstUpdatingResource(sources);

      if (sourceFilter == null || sourceFilter.test(source)) {
        target.set(name, value, source);
      } else {
        LOG.debug("'{}' didn't pass filter, skipping adding it", name);
      }
    }
    return target;
  }

  public static JobConf wrapWithJobConf(Configuration conf, Predicate<String> sourceFilter) {
    JobConf jc = new JobConf(false);
    copyInto(jc, defaultConf, sourceFilter);
    copyInto(jc, conf, sourceFilter);
    return jc;
  }

  public static void addProgrammaticallyAddedTezOptsToDagConf(Map<String, String> dagConf, JobConf srcConf) {
    Iterator<Map.Entry<String, String>> iter = srcConf.iterator();
    while (iter.hasNext()) {
      Map.Entry<String, String> entry = iter.next();
      String name = entry.getKey();

      if (name.startsWith("tez")) {
        String value = entry.getValue();
        String[] sources = getPropertyUpdatingResources(srcConf, name);
        final String source = resolveFirstUpdatingResource(sources);

        if ("programmatically".equalsIgnoreCase(source)) {
          try {
            TezConfiguration.validateProperty(name, Scope.DAG);
            LOG.debug("Adding programmatically set config to dag: {}={}", name, value);
            dagConf.put(name, value);
          } catch (IllegalStateException e) {
            // DAGImpl will throw an exception if dagConf contains an AM scoped property
            // let's not add it here programmatically (even if user added it by accident)
            LOG.warn("Skip adding '{}' to dagConf, as it's an AM scoped property", name);
          }
        }
      }
    }
  }

  private static String[] getPropertyUpdatingResources(Configuration src, String name) {
    String[] sources;
    try {
      sources = ((Map<String, String[]>) updatingResource.get(src)).get(name);
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    return sources;
  }

  private static String resolveFirstUpdatingResource(String[] sources) {
    final String source;
    if (sources == null || sources.length == 0) {
      source = null;
    } else {
      /*
       * If the property or its source wasn't found. Otherwise, returns a list of the sources of
       * the resource. The older sources are the first ones in the list.
       */
      source = sources[sources.length - 1];
    }
    return source;
  }
}
