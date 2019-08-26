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

import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.dag.api.TezConfiguration;

public class TezConfigurationFactory {
  private static TezConfiguration defaultConf = new TezConfiguration();

  public static Configuration copyInto(Configuration target, Configuration src,
      Predicate<String> sourceFilter) {
    Iterator<Map.Entry<String, String>> iter = src.iterator();
    while (iter.hasNext()) {
      Map.Entry<String, String> entry = iter.next();
      String name = entry.getKey();
      String value = entry.getValue();
      String[] sources = src.getPropertySources(name);
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

      if (sourceFilter == null || sourceFilter.test(source)) {
        target.set(name, value);
      } else {
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
}
