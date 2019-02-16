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

package org.apache.hadoop.hive.metastore;

import java.util.AbstractMap;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * This abstract class needs to be extended to  provide implementation of actions that need
 * to be performed when a function ends. These methods are called whenever a function ends.
 *
 * It also provides a way to add fb303 counters through the exportCounters method.
 */

public abstract class MetaStoreEndFunctionListener implements Configurable {

  private Configuration conf;

  public MetaStoreEndFunctionListener(Configuration config){
    this.conf = config;
  }

  public abstract void onEndFunction(String functionName, MetaStoreEndFunctionContext context);

  // Unless this is overridden, it does nothing
  public void exportCounters(AbstractMap<String, Long> counters) {
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration config) {
    this.conf = config;
  }


}
