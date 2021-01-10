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

package org.apache.hadoop.hive.ql.engine;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.Engine;
import org.apache.hadoop.hive.ql.engine.internal.NativeEngineHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * EngineLoader.  Tracks all database specific engines. There is a native engine
 * automatically loaded, but this also allows external engines to be loaded as well.
 */
@InterfaceStability.Unstable
public class EngineLoader {
  private static final Logger LOG = LoggerFactory.getLogger(EngineLoader.class);

  public static Map<Engine, EngineHelper> engineHelpers = new HashMap<>();

  static {
    // XXX: CDPD-20696 we shouldn't hardcode Impala here
    try {
      EngineHelper impalaHelper = (EngineHelper)
          Class.forName("org.apache.hadoop.hive.impala.ImpalaHelper").newInstance();
      engineHelpers.put(Engine.IMPALA, impalaHelper);
    } catch (Exception e) {
      LOG.info("Could not load Impala Helper class.", e);
    }
    EngineHelper defaultHelper = new NativeEngineHelper();
    engineHelpers.put(Engine.HIVE, defaultHelper);
  }

  // XXX: CDPD-20696 we should get rid of this flavor. We don't want to hardcode "impala"
  // anywhere within this module
  @Deprecated
  public static EngineHelper getExternalInstance() {
    return engineHelpers.get(Engine.IMPALA);
  }

  public static EngineHelper getInstance(HiveConf conf) {
    return engineHelpers.get(conf.getEngine());
  }
}
