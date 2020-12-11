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

/**
 * EngineHelper.  This abstract class is derived by engine specific code that will
 * allow engines to define engine specific logic.
 */
@InterfaceStability.Unstable
public abstract class EngineHelper {

  private final EngineCompileHelper compileHelper;
  private final EngineRuntimeHelper runtimeHelper;
  private final EngineSessionHelper sessionHelper;

  public EngineHelper(EngineCompileHelper compileHelper, EngineRuntimeHelper runtimeHelper,
      EngineSessionHelper sessionHelper) {
    this.compileHelper = compileHelper;
    this.runtimeHelper = runtimeHelper;
    this.sessionHelper = sessionHelper;
  }

  public EngineCompileHelper getCompileHelper() {
    return compileHelper;
  }

  public EngineRuntimeHelper getRuntimeHelper() {
    return runtimeHelper;
  }

  public EngineSessionHelper getSessionHelper() {
    return sessionHelper;
  }
}
