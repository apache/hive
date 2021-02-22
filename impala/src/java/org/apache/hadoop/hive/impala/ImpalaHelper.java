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

package org.apache.hadoop.hive.impala;

import org.apache.hadoop.hive.impala.funcmapper.ImpalaBuiltinsDb;
import org.apache.hadoop.hive.ql.engine.EngineHelper;

public class ImpalaHelper extends EngineHelper {

  static {
    // ensure that the instance is created with the "true" parameter.
    // If we don't call it here, it could be called from within impala-frontend with
    // the "false" parameter.
    ImpalaBuiltinsDb.getInstance();
  }

  public ImpalaHelper() {
    super(new ImpalaCompileHelper(), new ImpalaRuntimeHelper(),
        new ImpalaSessionHelper());
  }
}
