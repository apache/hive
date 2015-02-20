/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional debugrmation
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

package org.apache.hadoop.hive.llap;

import org.apache.commons.logging.Log;

public class LogLevels {
  private final boolean isT, isD, isI, isW, isE;

  public LogLevels(Log log) {
    isT = log.isTraceEnabled();
    isD = log.isDebugEnabled();
    isI = log.isInfoEnabled();
    isW = log.isWarnEnabled();
    isE = log.isErrorEnabled();
  }

  public boolean isTraceEnabled() {
    return isT;
  }

  public boolean isDebugEnabled() {
    return isD;
  }

  public boolean isInfoEnabled() {
    return isI;
  }

  public boolean isWarnEnabled() {
    return isW;
  }

  public boolean isErrorEnabled() {
    return isE;
  }
}
