/**
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
package org.apache.hadoop.hive.shims;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * Shim for Fair scheduler
 * HiveServer2 uses fair scheduler API to resolve the queue mapping for non-impersonation
 * mode. This shim is avoid direct dependency of yarn fair scheduler on Hive.
 */
public interface SchedulerShim {
  /**
   * Reset the default fair scheduler queue mapping to end user.
   * @param conf
   * @param userName end user name
   */
  public void refreshDefaultQueue(Configuration conf, String userName)
      throws IOException;
}
