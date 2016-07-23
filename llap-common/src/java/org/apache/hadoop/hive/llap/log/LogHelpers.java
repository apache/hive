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

package org.apache.hadoop.hive.llap.log;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.conf.Constants;

@InterfaceAudience.Private
// If changing this file, make sure to make corresponding changes in llap-daemon-log4j2.properties
public class LogHelpers {

  public static final String LLAP_LOGGER_NAME_QUERY_ROUTING =
      Constants.LLAP_LOGGER_NAME_QUERY_ROUTING;
  public static final String LLAP_LOGGER_NAME_CONSOLE = Constants.LLAP_LOGGER_NAME_CONSOLE;
  public static final String LLAP_LOGGER_NAME_RFA = Constants.LLAP_LOGGER_NAME_RFA;

}
