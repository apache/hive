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
package org.apache.hadoop.hive.ql.parse.repl.log.message;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractReplLog {
  static final Logger REPL_LOG = LoggerFactory.getLogger("ReplState");
  static final ObjectMapper mapper = new ObjectMapper(); // Thread-safe.

  static {
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationConfig.Feature.AUTO_DETECT_GETTERS, false);
    mapper.configure(SerializationConfig.Feature.AUTO_DETECT_IS_GETTERS, false);
    mapper.configure(SerializationConfig.Feature.AUTO_DETECT_FIELDS, false);
  }

  public void log(LogTag tag) {
    try {
      REPL_LOG.info("{}: {}", tag.toString(), mapper.writeValueAsString(this));
    } catch (Exception exception) {
      throw new IllegalArgumentException("Could not serialize: ", exception);
    }
  }
}
