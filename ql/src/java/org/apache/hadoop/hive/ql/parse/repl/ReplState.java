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
package org.apache.hadoop.hive.ql.parse.repl;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReplState.
 *
 * Logger class for Repl Events.
 **/
public abstract class ReplState {
  private static final Logger REPL_LOG = LoggerFactory.getLogger("ReplState");

  private static final ObjectMapper mapper = new ObjectMapper(); // Thread-safe.

  static {
    mapper.configure(MapperFeature.AUTO_DETECT_GETTERS, false);
    mapper.configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
    mapper.configure(MapperFeature.AUTO_DETECT_FIELDS, false);
  }

  public enum LogTag {
    START,
    TABLE_DUMP,
    FUNCTION_DUMP,
    EVENT_DUMP,
    ATLAS_DUMP_START,
    ATLAS_DUMP_END,
    RANGER_DUMP_START,
    RANGER_DUMP_END,
    TABLE_LOAD,
    FUNCTION_LOAD,
    EVENT_LOAD,
    ATLAS_LOAD_START,
    ATLAS_LOAD_END,
    RANGER_LOAD_START,
    RANGER_LOAD_END,
    END,
    DATA_COPY_END
  }

  public void log(LogTag tag) {
    try {
      REPL_LOG.info("REPL::{}: {}", tag.name(), mapper.writeValueAsString(this));
    } catch (Exception exception) {
      REPL_LOG.error("Could not serialize REPL log: {}", exception.getMessage());
    }
  }
}
