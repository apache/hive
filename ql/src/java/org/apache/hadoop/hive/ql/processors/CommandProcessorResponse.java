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

package org.apache.hadoop.hive.ql.processors;

import org.apache.hadoop.hive.metastore.api.Schema;

/**
 * Encapsulates the basic response info returned by classes the implement the
 * <code>CommandProcessor</code> interface. Typically <code>errorMessage</code>
 * and <code>SQLState</code> will only be set if the <code>responseCode</code>
 * is not 0.  Note that often {@code responseCode} ends up the exit value of
 * command shell process so should keep it to &lt; 127.
 */
public class CommandProcessorResponse {
  private final Schema schema;
  private final String message;

  public CommandProcessorResponse() {
    this(null, null);
  }

  public CommandProcessorResponse(Schema schema, String message) {
    this.schema = schema;
    this.message = message;
  }

  public Schema getSchema() {
    return schema;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return "(" + (schema == null ? "" : ", schema = " + schema + ", ") + "message = " + message + ")";
  }
}
