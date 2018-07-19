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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.defaultNullString;

public class ListResourceProcessor implements CommandProcessor {

  private static final String LIST_COLUMN_NAME = "resource";
  private static final Schema SCHEMA;

  static {
    SCHEMA = new Schema();
    SCHEMA.addToFieldSchemas(new FieldSchema(LIST_COLUMN_NAME, STRING_TYPE_NAME, null));
    SCHEMA.putToProperties(SERIALIZATION_NULL_FORMAT, defaultNullString);
  }

  @Override
  public CommandProcessorResponse run(String command) {
    SessionState ss = SessionState.get();
    String[] tokens = command.split("\\s+");
    SessionState.ResourceType t;
    if (tokens.length < 1 || (t = SessionState.find_resource_type(tokens[0])) == null) {
      String message = "Usage: list ["
          + StringUtils.join(SessionState.ResourceType.values(), "|") + "] [<value> [<value>]*]";
      return new CommandProcessorResponse(1, message, null);
    }
    List<String> filter = null;
    if (tokens.length > 1) {
      filter = Arrays.asList(Arrays.copyOfRange(tokens, 1, tokens.length));
    }
    Set<String> s = ss.list_resource(t, filter);
    if (s != null && !s.isEmpty()) {
      ss.out.println(StringUtils.join(s, "\n"));
    }
    return new CommandProcessorResponse(0, null, null, SCHEMA);
  }

  @Override
  public void close() throws Exception {
  }
}
