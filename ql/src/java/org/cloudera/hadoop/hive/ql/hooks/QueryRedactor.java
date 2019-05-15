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

package org.cloudera.hadoop.hive.ql.hooks;

import org.cloudera.log4j.redactor.StringRedactor;
import org.apache.hadoop.hive.ql.hooks.Redactor;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

public class QueryRedactor extends Redactor {

  private StringRedactor redactor;

  @Override
  public void setConf(Configuration conf) {
    String logRedactorPath = conf.get("hive.query.redaction.rules", "").trim();
    if (!logRedactorPath.isEmpty()) {
      try {
        redactor = StringRedactor.createFromJsonFile(logRedactorPath);
      } catch (Exception e) {
        String msg = "Error loading from " + logRedactorPath + ": " + e;
        throw new IllegalStateException(msg, e);
      }
    }
  }


  public String redactQuery(String query) {
    if (redactor != null) {
      return redactor.redact(query);
    }
    return query;
  }
}
