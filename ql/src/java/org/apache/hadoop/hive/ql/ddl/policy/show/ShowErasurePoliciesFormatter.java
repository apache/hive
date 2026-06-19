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

package org.apache.hadoop.hive.ql.ddl.policy.show;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.formatting.MapBuilder;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Formats SHOW ERASURE POLICIES results.
 */
abstract class ShowErasurePoliciesFormatter {
  static ShowErasurePoliciesFormatter getFormatter(HiveConf conf) {
    if (MetaDataFormatUtils.isJson(conf)) {
      return new JsonShowErasurePoliciesFormatter();
    } else {
      return new TextShowErasurePoliciesFormatter();
    }
  }

  abstract void showErasurePolicies(DataOutputStream out, List<ErasurePolicySummary> policies)
      throws HiveException;

  /** Collapse any embedded tab/newline so a value can never split or end a row. */
  static String singleLine(String value) {
    if (value == null) {
      return "";
    }
    return value.replaceAll("[\\t\\r\\n]+", " ").trim();
  }

  static class JsonShowErasurePoliciesFormatter extends ShowErasurePoliciesFormatter {
    @Override
    void showErasurePolicies(DataOutputStream out, List<ErasurePolicySummary> policies) throws HiveException {
      ShowUtils.asJson(out, MapBuilder.create().put("policies", policies).build());
    }
  }

  static class TextShowErasurePoliciesFormatter extends ShowErasurePoliciesFormatter {
    @Override
    void showErasurePolicies(DataOutputStream out, List<ErasurePolicySummary> policies) throws HiveException {
      try {
        for (ErasurePolicySummary policy : policies) {
          out.writeBytes(singleLine(policy.getPolicyName()));
          out.write(Utilities.tabCode);
          out.writeBytes(singleLine(policy.getActiveVersion()));
          out.write(Utilities.tabCode);
          out.writeBytes(singleLine(policy.getVersions()));
          out.write(Utilities.newLineCode);
        }
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }
  }
}
