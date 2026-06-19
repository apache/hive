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

package org.apache.hadoop.hive.ql.ddl.policy.desc;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ErasurePolicy;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyVersion;
import org.apache.hadoop.hive.metastore.api.PolicyVersionStatus;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.formatting.MapBuilder;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Formats DESC ERASURE POLICY results.
 */
abstract class DescErasurePolicyFormatter {
  static DescErasurePolicyFormatter getFormatter(HiveConf conf) {
    if (MetaDataFormatUtils.isJson(conf)) {
      return new JsonDescErasurePolicyFormatter();
    } else {
      return new TextDescErasurePolicyFormatter();
    }
  }

  abstract void showErasurePolicyDescription(DataOutputStream out, ErasurePolicy policy,
      List<ErasurePolicyVersion> versions, String selectVersion, boolean extended) throws HiveException;

  /**
   * Render a policy as a single text column: a short governance header followed
   * by the policy source, one line per row. Shared by the text formatter and the
   * unit test.
   */
  static List<String> renderLines(ErasurePolicy policy, List<ErasurePolicyVersion> versions,
      String selectVersion, boolean extended) {
    List<String> lines = new ArrayList<>();
    lines.add("Policy:    " + nullToDash(policy.getPolicyName()));

    String active = "(none)";
    ErasurePolicyVersion activeVersion = null;
    StringBuilder summary = new StringBuilder();
    if (versions != null) {
      versions.sort(Comparator.comparingLong(ErasurePolicyVersion::getVersionId));
      for (ErasurePolicyVersion v : versions) {
        if (summary.length() > 0) {
          summary.append(", ");
        }
        summary.append(v.getVersionLabel()).append('(').append(v.getStatus()).append(')');
        if (v.getStatus() == PolicyVersionStatus.ACTIVE) {
          active = v.getVersionLabel();
          activeVersion = v;
        }
      }
    }
    lines.add("Active:    " + active);
    lines.add("Versions:  " + (summary.length() == 0 ? "-" : summary.toString()));

    // Choose which version's body to render: an explicit VERSION selector, or the
    // active version by default. (Previously the policy-header doc -- the first
    // LOAD's source -- was rendered regardless of which version was active.)
    ErasurePolicyVersion target;
    if (selectVersion != null) {
      target = null;
      if (versions != null) {
        for (ErasurePolicyVersion v : versions) {
          if (selectVersion.equals(v.getVersionLabel())) {
            target = v;
            break;
          }
        }
      }
      if (target == null) {
        lines.add("");
        lines.add("(no version '" + selectVersion + "' on policy '"
            + nullToDash(policy.getPolicyName()) + "')");
        return lines;
      }
      lines.add("Showing:   " + target.getVersionLabel() + " (" + target.getStatus() + ")");
    } else {
      target = activeVersion;
    }

    if (extended && target != null) {
      if (target.getIdentityFieldName() != null) {
        lines.add("Identity:  " + target.getIdentityFieldName()
            + " (" + target.getIdentityFieldType() + ")");
      }
      lines.add("Schema:    " + target.getSchemaType());
      if (target.getValidatedBy() != null) {
        lines.add("Validated: " + target.getValidatedBy());
      }
      if (target.getActivatedBy() != null) {
        lines.add("Activated: " + target.getActivatedBy());
      }
      if (target.getSourceChecksum() != null) {
        lines.add("Source SHA-256: " + target.getSourceChecksum());
      }
    }

    lines.add("");
    // Render the chosen version's own .erp source (the single source of truth).
    String doc = (target != null) ? target.getSourceText() : null;
    if (doc != null) {
      // Each source line becomes its own single-column row. Tabs would be read as
      // column separators, so neutralise them; trailing blank lines are dropped.
      for (String line : doc.split("\n")) {
        lines.add(line.replace('\t', ' '));
      }
    }
    return lines;
  }

  private static String nullToDash(String s) {
    return s == null ? "-" : s;
  }

  // ------ Implementations ------

  static class JsonDescErasurePolicyFormatter extends DescErasurePolicyFormatter {
    @Override
    void showErasurePolicyDescription(DataOutputStream out, ErasurePolicy policy,
        List<ErasurePolicyVersion> versions, String selectVersion, boolean extended) throws HiveException {
      ShowUtils.asJson(out, MapBuilder.create().put("policy", policy).put("versions", versions).build());
    }
  }

  static class TextDescErasurePolicyFormatter extends DescErasurePolicyFormatter {
    @Override
    void showErasurePolicyDescription(DataOutputStream out, ErasurePolicy policy,
        List<ErasurePolicyVersion> versions, String selectVersion, boolean extended) throws HiveException {
      try {
        for (String line : renderLines(policy, versions, selectVersion, extended)) {
          out.write(line.getBytes(StandardCharsets.UTF_8));
          out.write(Utilities.newLineCode);
        }
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }
  }
}
