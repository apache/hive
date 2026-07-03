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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ErasurePolicy;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyVersion;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.DataOutputStream;
import java.util.List;

/**
 * Operation process of describing a data erasure policy.
 */
public class DescErasurePolicyOperation extends DDLOperation<DescErasurePolicyDesc> {
  public DescErasurePolicyOperation(DDLOperationContext context, DescErasurePolicyDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    try (DataOutputStream outStream = ShowUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      ErasurePolicy policy = context.getDb().getErasurePolicy(desc.getPolicyName());
      if (policy == null) {
        throw new HiveException(ErrorMsg.GENERIC_ERROR,
            "DESCRIBE ERASURE POLICY: policy not found: " + desc.getPolicyName());
      }

      // Version lifecycle for the governance header (active version, per-version
      // states, and — when EXTENDED — validation/activation provenance).
      List<ErasurePolicyVersion> versions =
          context.getDb().listErasurePolicyVersions(desc.getPolicyName());

      DescErasurePolicyFormatter formatter = DescErasurePolicyFormatter.getFormatter(context.getConf());
      formatter.showErasurePolicyDescription(outStream, policy, versions, desc.getVersionLabel(), desc.isExtended());
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR);
    }

    return 0;
  }
}
