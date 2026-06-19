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

package org.apache.hadoop.hive.ql.ddl.misc.conf;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Operation process of showing some configuration.
 */
public class ShowConfOperation extends DDLOperation<ShowConfDesc> {
  public ShowConfOperation(DDLOperationContext context, ShowConfDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException, IOException {
    ConfVars conf = HiveConf.getConfVars(desc.getConfName());
    if (conf == null) {
      throw new HiveException("invalid configuration name " + desc.getConfName());
    }
    String description = conf.getDescription();
    String defaultValue = conf.getDefaultValue();

    try (DataOutputStream output = ShowUtils.getOutputStream(desc.getResFile(), context)) {
      if (defaultValue != null) {
        output.write(defaultValue.getBytes(StandardCharsets.UTF_8));
      }
      output.write(Utilities.tabCode);
      output.write(conf.typeString().getBytes(StandardCharsets.UTF_8));
      output.write(Utilities.tabCode);
      if (description != null) {
        output.write(description.replaceAll(" *\n *", " ").getBytes(StandardCharsets.UTF_8));
      }
      output.write(Utilities.newLineCode);
    }

    return 0;
  }
}
