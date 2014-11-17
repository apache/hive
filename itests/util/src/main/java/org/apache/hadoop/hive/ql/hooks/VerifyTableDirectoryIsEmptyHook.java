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
package org.apache.hadoop.hive.ql.hooks;

import java.util.Arrays;
import java.io.IOException;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.session.SessionState;

// This hook verifies that the location of every output table is empty
public class VerifyTableDirectoryIsEmptyHook implements ExecuteWithHookContext {

  public void run(HookContext hookContext) throws IOException {
    for (WriteEntity output : hookContext.getOutputs()) {
      Path tableLocation = new Path(output.getTable().getDataLocation().toString());
      FileSystem fs = tableLocation.getFileSystem(SessionState.get().getConf());
      assert(fs.listStatus(tableLocation, FileUtils.HIDDEN_FILES_PATH_FILTER).length == 0);
    }
  }
}
