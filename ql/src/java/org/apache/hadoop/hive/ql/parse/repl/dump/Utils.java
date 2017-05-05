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
package org.apache.hadoop.hive.ql.parse.repl.dump;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.io.IOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class Utils {
  public static void writeOutput(List<String> values, Path outputFile, HiveConf hiveConf)
      throws SemanticException {
    DataOutputStream outStream = null;
    try {
      FileSystem fs = outputFile.getFileSystem(hiveConf);
      outStream = fs.create(outputFile);
      outStream.writeBytes((values.get(0) == null ? Utilities.nullStringOutput : values.get(0)));
      for (int i = 1; i < values.size(); i++) {
        outStream.write(Utilities.tabCode);
        outStream.writeBytes((values.get(i) == null ? Utilities.nullStringOutput : values.get(i)));
      }
      outStream.write(Utilities.newLineCode);
    } catch (IOException e) {
      throw new SemanticException(e);
    } finally {
      IOUtils.closeStream(outStream);
    }
  }
}
