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

package org.apache.hadoop.hive.common.cli;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hive.common.util.StreamPrinter;

public class ShellCmdExecutor {
  private String cmd;
  private PrintStream out;
  private PrintStream err;

  public ShellCmdExecutor(String cmd, PrintStream out, PrintStream err) {
    this.cmd = cmd;
    this.out = out;
    this.err = err;
  }

  public int execute() throws Exception {
    try {
      Process executor = Runtime.getRuntime().exec(cmd);
      StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, out);
      StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, err);

      outPrinter.start();
      errPrinter.start();

      int ret = executor.waitFor();
      outPrinter.join();
      errPrinter.join();
      return ret;
    } catch (IOException ex) {
      throw new Exception("Failed to execute " + cmd, ex);
    }
  }

}
