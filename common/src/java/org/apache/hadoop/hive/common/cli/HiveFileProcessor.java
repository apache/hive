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

package org.apache.hadoop.hive.common.cli;

import java.io.BufferedReader;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IOUtils;

/**
 * HiveFileProcessor is used for processing a file consist of Hive executable
 * statements
 */
public abstract class HiveFileProcessor implements IHiveFileProcessor {

  public int processFile(String fileName) throws IOException {
    BufferedReader bufferedReader = null;
    try {
      bufferedReader = loadFile(fileName);
      return (processReader(bufferedReader));
    } finally {
      IOUtils.closeStream(bufferedReader);
    }
  }

  /**
   * load commands into buffered reader from the file
   * @param fileName
   * @return
   * @throws IOException
   */
  protected abstract BufferedReader loadFile(String fileName)
      throws IOException;

  /**
   * execute the buffered reader which stores the commands
   * @param reader the buffered reader
   * @return the return code of the execution result
   * @throws IOException
   */
  protected int processReader(BufferedReader reader) throws IOException {
    String line;
    StringBuilder qsb = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      if (!line.startsWith("--")) {
        qsb.append(line);
      }
    }
    return processLine(qsb.toString());
  }

  /**
   * process the Hive command by lines
   * @param line contains a legal Hive command
   * @return the return code of the execution result
   */
  protected int processLine(String line) {
    int lastRet = 0, ret = 0;
    String command = "";
    for (String oneCmd : line.split(";")) {
      if (StringUtils.indexOf(oneCmd, "\\") != -1) {
        command += StringUtils.join(oneCmd.split("\\\\"));
      } else {
        command += oneCmd;
      }
      if (StringUtils.isBlank(command)) {
        continue;
      }

      ret = processCmd(command);
      command = "";
      lastRet = ret;
      if (ret != 0) {
        return ret;
      }
    }
    return lastRet;
  }

  /**
   * define the processor for each Hive command supported by Hive
   * @param cmd
   * @return the return code of the execution result
   */
  protected abstract int processCmd(String cmd);
}
