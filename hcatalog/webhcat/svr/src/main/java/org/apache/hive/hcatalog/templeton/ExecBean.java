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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

/**
 * ExecBean - The results of an exec call.
 */
public class ExecBean {
  public String stdout;
  public String stderr;
  public int exitcode;

  public ExecBean() {}

  /**
   * Create a new ExecBean.
   *
   * @param stdout     standard output of the the program.
   * @param stderr     error output of the the program.
   * @param exitcode   exit code of the program.
   */
  public ExecBean(String stdout, String stderr, int exitcode) {
    this.stdout = stdout;
    this.stderr = stderr;
    this.exitcode = exitcode;
  }

  public String toString() {
    return String.format("ExecBean(stdout=%s, stderr=%s, exitcode=%s)",
               stdout, stderr, exitcode);
  }
}
