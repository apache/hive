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

package org.apache.hadoop.hive.llap.cli.status;

/**
 * Representing the exceptions that may occur during the Llap state chacking.
 */
class LlapStatusCliException extends Exception {
  private final ExitCode exitCode;

  LlapStatusCliException(ExitCode exitCode, String message) {
    super(exitCode.getCode() +": " + message);
    this.exitCode = exitCode;
  }

  LlapStatusCliException(ExitCode exitCode, String message, Throwable cause) {
    super(exitCode.getCode() +": " + message, cause);
    this.exitCode = exitCode;
  }

  ExitCode getExitCode() {
    return exitCode;
  }
}
