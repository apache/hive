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
 * Enumeration of the potential outcomes of the Llap state checking.
 */
public enum ExitCode {
  SUCCESS(0),
  INCORRECT_USAGE(10),
  YARN_ERROR(20),
  SERVICE_CLIENT_ERROR_CREATE_FAILED(30),
  SERVICE_CLIENT_ERROR_OTHER(31),
  LLAP_REGISTRY_ERROR(40),
  LLAP_JSON_GENERATION_ERROR(50),
  // Error in the script itself - likely caused by an incompatible change, or new functionality / states added.
  INTERNAL_ERROR(100);

  private final int code;

  ExitCode(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }
}
