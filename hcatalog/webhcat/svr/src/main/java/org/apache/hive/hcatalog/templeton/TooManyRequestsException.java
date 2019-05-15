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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

/**
 * Raise this exception if web service is busy with existing requests and not able
 * service new requests.
 */
public class TooManyRequestsException extends SimpleWebException {
  /*
   * The current version of jetty server doesn't have the status
   * HttpStatus.TOO_MANY_REQUESTS_429. Hence, passing this as constant.
   */
  public static int TOO_MANY_REQUESTS_429 = 429;

  public TooManyRequestsException(String msg) {
    super(TOO_MANY_REQUESTS_429, msg);
  }
}
