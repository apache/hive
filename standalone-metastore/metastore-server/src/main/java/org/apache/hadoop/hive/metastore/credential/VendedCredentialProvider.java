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

package org.apache.hadoop.hive.metastore.credential;

import org.apache.hadoop.classification.InterfaceStability;

import java.util.List;

/**
 * A credential-vending service.
 */
@InterfaceStability.Unstable
public interface VendedCredentialProvider {
  /**
   * Checks whether this provider supports the given access request.
   *
   * @param request the access request
   * @return true if this provider supports the given access request
   */
  boolean supports(StorageAccessRequest request);

  /**
   * Vends credentials for the given access.
   *
   * @param username the authenticated username
   * @param accessRequests the vending requests
   * @return a list of vended credentials
   */
  List<VendedStorageCredential> vend(String username, List<StorageAccessRequest> accessRequests);
}
