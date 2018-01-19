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

package org.apache.hadoop.hive.ql.security.authorization;

import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;

/**
 * Hive's pluggable authorization provider interface
 */
public interface HiveAuthorizationProvider extends Configurable{

  public void init(Configuration conf) throws HiveException;

  public HiveAuthenticationProvider getAuthenticator();

  public void setAuthenticator(HiveAuthenticationProvider authenticator);

  /**
   * Authorization user level privileges.
   *
   * @param readRequiredPriv
   *          a list of privileges needed for inputs.
   * @param writeRequiredPriv
   *          a list of privileges needed for outputs.
   * @throws HiveException
   * @throws AuthorizationException
   */
  public void authorize(Privilege[] readRequiredPriv,
      Privilege[] writeRequiredPriv) throws HiveException,
      AuthorizationException;

  /**
   * Authorization privileges against a database object.
   *
   * @param db
   *          database
   * @param readRequiredPriv
   *          a list of privileges needed for inputs.
   * @param writeRequiredPriv
   *          a list of privileges needed for outputs.
   * @throws HiveException
   * @throws AuthorizationException
   */
  public void authorize(Database db, Privilege[] readRequiredPriv,
      Privilege[] writeRequiredPriv) throws HiveException,
      AuthorizationException;

  /**
   * Authorization privileges against a hive table object.
   *
   * @param table
   *          table object
   * @param readRequiredPriv
   *          a list of privileges needed for inputs.
   * @param writeRequiredPriv
   *          a list of privileges needed for outputs.
   * @throws HiveException
   * @throws AuthorizationException
   */
  public void authorize(Table table, Privilege[] readRequiredPriv,
      Privilege[] writeRequiredPriv) throws HiveException,
      AuthorizationException;

  /**
   * Authorization privileges against a hive partition object.
   *
   * @param part
   *          partition object
   * @param readRequiredPriv
   *          a list of privileges needed for inputs.
   * @param writeRequiredPriv
   *          a list of privileges needed for outputs.
   * @throws HiveException
   * @throws AuthorizationException
   */
  public void authorize(Partition part, Privilege[] readRequiredPriv,
      Privilege[] writeRequiredPriv) throws HiveException,
      AuthorizationException;

  /**
   * Authorization privileges against a list of columns. If the partition object
   * is not null, look at the column grants for the given partition. Otherwise
   * look at the table column grants.
   *
   * @param table
   *          table object
   * @param part
   *          partition object
   * @param columns
   *          a list of columns
   * @param readRequiredPriv
   *          a list of privileges needed for inputs.
   * @param writeRequiredPriv
   *          a list of privileges needed for outputs.
   * @throws HiveException
   * @throws AuthorizationException
   */
  public void authorize(Table table, Partition part, List<String> columns,
      Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException;

}
