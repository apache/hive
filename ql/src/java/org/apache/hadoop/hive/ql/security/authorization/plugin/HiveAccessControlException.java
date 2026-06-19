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

package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Exception thrown by the Authorization plugin api (v2). Indicates
 * a authorization check denying permissions for an action.
 */
@LimitedPrivate(value = { "Apache Argus (incubating)" })
@Evolving
public class HiveAccessControlException extends HiveException{

  private static final long serialVersionUID = 1L;

  public HiveAccessControlException(){
  }

  public HiveAccessControlException(String msg){
    super(msg);
  }

  public HiveAccessControlException(String msg, Throwable cause){
    super(msg, cause);
  }

  public HiveAccessControlException(Throwable cause){
    super(cause);
  }
}
