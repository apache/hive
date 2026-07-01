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

package org.apache.hadoop.hive.ql.anon.consts;

public final class AnonConst {

  private AnonConst() {
  }

  public static final String ANON_POLICY_DOC = "anon.policy.doc";
  public static final String ANON_PID_LIST = "anon.pid.list";
  public static final String ANON_INDEX_FOUND = "anon.index.found";
  public static final String ANON_MSG_OFFSET_IX = "anon.msg_offset.ix";
  public static final String ANON_MSG_ID_IX = "anon.msg_id.ix";
  public static final String ANON_BODY_IX = "anon.body.ix";
  public static final String ANON_SKIP_SWAP = "anon.skip.swap";
  public static final String ANON_INDEX_VERIFY = "anon.index.verify";

  public static final String ANON_POLICY_ENFORCE_PRIVILEGES = "anon.policy.enforce.privileges";

  public static final String PRIV_POLICY_VALIDATE = "POLICY_VALIDATE";
  public static final String PRIV_POLICY_ACTIVATE = "POLICY_ACTIVATE";
  public static final String PRIV_POLICY_MANAGE   = "POLICY_MANAGE";
  public static final String PRIV_ERASE           = "ERASE";
  public static final String PRIV_EXPORT          = "EXPORT";

  public static final String PRIV_ERASURE_ADMIN  = "ERASURE_ADMIN";

  public static final String ANON_POLICY_GRANT_ADMIN_USERS = "anon.policy.grant.admin.users";

  public static final String ANON_COLUMN_INTERNAL_FORMAT = "anon.column.internal.format";
  public static final String ANON_INDEX_TYPE = "anon.index.type";
  public static final String ANON_FILE_TYPE = "anon.file.type";

  public static final String ANON_EDGE_INDEX = "Index";
  public static final String ANON_EDGE_TBL = "TBL_IN";
  public static final String ANON_EDGE_IDX_IN = "IDX_IN";
  public static final String ANON_VERTEX_ANONYMIZER = "Anonymizer";

  public static final String ANON_COUNTER_GROUP = "DAE_ERASE";
  public static final String ANON_CTR_FILES_REWRITTEN = "FILES_REWRITTEN";

  public static final String IDENTITY_FIELD_NAME = "userId";

  public static final String ANON_INDEX_TEST_KEY_TYPE = "I";
  public static final String ANON_INDEX_TEST_POINTER_TYPE = "I";
  public static final String ANON_INDEX_TEST_VALUE_TYPES = "TLI";
}
