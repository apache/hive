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

package org.apache.hadoop.hive.ql.anon.tez;

import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;

public class RowContext {

  private final int msgIdIx;
  private final int msgOffsetIx;
  private final int bodyIx;
  private final String idFieldName;
  private final ColumnInternalFormat internalFormat;

  public RowContext(final int msgIdIx, final int msgOffsetIx, final int bodyIx, final String idFieldName, final ColumnInternalFormat internalFormat) {
    this.msgIdIx = msgIdIx;
    this.msgOffsetIx = msgOffsetIx;
    this.bodyIx = bodyIx;
    this.idFieldName = idFieldName;
    this.internalFormat = internalFormat;
  }

  public int getMsgIdIx() {
    return msgIdIx;
  }

  public int getMsgOffsetIx() {
    return msgOffsetIx;
  }

  public int getBodyIx() {
    return bodyIx;
  }

  public String getIdFieldName() {
    return idFieldName;
  }

  public ColumnInternalFormat getInternalFormat() {
    return internalFormat;
  }
}
