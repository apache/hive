/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hadoop.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StoredProcedure;

public class MStoredProc {
  private String name;
  private MDatabase database;
  private String owner;
  private String source;
  private int createTime = (int)(System.currentTimeMillis() / 1000);
  public static final int MAX_SOURCE_SIZE = 1073741823;

  public MStoredProc() {}

  public static void populate(MStoredProc result, StoredProcedure proc, MDatabase mDatabase) throws MetaException {
    result.setName(proc.getName());
    result.setOwner(proc.getOwnerName());
    result.setSource(proc.getSource());
    result.setDatabase(mDatabase);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public MDatabase getDatabase() {
    return database;
  }

  public void setDatabase(MDatabase database) {
    this.database = database;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) throws MetaException {
    if (source.length() > MAX_SOURCE_SIZE) {
      throw new MetaException("Source code is too long: " + source.length() + " max size: " + MAX_SOURCE_SIZE);
    }
    this.source = source;
  }
}
