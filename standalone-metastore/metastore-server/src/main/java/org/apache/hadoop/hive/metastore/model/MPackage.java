/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Package;
import org.apache.hadoop.hive.metastore.api.AddPackageRequest;

public class MPackage {
  private String name;
  private MDatabase database;
  private String owner;
  private String header;
  private String body;
  private int createTime = (int)(System.currentTimeMillis() / 1000);
  public static final int MAX_HEADER_SIZE = 1073741823;
  public static final int MAX_BODY_SIZE = 1073741823;

  public MPackage() {}

  public static MPackage populate(MPackage result, MDatabase mDatabase, AddPackageRequest pkg) throws MetaException {
    result.setName(pkg.getPackageName());
    result.setOwner(pkg.getOwnerName());
    result.setHeader(pkg.getHeader());
    result.setBody(pkg.getBody());
    result.setDatabase(mDatabase);
    return result;
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

  public String getHeader() {
    return header;
  }

  public void setHeader(String header) throws MetaException {
    if (header.length() > MAX_HEADER_SIZE) {
      throw new MetaException("Header is too long: " + header.length() + " max size: " + MAX_HEADER_SIZE);
    }
    this.header = header;
  }

  public String getBody() {
    return body;
  }

  public void setBody(String body) throws MetaException {
    if (body.length() > MAX_BODY_SIZE) {
      throw new MetaException("Source code is too long: " + body.length() + " max size: " + MAX_BODY_SIZE);
    }
    this.body = body;
  }

  public Package toPackage() {
    return new Package(
            getDatabase().getCatalogName(),
            getDatabase().getName(),
            getName(),
            getOwner(),
            getHeader(),
            getBody());
  }
}
