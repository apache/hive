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

package org.apache.hive.hplsql.packages;

import java.util.Optional;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.PackageRequest;
import org.apache.hadoop.hive.metastore.api.Package;
import org.apache.hive.hplsql.HplSqlSessionState;
import org.apache.thrift.TException;

public class HmsPackageRegistry implements PackageRegistry {
  private final IMetaStoreClient msc;
  private final HplSqlSessionState hplSqlSession;

  public HmsPackageRegistry(IMetaStoreClient msc, HplSqlSessionState hplSqlSession) {
    this.msc = msc;
    this.hplSqlSession = hplSqlSession;
  }

  @Override
  public Optional<String> findPackage(String name) {
    try {
      Package pkg = msc.findPackage(request(name));
      return pkg == null
              ? Optional.empty()
              : Optional.of(pkg.getHeader() + ";\n" + pkg.getBody());
    } catch (TException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public void createPackage(String name, String header) {
    try {
      msc.addPackage(makePackage(name, header, ""));
    } catch (TException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public void createPackageBody(String name, String body) {
    try {
      Package existing = msc.findPackage(request(name));
      if (existing == null) {
        msc.addPackage(makePackage(name, "", body));
      } else {
        existing.setBody(body);
        msc.addPackage(existing);
      }
    } catch (TException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  private PackageRequest request(String name) {
    return new PackageRequest(hplSqlSession.currentCatalog(), hplSqlSession.currentDatabase(), name.toUpperCase());
  }

  protected Package makePackage(String name, String header, String body) {
    return new Package(
            hplSqlSession.currentCatalog(),
            hplSqlSession.currentDatabase(),
            name.toUpperCase(),
            hplSqlSession.currentUser(),
            header,
            body);
  }
}
