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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AddPackageRequest;
import org.apache.hadoop.hive.metastore.api.DropPackageRequest;
import org.apache.hadoop.hive.metastore.api.GetPackageRequest;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
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
  public Optional<String> getPackage(String name) {
    try {
      Package pkg = findPackage(name);
      return pkg == null
              ? Optional.empty()
              : Optional.of(pkg.getHeader() + ";\n" + pkg.getBody());
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createPackageHeader(String name, String header, boolean replace) {
    try {
      Package existing = findPackage(name);
      if (existing != null && !replace)
        throw new RuntimeException("Package " + name + " already exists");
      msc.addPackage(makePackage(name, header, ""));
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createPackageBody(String name, String body, boolean replace) {
    try {
      Package existing = findPackage(name);
      if (existing == null || StringUtils.isEmpty(existing.getHeader()))
        throw new RuntimeException("Package header does not exists " + name);
      if (StringUtils.isNotEmpty(existing.getBody()) && !replace)
        throw new RuntimeException("Package body " + name + " already exists");
      msc.addPackage(makePackage(name, existing.getHeader(), body));
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private Package findPackage(String name) throws TException {
    try {
      return msc.findPackage(request(name));
    } catch (NoSuchObjectException e) {
      return null;
    }
  }

  @Override
  public void dropPackage(String name) {
    try {
      msc.dropPackage(new DropPackageRequest(hplSqlSession.currentCatalog(), hplSqlSession.currentDatabase(), name));
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private GetPackageRequest request(String name) {
    return new GetPackageRequest(hplSqlSession.currentCatalog(), hplSqlSession.currentDatabase(), name.toUpperCase());
  }

  private AddPackageRequest makePackage(String name, String header, String body) {
    return new AddPackageRequest(
            hplSqlSession.currentCatalog(),
            hplSqlSession.currentDatabase(),
            name.toUpperCase(),
            hplSqlSession.currentUser(),
            header,
            body);
  }
}
