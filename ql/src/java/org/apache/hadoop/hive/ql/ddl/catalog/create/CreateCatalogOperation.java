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

package org.apache.hadoop.hive.ql.ddl.catalog.create;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.Optional;

/**
 * Operation process of creating a catalog.
 */
public class CreateCatalogOperation extends DDLOperation<CreateCatalogDesc> {
  public CreateCatalogOperation(DDLOperationContext context, CreateCatalogDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws Exception {
    Catalog catalog = new Catalog(desc.getName(), desc.getLocationUri());
    catalog.setDescription(desc.getComment());
    Optional.ofNullable(desc.getCatlogProperties())
            .ifPresent(catalog::setParameters);
    try {
      context.getDb().createCatalog(catalog, desc.isIfNotExists());
    } catch (AlreadyExistsException e) {
      throw new HiveException(e, ErrorMsg.CATALOG_ALREADY_EXISTS, desc.getName());
    }
    return 0;
  }
}
