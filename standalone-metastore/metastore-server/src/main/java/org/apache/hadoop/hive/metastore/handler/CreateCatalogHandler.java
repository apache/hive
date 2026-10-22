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

package org.apache.hadoop.hive.metastore.handler;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CreateCatalogRequest;
import org.apache.hadoop.hive.metastore.api.CreateDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.events.CreateCatalogEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateCatalogEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

@SuppressWarnings("unused")
@RequestHandler(requestBody = CreateCatalogRequest.class)
public class CreateCatalogHandler
    extends AbstractRequestHandler<CreateCatalogRequest, CreateCatalogHandler.CreateCatalogResult> {
  private RawStore ms;
  private Catalog catalog;

  CreateCatalogHandler(IHMSHandler handler, CreateCatalogRequest request) {
    super(handler, false, request);
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    this.catalog = request.getCatalog();
    this.ms = handler.getMS();
    try {
      ms.getCatalog(catalog.getName());
      throw new AlreadyExistsException("Catalog " + catalog.getName() + " already exists");
    } catch (NoSuchObjectException e) {
      // expected — catalog does not yet exist
    }

    if (!MetaStoreUtils.validateName(catalog.getName(), null)) {
      throw new InvalidObjectException(catalog.getName() + " is not a valid catalog name");
    }

    if (catalog.getLocationUri() == null) {
      throw new InvalidObjectException("You must specify a path for the catalog");
    }
  }

  @Override
  protected CreateCatalogResult execute() throws TException, IOException {
    boolean success = false;
    boolean madeDir = false;
    Map<String, String> transactionalListenersResponses = Collections.emptyMap();
    Path catPath = new Path(catalog.getLocationUri());
    try {
      ((HMSHandler) handler).firePreEvent(new PreCreateCatalogEvent(handler, catalog));
      if (!handler.getWh().isDir(catPath)) {
        if (!handler.getWh().mkdirs(catPath)) {
          throw new MetaException("Unable to create catalog path " + catPath +
              ", failed to create catalog " + catalog.getName());
        }
        madeDir = true;
      }
      // set the create time of catalog
      long time = System.currentTimeMillis() / 1000;
      catalog.setCreateTime((int) time);
      ms.openTransaction();
      ms.createCatalog(catalog);

      // Create a default database inside the catalog
      CreateDatabaseRequest cdr = new CreateDatabaseRequest(DEFAULT_DATABASE_NAME);
      cdr.setCatalogName(catalog.getName());
      cdr.setLocationUri(catalog.getLocationUri());
      cdr.setParameters(Collections.emptyMap());
      cdr.setDescription("Default database for catalog " + catalog.getName());
      AbstractRequestHandler.offer(handler, cdr).getResult();

      if (!handler.getTransactionalListeners().isEmpty()) {
        transactionalListenersResponses =
            MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(),
                EventType.CREATE_CATALOG,
                new CreateCatalogEvent(true, handler, catalog));
      }

      success = ms.commitTransaction();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
        if (madeDir) {
          handler.getWh().deleteDir(catPath, false, false);
        }
      }
    }
    return new CreateCatalogResult(success, transactionalListenersResponses);
  }

  @Override
  protected void afterExecute(CreateCatalogResult result) throws TException, IOException {
    boolean success = result != null && result.success();
    if (!handler.getListeners().isEmpty()) {
      MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
          EventType.CREATE_CATALOG,
          new CreateCatalogEvent(success, handler, catalog),
          null,
          result != null ? result.transactionalListenersResponses() : Collections.emptyMap(), ms);
    }
    super.afterExecute(result);
  }

  @Override
  public String toString() {
    return "CreateCatalogHandler [" + id + "] - create catalog " + catalog.getName() + ":";
  }

  public record CreateCatalogResult(boolean success,
                                    Map<String, String> transactionalListenersResponses) implements Result {

  }
}
