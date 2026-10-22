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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.DropCatalogRequest;
import org.apache.hadoop.hive.metastore.api.DropDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.events.DropCatalogEvent;
import org.apache.hadoop.hive.metastore.events.PreDropCatalogEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.thrift.TException;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.prependNotNullCatToDbName;

@SuppressWarnings("unused")
@RequestHandler(requestBody = DropCatalogRequest.class)
public class DropCatalogHandler
    extends AbstractRequestHandler<DropCatalogRequest, DropCatalogHandler.DropCatalogResult> {
  private RawStore ms;
  private Catalog cat;
  private String catName;

  DropCatalogHandler(IHMSHandler handler, DropCatalogRequest request) {
    super(handler, false, request);
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    this.catName = request.getName();
    this.ms = handler.getMS();
    if (DEFAULT_CATALOG_NAME.equalsIgnoreCase(catName)) {
      throw new MetaException("Can not drop " + DEFAULT_CATALOG_NAME + " catalog");
    }
  }

  @Override
  protected DropCatalogResult execute() throws TException, IOException {
    boolean success = false;
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    try {
      ms.openTransaction();
      cat = ms.getCatalog(catName);

      ((HMSHandler) handler).firePreEvent(new PreDropCatalogEvent(handler, cat));

      List<String> allDbs = ((HMSHandler) handler).get_databases(prependNotNullCatToDbName(catName, null));
      if (allDbs != null && !allDbs.isEmpty()) {
        // Only the default database may remain; drop it if it is empty
        if (allDbs.size() == 1 && allDbs.get(0).equals(DEFAULT_DATABASE_NAME)) {
          try {
            DropDatabaseRequest req = new DropDatabaseRequest();
            req.setName(DEFAULT_DATABASE_NAME);
            req.setCatalogName(catName);
            req.setDeleteData(true);
            req.setCascade(false);
            ((HMSHandler) handler).drop_database_req(req);
          } catch (InvalidOperationException e) {
            throw new InvalidOperationException("There are still objects in the default " +
                "database for catalog " + catName);
          }
        } else {
          throw new InvalidOperationException("There are non-default databases in the catalog " +
              catName + " so it cannot be dropped.");
        }
      }

      ms.dropCatalog(catName);
      if (!handler.getTransactionalListeners().isEmpty()) {
        transactionalListenerResponses =
            MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(),
                EventType.DROP_CATALOG,
                new DropCatalogEvent(true, handler, cat));
      }

      success = ms.commitTransaction();
    } catch (NoSuchObjectException e) {
      if (!request.isIfExists()) {
        throw new NoSuchObjectException(e.getMessage());
      }
      success = true;
    } finally {
      if (success && cat != null) {
        handler.getWh().deleteDir(handler.getWh().getDnsPath(new Path(cat.getLocationUri())), false, false);
      } else {
        ms.rollbackTransaction();
      }
    }
    return new DropCatalogResult(success, transactionalListenerResponses);
  }

  @Override
  protected void afterExecute(DropCatalogResult result) throws TException, IOException {
    if (!handler.getListeners().isEmpty()) {
      MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
          EventType.DROP_CATALOG,
          new DropCatalogEvent(result != null && result.success(), handler, cat),
          null,
          result != null ? result.transactionalListenerResponses() : Collections.emptyMap(), ms);
    }
    super.afterExecute(result);
  }

  @Override
  public String toString() {
    return "DropCatalogHandler [" + id + "] - drop catalog " + catName + ":";
  }

  public record DropCatalogResult(boolean success,
                                  Map<String, String> transactionalListenerResponses) implements Result {

  }
}
