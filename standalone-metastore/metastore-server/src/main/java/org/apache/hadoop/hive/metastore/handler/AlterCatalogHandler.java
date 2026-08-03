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

import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.AlterCatalogRequest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.GetCatalogRequest;
import org.apache.hadoop.hive.metastore.api.GetCatalogResponse;
import org.apache.hadoop.hive.metastore.events.AlterCatalogEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterCatalogEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
@RequestHandler(requestBody = AlterCatalogRequest.class)
public class AlterCatalogHandler
    extends AbstractRequestHandler<AlterCatalogRequest, AlterCatalogHandler.AlterCatalogResult> {
  private static final Logger LOG = LoggerFactory.getLogger(AlterCatalogHandler.class);
  private RawStore ms;
  private Catalog oldCat;

  AlterCatalogHandler(IHMSHandler handler, AlterCatalogRequest request) {
    super(handler, false, request);
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    this.ms = handler.getMS();
    GetCatalogResponse oldCatResp = ((HMSHandler) handler).get_catalog(new GetCatalogRequest(request.getName()));
    assert oldCatResp != null && oldCatResp.getCatalog() != null;
    this.oldCat = oldCatResp.getCatalog();
    ((HMSHandler) handler).firePreEvent(new PreAlterCatalogEvent(oldCat, request.getNewCat(), handler));
  }

  @Override
  protected AlterCatalogResult execute() throws TException, IOException {
    boolean success = false;
    Map<String, String> transactionalListenersResponses = Collections.emptyMap();
    ms.openTransaction();
    try {
      ms.alterCatalog(request.getName(), request.getNewCat());

      if (!handler.getTransactionalListeners().isEmpty()) {
        transactionalListenersResponses =
            MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(),
                EventType.ALTER_CATALOG,
                new AlterCatalogEvent(oldCat, request.getNewCat(), true, handler));
      }

      success = ms.commitTransaction();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }
    }
    return new AlterCatalogResult(success, transactionalListenersResponses);
  }

  @Override
  protected void afterExecute(AlterCatalogResult result) throws TException, IOException {
    boolean success = result != null && result.success();
    if (!handler.getListeners().isEmpty()) {
      MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
          EventType.ALTER_CATALOG,
          new AlterCatalogEvent(oldCat, request.getNewCat(), success, handler),
          null,
          result != null ? result.transactionalListenersResponses() : Collections.emptyMap(), ms);
    }
    super.afterExecute(result);
  }

  @Override
  public String toString() {
    return "AlterCatalogHandler [" + id + "] - alter catalog " + request.getName() + ":";
  }

  public record AlterCatalogResult(boolean success, Map<String, String> transactionalListenersResponses)
      implements Result {

  }
}
