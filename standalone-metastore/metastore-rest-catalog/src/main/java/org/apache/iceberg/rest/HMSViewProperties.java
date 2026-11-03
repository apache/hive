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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveHadoopUtil;
import org.apache.iceberg.hive.MetadataLocator;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ImmutableLoadViewResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.view.ViewMetadata;

/**
 * The view properties naming state HMS keeps outside the Iceberg view metadata: the recorded owner
 * ({@link HiveCatalog#HMS_TABLE_OWNER}) and the authorization marker ({@link HiveCatalog#VIEW_AUTHORIZED_PROPERTY}).
 * {@link HMSCatalogAdapter} drops them from view requests and sets them from HMS in view responses.
 */
final class HMSViewProperties {
  // The HMS behind the catalog, if any: the source of the view state reported to clients (see withViewAuthorization)
  private final MetadataLocator metadataLocator;
  // Whether the catalog honors a client-requested owner; otherwise the owner property is stripped from requests
  private final boolean clientSuppliedOwnerEnabled;

  /**
   * @param catalog the catalog served, a {@link HiveCatalog} possibly wrapped by an {@link HMSCachingCatalog};
   *     any other catalog has no HMS behind it
   */
  HMSViewProperties(Catalog catalog) {
    HiveCatalog hiveCatalog;
    if (catalog instanceof HMSCachingCatalog cachingCatalog) {
      hiveCatalog = cachingCatalog.getCatalog();
    } else {
      hiveCatalog = catalog instanceof HiveCatalog hive ? hive : null;
    }
    this.metadataLocator = hiveCatalog == null ? null : new MetadataLocator(hiveCatalog);
    this.clientSuppliedOwnerEnabled = hiveCatalog != null && hiveCatalog.getConf() != null &&
        hiveCatalog.getConf().getBoolean(HiveHadoopUtil.CLIENT_SUPPLIED_OWNER_ENABLED,
            HiveHadoopUtil.CLIENT_SUPPLIED_OWNER_ENABLED_DEFAULT);
  }

  /**
   * Whether a view property is reserved for state HMS manages: the authorization marker, under either name, and
   * the owner unless client-supplied owners are enabled.
   */
  private boolean isReservedViewProperty(String key) {
    return HiveCatalog.isViewAuthorizationProperty(key) ||
        (!clientSuppliedOwnerEnabled && HiveCatalog.HMS_TABLE_OWNER.equalsIgnoreCase(key));
  }

  private Map<String, String> withoutReservedProperties(Map<String, String> properties) {
    Map<String, String> kept = new HashMap<>(properties);
    kept.keySet().removeIf(this::isReservedViewProperty);
    return kept;
  }

  /**
   * Drops the reserved properties (see {@link #isReservedViewProperty}) from a view creation, so that they are not
   * stored in the Iceberg view metadata file. Dropping rather than rejecting keeps clients that send back the
   * properties of a loaded view working.
   */
  CreateViewRequest withoutReservedProperties(CreateViewRequest request) {
    if (request.properties().keySet().stream().noneMatch(this::isReservedViewProperty)) {
      return request;
    }
    return ImmutableCreateViewRequest.builder()
        .from(request)
        .properties(withoutReservedProperties(request.properties()))
        .build();
  }

  /** Drops the reserved properties from the property updates of a view update; see the creation overload. */
  UpdateTableRequest withoutReservedProperties(UpdateTableRequest request) {
    List<MetadataUpdate> updates = new ArrayList<>(request.updates().size());
    boolean stripped = false;
    for (MetadataUpdate update : request.updates()) {
      if (update instanceof MetadataUpdate.SetProperties setProperties &&
          setProperties.updated().keySet().stream().anyMatch(this::isReservedViewProperty)) {
        stripped = true;
        Map<String, String> kept = withoutReservedProperties(setProperties.updated());
        if (!kept.isEmpty()) {
          updates.add(new MetadataUpdate.SetProperties(kept));
        }
      } else {
        updates.add(update);
      }
    }
    return stripped ? UpdateTableRequest.create(request.identifier(), request.requirements(), updates) : request;
  }

  /**
   * Reports the view state HMS keeps outside the Iceberg view metadata, overwriting whatever the view properties
   * hold: {@link HiveCatalog#VIEW_AUTHORIZED_PROPERTY}, so that HiveServer2 reading through the REST catalog knows
   * when to authorize the reader on the underlying tables, and {@link HiveCatalog#HMS_TABLE_OWNER}, the recorded
   * owner. Without an HMS behind the catalog, the view is reported as not authorized.
   * <p>Views are not cached (see {@link HMSCachingCatalog#loadView}); this state is fetched from HMS for every view
   * response, limited to the owner and parameters.</p>
   */
  LoadViewResponse withViewAuthorization(TableIdentifier ident, LoadViewResponse response) {
    org.apache.hadoop.hive.metastore.api.Table hmsView =
        metadataLocator == null ? null : metadataLocator.getOwnerAndParameters(ident);
    Map<String, String> state = viewState(hmsView, response.metadataLocation());
    ViewMetadata.Builder builder = ViewMetadata.buildFrom(response.metadata()).setProperties(state);
    if (metadataLocator != null && !state.containsKey(HiveCatalog.HMS_TABLE_OWNER)) {
      // HMS records no owner for this view: do not report the one from the metadata file
      builder.removeProperties(Set.of(HiveCatalog.HMS_TABLE_OWNER));
    }
    return ImmutableLoadViewResponse.builder().from(response).metadata(builder.build()).build();
  }

  /**
   * Computes the view state to report for a view response. The HMS object is read after the view metadata, so the
   * view is reported as authorized only if HMS records it so for the metadata version returned; otherwise, or if
   * HMS has no such view, it is reported as not authorized.
   *
   * @param hmsView the HMS view holding at least the owner and parameters, or null if HMS has none
   * @param metadataLocation the metadata location of the view metadata returned
   * @return the {@link HiveCatalog#VIEW_AUTHORIZED_PROPERTY} value and, when known, the
   *     {@link HiveCatalog#HMS_TABLE_OWNER}
   */
  @VisibleForTesting
  static Map<String, String> viewState(org.apache.hadoop.hive.metastore.api.Table hmsView, String metadataLocation) {
    Map<String, String> state = new HashMap<>();
    boolean authorized = false;
    if (hmsView != null) {
      Map<String, String> parameters = hmsView.getParameters();
      String hmsLocation = parameters == null ? null :
          parameters.get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
      authorized = metadataLocation != null && Objects.equals(hmsLocation, metadataLocation) &&
          HiveCatalog.isViewAuthorized(hmsView);
      if (hmsView.getOwner() != null) {
        state.put(HiveCatalog.HMS_TABLE_OWNER, hmsView.getOwner());
      }
    }
    state.put(HiveCatalog.VIEW_AUTHORIZED_PROPERTY, String.valueOf(authorized));
    return state;
  }
}
