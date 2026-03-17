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

package org.apache.hadoop.hive.metastore.events;

import com.google.common.collect.Lists;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ClientCapabilities;
import org.apache.hadoop.hive.metastore.api.ClientCapability;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class ReloadEvent extends ListenerEvent {
    private final Table tableObj;
    private final Partition ptnObj; // for backward compatibility
    private final List<Partition> ptns;
    private final boolean refreshEvent;

    /**
     * @param catName name of the catalog
     * @param db name of the database the table is in
     * @param table name of the table being inserted into
     * @param partVals list of partition values, can be null
     * @param status status of insert, true = success, false = failure
     * @param refreshEvent status of insert,
     * @param handler handler that is firing the event
     */
    public ReloadEvent(String catName, String db, String table, List<List<String>> partVals, boolean status,
            boolean refreshEvent, Map<String, String> tblParams, IHMSHandler handler) throws MetaException,
            NoSuchObjectException {
        super(status, handler);

        GetTableRequest req = new GetTableRequest(db, table);
        req.setCatName(catName);
        req.setCapabilities(new ClientCapabilities(
                Lists.newArrayList(ClientCapability.TEST_CAPABILITY, ClientCapability.INSERT_ONLY_TABLES)));
        try {
            this.tableObj = handler.get_table_req(req).getTable();
            if (tblParams != null) {
                this.tableObj.getParameters().putAll(tblParams);
            }
            if (partVals != null) {
                if (partVals.size() == 1) {
                    this.ptnObj = handler.get_partition(MetaStoreUtils.prependNotNullCatToDbName(catName, db),
                        table, partVals.get(0));
                    this.ptns = null;
                } else {
                    this.ptnObj = null;
                    this.ptns = new ArrayList<>();
                    List<String> part_names = new ArrayList<>();
                    for (List<String> partVal : partVals) {
                        part_names.add(Warehouse.makePartName(this.tableObj.getPartitionKeys(), partVal));
                    }
                    GetPartitionsByNamesRequest partitionsReq = new GetPartitionsByNamesRequest(
                        MetaStoreUtils.prependNotNullCatToDbName(catName, db), table);
                    partitionsReq.setNames(part_names);
                    partitionsReq.setGet_col_stats(false);
                    this.ptns.addAll(handler.get_partitions_by_names_req(partitionsReq).getPartitions());
                }
            } else {
                this.ptnObj = null;
                this.ptns = null;
            }
        } catch (NoSuchObjectException e) {
            // This is to mimic previous behavior where NoSuchObjectException was thrown through this
            // method.
            throw e;
        } catch (TException e) {
            throw MetaStoreUtils.newMetaException(e);
        }
        // mark this event as refresh table[partition] event. Refresh table [partition] query is fired from impala.
        this.refreshEvent = refreshEvent;
    }

    /**
     * @return Table object
     */
    public Table getTableObj() {
        return tableObj;
    }

    /**
     * @return Partition object
     */
    public Partition getPartitionObj() {
        return ptnObj;
    }

    /**
     * @return List of Partition objects
     */
    public List<Partition> getPartitions() {
        return ptns;
    }

    /**
     * @return The refresh event flag.
     */
    public boolean isRefreshEvent() {
        return refreshEvent;
    }
}
