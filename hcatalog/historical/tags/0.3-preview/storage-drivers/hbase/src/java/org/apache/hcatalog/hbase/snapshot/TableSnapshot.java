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
package org.apache.hcatalog.hbase.snapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The snapshot for a table and a list of column families.
 */
public class TableSnapshot {

    private String name;

    private Map<String, Long> cfRevisionMap;


    public TableSnapshot(String name, Map<String, Long> cfRevMap) {
        this.name = name;
        this.cfRevisionMap = cfRevMap;
    }

    /**
     * Gets the table name.
     *
     * @return String The name of the table.
     */
    public String getTableName() {
        return name;
    }

    /**
     * Gets the column families.
     *
     * @return List<String> A list of column families associated with the snapshot.
     */
    public List<String> getColumnFamilies(){
        return  new ArrayList<String>(this.cfRevisionMap.keySet());
    }

    /**
     * Gets the revision.
     *
     * @param familyName The name of the column family.
     * @return the revision
     */
    public long getRevision(String familyName){
        return this.cfRevisionMap.get(familyName);
    }

    @Override
    public String toString() {
        String snapshot = "Table Name : " + name
                + " Column Familiy revision : " + cfRevisionMap.toString();
        return snapshot;
    }
}
