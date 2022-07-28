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
package org.apache.hadoop.hive.metastore.tools.metatool;
import java.util.*;

public class CatalogSummary {
    String cat_name;
    List<DatabaseSummary> database_names;

    public CatalogSummary(String cat_name, List<DatabaseSummary> database_names) {
        this.cat_name = cat_name;
        this.database_names = database_names;
    }

    public String getCat_name() {
        return cat_name;
    }

    public void setCat_name(String cat_name) {
        this.cat_name = cat_name;
    }

    public List<DatabaseSummary> getDatabase_names() {
        return database_names;
    }

    public void setDatabase_names(List<DatabaseSummary> database_names) {
        this.database_names = database_names;
    }

    @Override
    public String toString() {
        return "CatalogSummary{" +
                "cat_name='" + cat_name + '\'' +
                ", database_names=" + database_names +
                '}';
    }
}
