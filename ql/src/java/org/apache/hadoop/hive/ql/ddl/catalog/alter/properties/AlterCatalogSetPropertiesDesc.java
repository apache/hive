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

package org.apache.hadoop.hive.ql.ddl.catalog.alter.properties;

import org.apache.hadoop.hive.ql.ddl.catalog.alter.AbstractAlterCatalogDesc;
import org.apache.hadoop.hive.ql.plan.Explain;

import java.util.Map;

/**
 * DDL task description for ALTER CATALOG ... SET PROPERTIES ... commands.
 */
@Explain(displayName = "Set Catalog Properties", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED })
public class AlterCatalogSetPropertiesDesc extends AbstractAlterCatalogDesc {
    private static final long serialVersionUID = 1L;

    private final Map<String, String> catProperties;

    public AlterCatalogSetPropertiesDesc(String catalogName, Map<String, String> catProperties) {
        super(catalogName);
        this.catProperties = catProperties;
    }

    @Explain(displayName="properties")
    public Map<String, String> getCatalogProperties() {
        return catProperties;
    }
}
