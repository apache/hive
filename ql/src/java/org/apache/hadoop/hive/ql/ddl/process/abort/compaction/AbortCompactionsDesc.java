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
package org.apache.hadoop.hive.ql.ddl.process.abort.compaction;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;

import java.io.Serializable;
import java.util.List;
/**
 * DDL task description for ABORT COMPACTIONS commands.
 */
@Explain(displayName = "Abort Compaction", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED })
public class AbortCompactionsDesc implements DDLDesc, Serializable {
    private static final long serialVersionUID = 1L;

    private final List<Long> compactionIds;
    private final String resFile;

    public AbortCompactionsDesc(Path resFile,List<Long> compactionIds) {
        this.compactionIds = compactionIds;
        this.resFile = resFile.toString();
    }


    @Explain(displayName = "Compactions IDs", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED})
    public List<Long> getCompactionIds() {
        return compactionIds;
    }
    public String getResFile() {
        return resFile;
    }
}