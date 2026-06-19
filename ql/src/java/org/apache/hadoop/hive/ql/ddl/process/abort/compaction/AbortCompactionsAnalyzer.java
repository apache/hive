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

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.ArrayList;
import java.util.List;

@DDLSemanticAnalyzerFactory.DDLType(types = HiveParser.TOK_ABORT_COMPACTIONS)
public class AbortCompactionsAnalyzer extends BaseSemanticAnalyzer {
    public AbortCompactionsAnalyzer(QueryState queryState) throws SemanticException {
        super(queryState);
    }

    @Override
    public void analyzeInternal(ASTNode root) throws SemanticException {
        ctx.setResFile(ctx.getLocalTmpPath());
        List<Long> compactionIds = new ArrayList<>();
        for (Node child : root.getChildren()) {
            compactionIds.add(Long.parseLong(((Tree)child).getText()));
        }
        AbortCompactionsDesc desc = new AbortCompactionsDesc(ctx.getResFile(),compactionIds);
        rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
    }

}
