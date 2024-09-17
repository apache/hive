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
import org.apache.hadoop.hive.metastore.api.AbortCompactResponse;
import org.apache.hadoop.hive.metastore.api.AbortCompactionRequest;
import org.apache.hadoop.hive.metastore.api.AbortCompactionResponseElement;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.DataOutputStream;
import java.io.IOException;


/**
 * Operation process of aborting compactions.
 */
public class AbortCompactionsOperation extends DDLOperation<AbortCompactionsDesc> {
    public AbortCompactionsOperation(DDLOperationContext context, AbortCompactionsDesc desc) {
        super(context, desc);
    }

    @Override
    public int execute() throws HiveException {
        AbortCompactionRequest request = new AbortCompactionRequest();
        request.setCompactionIds(desc.getCompactionIds());
        AbortCompactResponse response = context.getDb().abortCompactions(request);
        try (DataOutputStream os = ShowUtils.getOutputStream(new Path(desc.getResFile()), context)) {
            writeHeader(os);
            if (response.getAbortedcompacts() != null) {
                for (AbortCompactionResponseElement e : response.getAbortedcompacts().values()) {
                    writeRow(os, e);
                }
            }
        } catch (Exception e) {
            LOG.warn("abort compactions: ", e);
            return 1;
        }
        return 0;
    }

    private void writeHeader(DataOutputStream os) throws IOException {
        os.writeBytes("CompactionId");
        os.write(Utilities.tabCode);
        os.writeBytes("Status");
        os.write(Utilities.tabCode);
        os.writeBytes("Message");
        os.write(Utilities.newLineCode);

    }

    private void writeRow(DataOutputStream os, AbortCompactionResponseElement e) throws IOException {
        os.writeBytes(Long.toString(e.getCompactionId()));
        os.write(Utilities.tabCode);
        os.writeBytes(e.getStatus());
        os.write(Utilities.tabCode);
        os.writeBytes(e.getMessage());
        os.write(Utilities.newLineCode);

    }
}
