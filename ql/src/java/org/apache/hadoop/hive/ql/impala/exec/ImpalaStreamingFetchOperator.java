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
package org.apache.hadoop.hive.ql.impala.exec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.exec.FetchOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.service.rpc.thrift.TRowSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *  Specialized FetchOperator for streaming results from an Impala coordinator.
 */
public class ImpalaStreamingFetchOperator extends FetchOperator {
    private static final Logger LOG = LoggerFactory.getLogger(ImpalaStreamingFetchOperator.class);

    /* Last row returned by this operator */
    private transient final InspectableObject inspectable = new InspectableObject();
    /* Inspector for output of this operator */
    private final StructObjectInspector outputInspector;
    /* Holds context required to communicate with Impala */
    private ImpalaFetchContext context;
    /* Last returned rowSet */
    private TRowSet rowSet = null;
    /* Next position in rowSet */
    private int rowSetPosition = 0;

    public ImpalaStreamingFetchOperator(FetchWork work, JobConf job, Operator<?> operator,
                                        List<VirtualColumn> vcCols, Schema resultSchema) throws HiveException {
        super(work, job, operator, vcCols);

        List<ObjectInspector> columnOIs = new ArrayList<>(resultSchema.getFieldSchemasSize());
        List<String> columnNames =  new ArrayList<>(resultSchema.getFieldSchemasSize());
        // iterate over the result schema and create the appropriate primitive inspectors
        for (FieldSchema schema : resultSchema.getFieldSchemas()) {
            columnNames.add(schema.getName());
            columnOIs.add(ImpalaThriftInspectorFactory.getImpalaThriftObjectInspector(
                    TypeInfoFactory.getPrimitiveTypeInfo(schema.getType())));
        }
        outputInspector = ImpalaResultInspector.getImpalaResultInspector(columnNames, columnOIs);
    }

    /// CDPD-6964: Investigate ACID support in Impala streaming
    @Override
    public void setValidWriteIdList(String writeIdStr) {

    }

    @Override
    public FetchWork getWork() {
        return super.getWork();
    }

    @Override
    public void setWork(FetchWork work) {
        super.setWork(work);
    }

    @Override
    public boolean pushRow() throws IOException, HiveException {
        InspectableObject row = getNextRow();
        if (row != null) {
            pushRow(row);
            flushRow();
            return true;
        }
        return false;
    }

    @Override
    protected void pushRow(InspectableObject row) throws HiveException {
        super.pushRow(row);
    }

    @Override
    protected void flushRow() throws HiveException {
        super.flushRow();
    }

    @Override
    public InspectableObject getNextRow() throws IOException {
        try {
            if (rowSet == null || rowSetPosition >= rowSet.getRowsSize()) {
                rowSetPosition = 0;
                rowSet = context.fetch();
            }
        } catch (Exception e) {
            throw new IOException(e);
        }

        if (rowSet == null || rowSet.getRowsSize() <= 0) {
            return null;
        }

        inspectable.oi = outputInspector;
        inspectable.o = rowSet.getRows().get(rowSetPosition);
        rowSetPosition++;
        return inspectable;
    }

    @Override
    public void clearFetchContext() throws HiveException {
        if (!HiveConf.getBoolVar(SessionState.get().getConf(), HiveConf.ConfVars.HIVE_IMPALA_ROW_FETCH_EARLY_CLOSE)) {

          context.close();
        }
    }

    @Override
    public void closeOperator() throws HiveException {
        // Fetch driver ends up calling closeOperator multiple times to flush results. In streaming mode we want to
        // only call close once.
        if (HiveConf.getBoolVar(SessionState.get().getConf(), HiveConf.ConfVars.HIVE_IMPALA_ROW_FETCH_EARLY_CLOSE)) {

          context.close();
        }
        rowSet = null;
    }

    @Override
    public ObjectInspector getOutputObjectInspector() {
        return outputInspector;
    }

    @Override
    public Configuration getJobConf() {
        return super.getJobConf();
    }

    public void setImpalaFetchContext(ImpalaFetchContext context) {
        this.context = context;
    }
}
