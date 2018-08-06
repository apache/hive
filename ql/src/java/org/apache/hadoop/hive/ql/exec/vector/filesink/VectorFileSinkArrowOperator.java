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

package org.apache.hadoop.hive.ql.exec.vector.filesink;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.TerminalOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.ql.plan.VectorFileSinkDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.io.arrow.ArrowWrapperWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.hive.llap.LlapOutputFormatService;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.hive.ql.io.arrow.Serializer;
import static org.apache.hadoop.hive.llap.LlapOutputFormat.LLAP_OF_ID_KEY;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;

/**
 * Native Vectorized File Sink operator implementation for Arrow.
 * Assumes output to LlapOutputFormatService
 **/
public class VectorFileSinkArrowOperator extends TerminalOperator<FileSinkDesc>
    implements Serializable, VectorizationOperator {

  private static final long serialVersionUID = 1L;

  private VectorizationContext vContext;
  private VectorFileSinkDesc vectorDesc;
  public static final Logger LOG = LoggerFactory.getLogger(VectorFileSinkArrowOperator.class.getName());

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------
  
  private transient Serializer converter;
  private transient RecordWriter recordWriter;
  private transient boolean wroteData;
  private transient String attemptId;

  public VectorFileSinkArrowOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) {
    this(ctx);
    this.conf = (FileSinkDesc) conf;
    this.vContext = vContext;
    this.vectorDesc = (VectorFileSinkDesc) vectorDesc;
  }

  /** Kryo ctor. */
  @VisibleForTesting
  public VectorFileSinkArrowOperator() {
    super();
  }

  public VectorFileSinkArrowOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  public VectorizationContext getInputVectorizationContext() {
    return vContext;
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    //attemptId identifies a RecordWriter initialized by LlapOutputFormatService
    this.attemptId = hconf.get(LLAP_OF_ID_KEY);
    try {
      //Initialize column names and types
      List<TypeInfo> typeInfos = new ArrayList<>();
      List<String> fieldNames = new ArrayList<>();
      StructObjectInspector schema = (StructObjectInspector) inputObjInspectors[0];
      for(int i = 0; i < schema.getAllStructFieldRefs().size(); i++) {
        StructField structField = schema.getAllStructFieldRefs().get(i);
        fieldNames.add(structField.getFieldName());
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(structField.getFieldObjectInspector());
        typeInfos.add(typeInfo);
      }
      //Initialize an Arrow serializer
      converter = new Serializer(hconf, attemptId, typeInfos, fieldNames);
    } catch (Exception e) {
      LOG.error("Unable to initialize VectorFileSinkArrowOperator");
      throw new RuntimeException(e);
    }
  }

  @Override
  public void process(Object data, int tag) throws HiveException {
    //ArrowStreamReader expects at least the schema metadata, if this op writes no data,
    //we need to send the schema to close the stream gracefully
    VectorizedRowBatch batch = (VectorizedRowBatch) data;
    try {
      if(recordWriter == null) {
        recordWriter = LlapOutputFormatService.get().getWriter(this.attemptId);
      }
      //Convert the VectorizedRowBatch to a handle for the Arrow batch
      ArrowWrapperWritable writable = converter.serializeBatch(batch, true);
      //Pass the handle to the LlapOutputFormatService recordWriter
      recordWriter.write(null, writable);
      this.wroteData = true;
    } catch(Exception e) {
      LOG.error("Failed to convert VectorizedRowBatch to Arrow batch");
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
    try {
      if(!wroteData) {
        //Send a schema only batch to signal EOS with no data written
        ArrowWrapperWritable writable = converter.emptyBatch();
        if(recordWriter == null) {
          recordWriter = LlapOutputFormatService.get().getWriter(this.attemptId);
        }
        recordWriter.write(null, writable);
      }
    } catch(Exception e) {
      LOG.error("Failed to write Arrow stream schema");
      throw new RuntimeException(e);
    } finally {
      try {
        //Close the recordWriter with null Reporter
        recordWriter.close(null);
      } catch(Exception e) {
        LOG.error("Failed to close Arrow stream");
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public VectorDesc getVectorDesc() {
    return vectorDesc;
  }

  @Override
  public OperatorType getType() {
    return OperatorType.FILESINK;
  }
}

