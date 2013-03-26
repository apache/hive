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
package org.apache.hcatalog.pig;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.Pair;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hcatalog.mapreduce.HCatTableInfo;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.Expression.BinaryExpression;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.UDFContext;

/**
 * Pig {@link LoadFunc} to read data from Howl
 */

public class HCatLoader extends LoadFunc implements LoadMetadata, LoadPushDown {

  private static final String PRUNE_PROJECTION_INFO = "prune.projection.info";
  private static final String PARTITION_FILTER = "partition.filter"; // for future use

  private HCatInputFormat howlInputFormat = null;
  private RecordReader<?, ?> reader;
  private String dbName;
  private String tableName;
  private String howlServerUri;
  private String signature;
  private String partitionFilterString;
  private final PigHCatUtil phutil = new PigHCatUtil();

  HCatSchema outputSchema = null;

  @Override
  public InputFormat<?,?> getInputFormat() throws IOException {
    if(howlInputFormat == null) {
      howlInputFormat = new HCatInputFormat();
    }
    return howlInputFormat;
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      HCatRecord hr =  (HCatRecord) (reader.nextKeyValue() ? reader.getCurrentValue() : null);
      Tuple t = PigHCatUtil.transformToTuple(hr,outputSchema);
      // TODO : we were discussing an iter interface, and also a LazyTuple
      // change this when plans for that solidifies.
      return t;
    } catch (ExecException e) {
      int errCode = 6018;
      String errMsg = "Error while reading input";
      throw new ExecException(errMsg, errCode,
          PigException.REMOTE_ENVIRONMENT, e);
    } catch (Exception eOther){
      int errCode = 6018;
      String errMsg = "Error converting read value to tuple";
      throw new ExecException(errMsg, errCode,
          PigException.REMOTE_ENVIRONMENT, eOther);
    }

  }

  @SuppressWarnings("unchecked")
  @Override
  public void prepareToRead(RecordReader reader, PigSplit arg1) throws IOException {
    this.reader = reader;
  }

  @Override
  public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
    return location;
  }

@Override
  public void setLocation(String location, Job job) throws IOException {

    Pair<String, String> dbTablePair = PigHCatUtil.getDBTableNames(location);
    dbName = dbTablePair.first;
    tableName = dbTablePair.second;

    // get partitionFilterString stored in the UDFContext - it would have
    // been stored there by an earlier call to setPartitionFilter
    // call setInput on OwlInputFormat only in the frontend because internally
    // it makes calls to the owl server - we don't want these to happen in
    // the backend
    // in the hadoop front end mapred.task.id property will not be set in
    // the Configuration
    if (!HCatUtil.checkJobContextIfRunningFromBackend(job)){

      HCatInputFormat.setInput(job, HCatTableInfo.getInputTableInfo(
              howlServerUri!=null ? howlServerUri :
                  (howlServerUri = PigHCatUtil.getHowlServerUri(job)),
              PigHCatUtil.getHowlServerPrincipal(job),
              dbName,
              tableName,
              getPartitionFilterString()));
    }

    // Need to also push projections by calling setOutputSchema on
    // OwlInputFormat - we have to get the RequiredFields information
    // from the UdfContext, translate it to an Schema and then pass it
    // The reason we do this here is because setLocation() is called by
    // Pig runtime at InputFormat.getSplits() and
    // InputFormat.createRecordReader() time - we are not sure when
    // OwlInputFormat needs to know about pruned projections - so doing it
    // here will ensure we communicate to OwlInputFormat about pruned
    // projections at getSplits() and createRecordReader() time

    UDFContext udfContext = UDFContext.getUDFContext();
    Properties props = udfContext.getUDFProperties(this.getClass(),
        new String[]{signature});
    RequiredFieldList requiredFieldsInfo =
      (RequiredFieldList)props.get(PRUNE_PROJECTION_INFO);
    if(requiredFieldsInfo != null) {
      // convert to owlschema and pass to OwlInputFormat
      try {
        outputSchema = phutil.getHCatSchema(requiredFieldsInfo.getFields(),signature,this.getClass());
        HCatInputFormat.setOutputSchema(job, outputSchema);
      } catch (Exception e) {
        throw new IOException(e);
      }
    } else{
      // else - this means pig's optimizer never invoked the pushProjection
      // method - so we need all fields and hence we should not call the
      // setOutputSchema on OwlInputFormat
      if (HCatUtil.checkJobContextIfRunningFromBackend(job)){
        try {
          HCatSchema howlTableSchema = (HCatSchema) props.get(HCatConstants.HCAT_TABLE_SCHEMA);
          outputSchema = howlTableSchema;
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }
  }

  @Override
  public String[] getPartitionKeys(String location, Job job)
  throws IOException {
    Table table = phutil.getTable(location,
        howlServerUri!=null?howlServerUri:PigHCatUtil.getHowlServerUri(job),
            PigHCatUtil.getHowlServerPrincipal(job));
    List<FieldSchema> tablePartitionKeys = table.getPartitionKeys();
    String[] partitionKeys = new String[tablePartitionKeys.size()];
    for(int i = 0; i < tablePartitionKeys.size(); i++) {
      partitionKeys[i] = tablePartitionKeys.get(i).getName();
    }
    return partitionKeys;
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    Table table = phutil.getTable(location,
        howlServerUri!=null?howlServerUri:PigHCatUtil.getHowlServerUri(job),
            PigHCatUtil.getHowlServerPrincipal(job));
    HCatSchema howlTableSchema = HCatUtil.getTableSchemaWithPtnCols(table);
    try {
      PigHCatUtil.validateHowlTableSchemaFollowsPigRules(howlTableSchema);
    } catch (IOException e){
      throw new PigException(
          "Table schema incompatible for reading through HowlLoader :" + e.getMessage()
          + ";[Table schema was "+ howlTableSchema.toString() +"]"
          ,PigHCatUtil.PIG_EXCEPTION_CODE, e);
    }
    storeInUDFContext(signature, HCatConstants.HCAT_TABLE_SCHEMA, howlTableSchema);
    outputSchema = howlTableSchema;
    return PigHCatUtil.getResourceSchema(howlTableSchema);
  }

  @Override
  public ResourceStatistics getStatistics(String location, Job job) throws IOException {
    // statistics not implemented currently
    return null;
  }

  @Override
  public void setPartitionFilter(Expression partitionFilter) throws IOException {
    // convert the partition filter expression into a string expected by
    // howl and pass it in setLocation()

    partitionFilterString = getHowlComparisonString(partitionFilter);

    // store this in the udf context so we can get it later
    storeInUDFContext(signature,
        PARTITION_FILTER, partitionFilterString);
  }

  @Override
  public List<OperatorSet> getFeatures() {
    return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
  }

  @Override
  public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldsInfo) throws FrontendException {
    // Store the required fields information in the UDFContext so that we
    // can retrieve it later.
    storeInUDFContext(signature, PRUNE_PROJECTION_INFO, requiredFieldsInfo);

    // Howl will always prune columns based on what we ask of it - so the
    // response is true
    return new RequiredFieldResponse(true);
  }

  @Override
  public void setUDFContextSignature(String signature) {
    this.signature = signature;
  }


  // helper methods
  private void storeInUDFContext(String signature, String key, Object value) {
    UDFContext udfContext = UDFContext.getUDFContext();
    Properties props = udfContext.getUDFProperties(
        this.getClass(), new String[] {signature});
    props.put(key, value);
  }


  private String getPartitionFilterString() {
    if(partitionFilterString == null) {
      Properties props = UDFContext.getUDFContext().getUDFProperties(
          this.getClass(), new String[] {signature});
      partitionFilterString = props.getProperty(PARTITION_FILTER);
    }
    return partitionFilterString;
  }

  private String getHowlComparisonString(Expression expr) {
    if(expr instanceof BinaryExpression){
      // call getOwlComparisonString on lhs and rhs, and and join the
      // results with OpType string

      // we can just use OpType.toString() on all Expression types except
      // Equal, NotEqualt since Equal has '==' in toString() and
      // we need '='
      String opStr = null;
      switch(expr.getOpType()){
        case OP_EQ :
          opStr = " = ";
          break;
        default:
          opStr = expr.getOpType().toString();
      }
      BinaryExpression be = (BinaryExpression)expr;
      return "(" + getHowlComparisonString(be.getLhs()) +
                  opStr +
                  getHowlComparisonString(be.getRhs()) + ")";
    } else {
      // should be a constant or column
      return expr.toString();
    }
  }

}
