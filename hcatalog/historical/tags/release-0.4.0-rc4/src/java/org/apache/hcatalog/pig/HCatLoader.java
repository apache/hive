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
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.Pair;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.apache.pig.Expression;
import org.apache.pig.Expression.BinaryExpression;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.impl.util.UDFContext;

/**
 * Pig {@link LoadFunc} to read data from HCat
 */

public class HCatLoader extends HCatBaseLoader {

  private static final String PARTITION_FILTER = "partition.filter"; // for future use

  private HCatInputFormat hcatInputFormat = null;
  private String dbName;
  private String tableName;
  private String hcatServerUri;
  private String partitionFilterString;
  private final PigHCatUtil phutil = new PigHCatUtil();
  
  // Signature for wrapped loader, see comments in LoadFuncBasedInputDriver.initialize
  final public static String INNER_SIGNATURE = "hcatloader.inner.signature";
  final public static String INNER_SIGNATURE_PREFIX = "hcatloader_inner_signature";

  @Override
  public InputFormat<?,?> getInputFormat() throws IOException {
    if(hcatInputFormat == null) {
      hcatInputFormat = new HCatInputFormat();
    }
    return hcatInputFormat;
  }

  @Override
  public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
    return location;
  }

@Override
  public void setLocation(String location, Job job) throws IOException {

    job.getConfiguration().set(INNER_SIGNATURE, INNER_SIGNATURE_PREFIX + "_" + signature);
    Pair<String, String> dbTablePair = PigHCatUtil.getDBTableNames(location);
    dbName = dbTablePair.first;
    tableName = dbTablePair.second;

    // get partitionFilterString stored in the UDFContext - it would have
    // been stored there by an earlier call to setPartitionFilter
    // call setInput on HCatInputFormat only in the frontend because internally
    // it makes calls to the hcat server - we don't want these to happen in
    // the backend
    // in the hadoop front end mapred.task.id property will not be set in
    // the Configuration
    if (!HCatUtil.checkJobContextIfRunningFromBackend(job)){
      HCatInputFormat.setInput(job,
                               InputJobInfo.create(dbName,
                                                   tableName,
                                                   getPartitionFilterString()));
    }

    // Need to also push projections by calling setOutputSchema on
    // HCatInputFormat - we have to get the RequiredFields information
    // from the UdfContext, translate it to an Schema and then pass it
    // The reason we do this here is because setLocation() is called by
    // Pig runtime at InputFormat.getSplits() and
    // InputFormat.createRecordReader() time - we are not sure when
    // HCatInputFormat needs to know about pruned projections - so doing it
    // here will ensure we communicate to HCatInputFormat about pruned
    // projections at getSplits() and createRecordReader() time

    UDFContext udfContext = UDFContext.getUDFContext();
    Properties props = udfContext.getUDFProperties(this.getClass(),
        new String[]{signature});
    RequiredFieldList requiredFieldsInfo =
      (RequiredFieldList)props.get(PRUNE_PROJECTION_INFO);
    if(requiredFieldsInfo != null) {
      // convert to hcatschema and pass to HCatInputFormat
      try {
        outputSchema = phutil.getHCatSchema(requiredFieldsInfo.getFields(),signature,this.getClass());
        HCatInputFormat.setOutputSchema(job, outputSchema);
      } catch (Exception e) {
        throw new IOException(e);
      }
    } else{
      // else - this means pig's optimizer never invoked the pushProjection
      // method - so we need all fields and hence we should not call the
      // setOutputSchema on HCatInputFormat
      if (HCatUtil.checkJobContextIfRunningFromBackend(job)){
        try {
          HCatSchema hcatTableSchema = (HCatSchema) props.get(HCatConstants.HCAT_TABLE_SCHEMA);
          outputSchema = hcatTableSchema;
          HCatInputFormat.setOutputSchema(job, outputSchema);
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
        hcatServerUri!=null?hcatServerUri:PigHCatUtil.getHCatServerUri(job),
            PigHCatUtil.getHCatServerPrincipal(job));
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
        hcatServerUri!=null?hcatServerUri:PigHCatUtil.getHCatServerUri(job),
            PigHCatUtil.getHCatServerPrincipal(job));
    HCatSchema hcatTableSchema = HCatUtil.getTableSchemaWithPtnCols(table);
    try {
      PigHCatUtil.validateHCatTableSchemaFollowsPigRules(hcatTableSchema);
    } catch (IOException e){
      throw new PigException(
          "Table schema incompatible for reading through HCatLoader :" + e.getMessage()
          + ";[Table schema was "+ hcatTableSchema.toString() +"]"
          ,PigHCatUtil.PIG_EXCEPTION_CODE, e);
    }
    storeInUDFContext(signature, HCatConstants.HCAT_TABLE_SCHEMA, hcatTableSchema);
    outputSchema = hcatTableSchema;
    return PigHCatUtil.getResourceSchema(hcatTableSchema);
  }

  @Override
  public void setPartitionFilter(Expression partitionFilter) throws IOException {
    // convert the partition filter expression into a string expected by
    // hcat and pass it in setLocation()

    partitionFilterString = getHCatComparisonString(partitionFilter);

    // store this in the udf context so we can get it later
    storeInUDFContext(signature,
        PARTITION_FILTER, partitionFilterString);
  }

  private String getPartitionFilterString() {
    if(partitionFilterString == null) {
      Properties props = UDFContext.getUDFContext().getUDFProperties(
          this.getClass(), new String[] {signature});
      partitionFilterString = props.getProperty(PARTITION_FILTER);
    }
    return partitionFilterString;
  }

  private String getHCatComparisonString(Expression expr) {
    if(expr instanceof BinaryExpression){
      // call getHCatComparisonString on lhs and rhs, and and join the
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
      return "(" + getHCatComparisonString(be.getLhs()) +
                  opStr +
                  getHCatComparisonString(be.getRhs()) + ")";
    } else {
      // should be a constant or column
      return expr.toString();
    }
  }

}
