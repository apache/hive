/**
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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;

/**
 * FetchTask implementation
 **/
public class FetchTask extends Task<fetchWork> implements Serializable {
  private static final long serialVersionUID = 1L;

  private int maxRows = 100;
  private FetchOperator ftOp;
 	private LazySimpleSerDe mSerde;
 	private int totalRows;
  
  public void initialize (HiveConf conf) {
    super.initialize(conf);
    
   	try {
       // Create a file system handle  
       JobConf job = new JobConf(conf, ExecDriver.class);

       mSerde = new LazySimpleSerDe();
       Properties mSerdeProp = new Properties();
       mSerdeProp.put(Constants.SERIALIZATION_FORMAT, "" + Utilities.tabCode);
       mSerdeProp.put(Constants.SERIALIZATION_NULL_FORMAT, ((fetchWork)work).getSerializationNullFormat());
       mSerde.initialize(job, mSerdeProp);
       
       ftOp = new FetchOperator(work, job);
    } catch (Exception e) {
      // Bail out ungracefully - we should never hit
      // this here - but would have hit it in SemanticAnalyzer
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException (e);
    }
  }
  
  public int execute() {
    assert false;
    return 0;
  }
  /**
   * Return the tableDesc of the fetchWork
   */
  public tableDesc getTblDesc() {
    return work.getTblDesc();
  }

  /**
   * Return the maximum number of rows returned by fetch
   */
  public int getMaxRows() {
    return maxRows;
  }

  /**
   * Set the maximum number of rows returned by fetch
   */
  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }
	
  public boolean fetch(Vector<String> res) {
    try {
      int numRows = 0;
      int rowsRet = maxRows;
      if ((work.getLimit() >= 0) && ((work.getLimit() - totalRows) < rowsRet))
        rowsRet = work.getLimit() - totalRows;
      if (rowsRet <= 0) {
        ftOp.clearFetchContext();
        return false;
      }

    	while (numRows < rowsRet) {
    	  InspectableObject io = ftOp.getNextRow();
    	  if (io == null) {
          if (numRows == 0) 
            return false;
          totalRows += numRows;
          return true;
    	  } 
  	    
    	  res.add(((Text)mSerde.serialize(io.o, io.oi)).toString());
   	    numRows++;
    	}
      totalRows += numRows;
      return true;
    }
    catch (Exception e) {
      console.printError("Failed with exception " + e.getClass().getName() + ":" +   e.getMessage(), "\n" + StringUtils.stringifyException(e));
      return false;
    }
  }
}
