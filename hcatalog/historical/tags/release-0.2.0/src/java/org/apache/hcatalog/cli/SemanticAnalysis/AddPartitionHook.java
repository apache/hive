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
package org.apache.hcatalog.cli.SemanticAnalysis;

import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hcatalog.common.HCatConstants;

public class AddPartitionHook extends AbstractSemanticAnalyzerHook{

  private String tblName, inDriver, outDriver;

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
      throws SemanticException {
    Map<String, String> tblProps;
    tblName = ast.getChild(0).getText();
    try {
      tblProps = context.getHive().getTable(tblName).getParameters();
    } catch (HiveException he) {
      throw new SemanticException(he);
    }

    inDriver = tblProps.get(HCatConstants.HCAT_ISD_CLASS);
    outDriver = tblProps.get(HCatConstants.HCAT_OSD_CLASS);

    if(inDriver == null  || outDriver == null){
      throw new SemanticException("Operation not supported. Partitions can be added only in a table created through HCatalog. " +
      		"It seems table "+tblName+" was not created through HCatalog.");
    }
    return ast;
  }

//  @Override
//  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
//      List<Task<? extends Serializable>> rootTasks) throws SemanticException {
//
//    try {
//      Hive db = context.getHive();
//      Table tbl = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName);
//      for(Task<? extends Serializable> task : rootTasks){
//        System.err.println("PArt spec: "+((DDLWork)task.getWork()).getAddPartitionDesc().getPartSpec());
//        Partition part = db.getPartition(tbl,((DDLWork)task.getWork()).getAddPartitionDesc().getPartSpec(),false);
//        Map<String,String> partParams = part.getParameters();
//        if(partParams == null){
//          System.err.println("Part map null ");
//          partParams = new HashMap<String, String>();
//        }
//        partParams.put(InitializeInput.HOWL_ISD_CLASS, inDriver);
//        partParams.put(InitializeInput.HOWL_OSD_CLASS, outDriver);
//        part.getTPartition().setParameters(partParams);
//        db.alterPartition(tblName, part);
//      }
//    } catch (HiveException he) {
//      throw new SemanticException(he);
//    } catch (InvalidOperationException e) {
//      throw new SemanticException(e);
//    }
//  }
}



