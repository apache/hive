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

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.rcfile.RCFileInputDriver;
import org.apache.hcatalog.rcfile.RCFileOutputDriver;

public class AlterTableFileFormatHook extends AbstractSemanticAnalyzerHook {

  private String inDriver, outDriver, tableName;

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast) throws SemanticException {

    String inputFormat = null, outputFormat = null;
    tableName = BaseSemanticAnalyzer.unescapeIdentifier(((ASTNode)ast.getChild(0)).getChild(0).getText());
    ASTNode child =  (ASTNode)((ASTNode)ast.getChild(1)).getChild(0);

    switch (child.getToken().getType()) {
    case HiveParser.TOK_TABLEFILEFORMAT:
      inputFormat  = BaseSemanticAnalyzer.unescapeSQLString(((ASTNode) child.getChild(0)).getToken().getText());
      outputFormat = BaseSemanticAnalyzer.unescapeSQLString(((ASTNode) child.getChild(1)).getToken().getText());
      inDriver     = BaseSemanticAnalyzer.unescapeSQLString(((ASTNode) child.getChild(2)).getToken().getText());
      outDriver    = BaseSemanticAnalyzer.unescapeSQLString(((ASTNode) child.getChild(3)).getToken().getText());
      break;

    case HiveParser.TOK_TBLSEQUENCEFILE:
      throw new SemanticException("Operation not supported. HCatalog doesn't support Sequence File by default yet. " +
      "You may specify it through INPUT/OUTPUT storage drivers.");

    case HiveParser.TOK_TBLTEXTFILE:
      throw new SemanticException("Operation not supported. HCatalog doesn't support Text File by default yet. " +
      "You may specify it through INPUT/OUTPUT storage drivers.");

    case HiveParser.TOK_TBLRCFILE:
      inputFormat = RCFileInputFormat.class.getName();
      outputFormat = RCFileOutputFormat.class.getName();
      inDriver = RCFileInputDriver.class.getName();
      outDriver = RCFileOutputDriver.class.getName();
      break;
    }

    if(inputFormat == null || outputFormat == null || inDriver == null || outDriver == null){
      throw new SemanticException("File format specification in command Alter Table file format is incorrect.");
    }
    return ast;
  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
      List<Task<? extends Serializable>> rootTasks) throws SemanticException {

    Map<String,String> partSpec = ((DDLWork)rootTasks.get(rootTasks.size()-1).getWork()).getAlterTblDesc().getPartSpec();
    Map<String, String> hcatProps = new HashMap<String, String>(2);
    hcatProps.put(HCatConstants.HCAT_ISD_CLASS, inDriver);
    hcatProps.put(HCatConstants.HCAT_OSD_CLASS, outDriver);

    try {
      Hive db = context.getHive();
      Table tbl = db.getTable(tableName);
      if(partSpec == null){
        // File format is for table; not for partition.
        tbl.getTTable().getParameters().putAll(hcatProps);
        db.alterTable(tableName, tbl);
      }else{
        Partition part = db.getPartition(tbl,partSpec,false);
        Map<String,String> partParams = part.getParameters();
        if(partParams == null){
          partParams = new HashMap<String, String>();
        }
        partParams.putAll(hcatProps);
        part.getTPartition().setParameters(partParams);
        db.alterPartition(tableName, part);
      }
    } catch (HiveException he) {
      throw new SemanticException(he);
    } catch (InvalidOperationException e) {
      throw new SemanticException(e);
    }
  }
}
