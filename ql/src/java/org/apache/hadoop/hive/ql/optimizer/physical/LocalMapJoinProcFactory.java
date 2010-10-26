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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.JDBMDummyOperator;
import org.apache.hadoop.hive.ql.exec.JDBMSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.physical.MapJoinResolver.LocalMapJoinProcCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.JDBMDummyDesc;
import org.apache.hadoop.hive.ql.plan.JDBMSinkDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;

/**
 * Node processor factory for skew join resolver.
 */
public final class LocalMapJoinProcFactory {



  public static NodeProcessor getJoinProc() {
    return new LocalMapJoinProcessor();
  }
  public static NodeProcessor getMapJoinMapJoinProc() {
    return new MapJoinMapJoinProc();
  }
  public static NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs)
          throws SemanticException {
        return null;
      }
    };
  }

  /**
   * LocalMapJoinProcessor.
   *
   */
  public static class LocalMapJoinProcessor implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      LocalMapJoinProcCtx context = (LocalMapJoinProcCtx) ctx;

      if(!nd.getName().equals("MAPJOIN")){
        return null;
      }
      MapJoinOperator mapJoinOp = (MapJoinOperator) nd;

      //create an new operator: JDBMSinkOperator
      JDBMSinkDesc jdbmSinkDesc = new JDBMSinkDesc(mapJoinOp.getConf());
      JDBMSinkOperator jdbmSinkOp =(JDBMSinkOperator)OperatorFactory.get(jdbmSinkDesc);


      //get the last operator for processing big tables
      int bigTable = mapJoinOp.getConf().getPosBigTable();
      Byte[] order = mapJoinOp.getConf().getTagOrder();
      int bigTableAlias=(int)order[bigTable];

      Operator<? extends Serializable> bigOp = mapJoinOp.getParentOperators().get(bigTable);

      //the parent ops for jdbmSinkOp
      List<Operator<?extends Serializable>> smallTablesParentOp= new ArrayList<Operator<?extends Serializable>>();

      List<Operator<?extends Serializable>> dummyOperators= new ArrayList<Operator<?extends Serializable>>();
      //get all parents
      List<Operator<? extends Serializable> >  parentsOp = mapJoinOp.getParentOperators();
      for(int i = 0; i<parentsOp.size();i++){
        if(i == bigTableAlias){
          smallTablesParentOp.add(null);
          continue;
        }

        Operator<? extends Serializable> parent = parentsOp.get(i);
        //let jdbmOp be the child of this parent
        parent.replaceChild(mapJoinOp, jdbmSinkOp);
        //keep the parent id correct
        smallTablesParentOp.add(parent);

        //create an new operator: JDBMDummyOpeator, which share the table desc
        JDBMDummyDesc desc = new JDBMDummyDesc();
        JDBMDummyOperator dummyOp =(JDBMDummyOperator)OperatorFactory.get(desc);
        TableDesc tbl;

        if(parent.getSchema()==null){
          if(parent instanceof TableScanOperator ){
            tbl = ((TableScanOperator)parent).getTableDesc();
         }else{
           throw new SemanticException();
         }
        }else{
          //get parent schema
          RowSchema rowSchema = parent.getSchema();
          tbl = PlanUtils.getIntermediateFileTableDesc(PlanUtils
              .getFieldSchemasFromRowSchema(rowSchema, ""));
        }


        dummyOp.getConf().setTbl(tbl);

        //let the dummy op  be the parent of mapjoin op
        mapJoinOp.replaceParent(parent, dummyOp);
        List<Operator<? extends Serializable>> dummyChildren = new ArrayList<Operator<? extends Serializable>>();
        dummyChildren.add(mapJoinOp);
        dummyOp.setChildOperators(dummyChildren);

        //add this dummy op to the dummp operator list
        dummyOperators.add(dummyOp);

      }

      jdbmSinkOp.setParentOperators(smallTablesParentOp);
      for(Operator<? extends Serializable> op: dummyOperators){
        context.addDummyParentOp(op);
      }
      return null;
    }

  }

  /**
   * LocalMapJoinProcessor.
   *
   */
  public static class MapJoinMapJoinProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      LocalMapJoinProcCtx context = (LocalMapJoinProcCtx) ctx;
      if(!nd.getName().equals("MAPJOIN")){
        return null;
      }
      System.out.println("Mapjoin * MapJoin");

      return null;
    }
  }


  private LocalMapJoinProcFactory() {
    // prevent instantiation
  }
 }

