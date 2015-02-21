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

package org.apache.hadoop.hive.ql.udf.ptf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class NoopWithMap extends Noop
{

  @Override
  protected PTFPartition _transformRawInput(PTFPartition iPart) throws HiveException
  {
    return iPart;
  }

  public static class NoopWithMapResolver extends TableFunctionResolver
  {

    @Override
    protected TableFunctionEvaluator createEvaluator(PTFDesc ptfDesc, PartitionedTableFunctionDef tDef)
    {
      return new NoopWithMap();
    }

    @Override
    public void setupOutputOI() throws SemanticException
    {
      StructObjectInspector OI = getEvaluator().getTableDef().getInput().getOutputShape().getOI();
      setOutputOI(OI);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.hive.ql.udf.ptf.TableFunctionResolver#carryForwardNames()
     * Setting to true is correct only for special internal Functions.
     */
    @Override
    public boolean carryForwardNames() {
      return true;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.hive.ql.udf.ptf.TableFunctionResolver#getOutputNames()
     * Set to null only because carryForwardNames is true.
     */
    @Override
    public ArrayList<String> getOutputColumnNames() {
      return null;
    }

    @Override
    public void setupRawInputOI() throws SemanticException
    {
      StructObjectInspector OI = getEvaluator().getTableDef().getInput().getOutputShape().getOI();
      setRawInputOI(OI);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.hive.ql.udf.ptf.TableFunctionResolver#getOutputNames()
     * Set to null only because carryForwardNames is true.
     */
    @Override
    public ArrayList<String> getRawInputColumnNames() throws SemanticException {
      return null;
    }

    @Override
    public boolean transformsRawInput()
    {
      return true;
    }

    @Override
    public void initializeOutputOI() throws HiveException {
      setupOutputOI();
    }

    @Override
    public void initializeRawInputOI() throws HiveException {
      setupRawInputOI();
    }

  }


}
