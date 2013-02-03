package org.apache.hadoop.hive.ql.udf.ptf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PTFDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.TableFuncDef;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class Noop extends TableFunctionEvaluator
{

  @Override
  public PTFPartition execute(PTFPartition iPart) throws HiveException
  {
    return iPart;
  }

  @Override
  protected void execute(PTFPartitionIterator<Object> pItr, PTFPartition oPart)
  {
    throw new UnsupportedOperationException();
  }

  public static class NoopResolver extends TableFunctionResolver
  {

    @Override
    protected TableFunctionEvaluator createEvaluator(PTFDef qDef, TableFuncDef tDef)
    {
      return new Noop();
    }

    @Override
    public void setupOutputOI() throws SemanticException
    {
      StructObjectInspector OI = getEvaluator().getTableDef().getInput().getOI();
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
    public boolean transformsRawInput()
    {
      return false;
    }

    @Override
    public void initializeOutputOI() throws HiveException {
      setupOutputOI();

    }

  }

}