package org.apache.hadoop.hive.ql.udf.ptf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc.TableFuncDef;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class NoopWithMap extends Noop
{
  @Override
  public PTFPartition execute(PTFPartition iPart) throws HiveException
  {
    return iPart;
  }

  @Override
  protected PTFPartition _transformRawInput(PTFPartition iPart) throws HiveException
  {
    return iPart;
  }

  public static class NoopWithMapResolver extends TableFunctionResolver
  {

    @Override
    protected TableFunctionEvaluator createEvaluator(PTFDesc ptfDesc, TableFuncDef tDef)
    {
      return new NoopWithMap();
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
    public void setupRawInputOI() throws SemanticException
    {
      StructObjectInspector OI = getEvaluator().getTableDef().getInput().getOI();
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
