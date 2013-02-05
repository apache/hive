package org.apache.hadoop.hive.ql.udf.ptf;

import static org.apache.hadoop.hive.ql.exec.PTFUtils.sprintf;

import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;
import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc.TableFuncDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Based on Hive {@link GenericUDAFEvaluator}. Break up the responsibility of the old AsbtractTableFunction
 * class into a Resolver and Evaluator.
 * <p>
 * The Evaluator also holds onto the {@link TableFunctionDef}. This provides information
 * about the arguments to the function, the shape of the Input partition and the Partitioning details.
 * The Evaluator is responsible for providing the 2 execute methods:
 * <ol>
 * <li><b>execute:</b> which is invoked after the input is partitioned; the contract
 * is, it is given an input Partition and must return an output Partition. The shape of the output
 * Partition is obtained from the getOutputOI call.
 * <li><b>transformRawInput:</b> In the case where this function indicates that it will transform the raw input
 * before it is fed through the partitioning mechanics, this function is called. Again the contract is
 * t is given an input Partition and must return an Partition. The shape of the output Partition is
 * obtained from getRawInputOI() call.
 * </ol>
 *
 */
public abstract class TableFunctionEvaluator
{
  transient protected StructObjectInspector OI;
  transient protected StructObjectInspector rawInputOI;
  protected TableFuncDef tDef;
  protected PTFDesc ptfDesc;
  String partitionClass;
  int partitionMemSize;
  boolean transformsRawInput;

  static{
    PTFUtils.makeTransient(TableFunctionEvaluator.class, "OI");
    PTFUtils.makeTransient(TableFunctionEvaluator.class, "rawInputOI");
  }


  public StructObjectInspector getOutputOI()
  {
    return OI;
  }

  protected void setOutputOI(StructObjectInspector outputOI)
  {
    OI = outputOI;
  }

  public TableFuncDef getTableDef()
  {
    return tDef;
  }

  public void setTableDef(TableFuncDef tDef)
  {
    this.tDef = tDef;
  }

  protected PTFDesc getQueryDef()
  {
    return ptfDesc;
  }

  protected void setQueryDef(PTFDesc ptfDesc)
  {
    this.ptfDesc = ptfDesc;
  }

  public String getPartitionClass()
  {
    return partitionClass;
  }

  public void setPartitionClass(String partitionClass)
  {
    this.partitionClass = partitionClass;
  }

  public int getPartitionMemSize()
  {
    return partitionMemSize;
  }

  public void setPartitionMemSize(int partitionMemSize)
  {
    this.partitionMemSize = partitionMemSize;
  }

  public StructObjectInspector getRawInputOI()
  {
    return rawInputOI;
  }

  protected void setRawInputOI(StructObjectInspector rawInputOI)
  {
    this.rawInputOI = rawInputOI;
  }

  public boolean isTransformsRawInput() {
    return transformsRawInput;
  }

  public void setTransformsRawInput(boolean transformsRawInput) {
    this.transformsRawInput = transformsRawInput;
  }

  public PTFPartition execute(PTFPartition iPart)
      throws HiveException
  {
    PTFPartitionIterator<Object> pItr = iPart.iterator();
    PTFOperator.connectLeadLagFunctionsToPartition(ptfDesc, pItr);
    PTFPartition outP = new PTFPartition(getPartitionClass(),
        getPartitionMemSize(), tDef.getSerde(), OI);
    execute(pItr, outP);
    return outP;
  }

  protected abstract void execute(PTFPartitionIterator<Object> pItr, PTFPartition oPart) throws HiveException;

  public PTFPartition transformRawInput(PTFPartition iPart) throws HiveException
  {
    if ( !isTransformsRawInput())
    {
      throw new HiveException(sprintf("Internal Error: mapExecute called on function (%s)that has no Map Phase", tDef.getName()));
    }
    return _transformRawInput(iPart);
  }

  protected PTFPartition _transformRawInput(PTFPartition iPart) throws HiveException
  {
    return null;
  }
}
