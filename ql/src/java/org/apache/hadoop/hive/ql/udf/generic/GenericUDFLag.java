package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;

@UDFType(impliesOrder = true)
public class GenericUDFLag extends GenericUDFLeadLag {
  @Override
  protected String _getFnName() {
    return "lag";
  }

  @Override
  protected int getIndex(int amt) {
    return pItr.getIndex() - 1 - amt;
  }

  @Override
  protected Object getRow(int amt) throws HiveException {
    return pItr.lag(amt + 1);
  }

}