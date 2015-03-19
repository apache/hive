package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

public abstract class GenericUDFBaseDTI extends GenericUDFBaseBinary {
  protected transient PrimitiveObjectInspector[] inputOIs;

  protected boolean checkArgs(PrimitiveCategory leftType, PrimitiveCategory rightType) {
    boolean result = false;
    if (inputOIs[0].getPrimitiveCategory() == leftType
        && inputOIs[1].getPrimitiveCategory() == rightType) {
      result = true;
    }
    return result;
  }
}
