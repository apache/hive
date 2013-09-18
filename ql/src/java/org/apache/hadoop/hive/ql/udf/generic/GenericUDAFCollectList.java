package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMkCollectionEvaluator.BufferType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@Description(name = "collect_list", value = "_FUNC_(x) - Returns a list of objects with duplicates")
public class GenericUDAFCollectList extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(GenericUDAFCollectList.class.getName());

  public GenericUDAFCollectList() {
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly one argument is expected.");
    }
    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
          + parameters[0].getTypeName() + " was passed as parameter 1.");
    }
    return new GenericUDAFMkCollectionEvaluator(BufferType.LIST);
  }
}
