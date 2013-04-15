package org.apache.hadoop.hive.ql.udf.generic;

import java.util.List;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

@Description(name ="inline", value= "_FUNC_( ARRAY( STRUCT()[,STRUCT()] "
+ "- explodes and array and struct into a table")
public class GenericUDTFInline extends GenericUDTF {

  private Object[] forwardObj;
  private ListObjectInspector li;
  private StructObjectInspector daStruct;

  public GenericUDTFInline(){

  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] ois) throws UDFArgumentException {
    //There should be one argument that is a array of struct
    if (ois.length!=1){
      throw new UDFArgumentException("UDF tables only one argument");
    }
    if (ois[0].getCategory()!= Category.LIST){
      throw new UDFArgumentException("Top level object must be an array but "
              + "was "+ois[0].getTypeName());
    }
    li = (ListObjectInspector) ois[0];
    ObjectInspector sub=li.getListElementObjectInspector();
    if (sub.getCategory() != Category.STRUCT){
      throw new UDFArgumentException("The sub element must be struct, but was "+sub.getTypeName());
    }
    daStruct = (StructObjectInspector) sub;
    forwardObj = new Object[daStruct.getAllStructFieldRefs().size()];
    return daStruct;
  }

  @Override
  public void process(Object[] os) throws HiveException {
    //list is always one item
    List l = li.getList(os);
    List<? extends StructField> fields = this.daStruct.getAllStructFieldRefs();
    for (Object linner: l ){
      List<List> innerList = (List) linner;
      for (List rowList : innerList){
        int i=0;
        for (StructField f: fields){
          GenericUDFUtils.ReturnObjectInspectorResolver res
            = new GenericUDFUtils.ReturnObjectInspectorResolver();
          res.update(f.getFieldObjectInspector());
          this.forwardObj[i]=res.convertIfNecessary(rowList.get(i), f.getFieldObjectInspector());
          i++;
        }
        forward(this.forwardObj);
      }
    }
  }

  @Override
  public void close() throws HiveException {
  }

  @Override
  public String toString() {
    return "inline";
  }
}
