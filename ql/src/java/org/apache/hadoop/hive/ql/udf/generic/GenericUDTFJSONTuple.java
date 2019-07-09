package org.apache.hadoop.hive.ql.udf.generic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Iterator;

public class GenericUDTFJSONTuple extends GenericUDTF {
    int numCols;
    String[] paths;
    private transient Text[] retCols;
    private transient Text[] cols;
    private transient Object[] nullCols;
    private transient ObjectInspector[] inputOIs;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        inputOIs = argOIs;
        numCols = argOIs.length - 1;
        if (numCols < 1) {
            throw new UDFArgumentException("至少需要两个参数，第一个参数是json，第二个参数是字段名");
        }

        for (int i = 0; i < numCols; i++) {
            if (argOIs[i].getCategory() != ObjectInspector.Category.PRIMITIVE ||
                    !argOIs[i].equals(serdeConstants.STRING_TYPE_NAME))
                throw new UDFArgumentException("参数必须是string类型");
        }
        paths = new String[numCols];
        cols = new Text[numCols];
        retCols = new Text[numCols];
        nullCols = new Object[numCols];

        for (int i = 0; i < numCols; i++) {
            cols[i] = new Text();
            retCols[i] = cols[i];
            nullCols[i] = null;
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldDataType = new ArrayList<ObjectInspector>();

        for (int i = 0; i < numCols; i++) {
            fieldNames.add("c" + i);
            fieldDataType.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldDataType);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        if (args[0] == null) {
            forward(nullCols);
            return;
        }
        for (int i = 0; i < numCols; i++) {
            paths[i] = ((StringObjectInspector) inputOIs[i + 1]).getPrimitiveJavaObject(args[i + 1]);

        }
        String jsonStr = ((StringObjectInspector) inputOIs[0]).getPrimitiveJavaObject(args[0]);

        if (jsonStr == null) {
            forward(nullCols);
            return;
        }

        JSON json = null;

        try {
            if (jsonStr.startsWith("[")) {
                json = JSONArray.parseArray(jsonStr);
            } else {
                json = JSONObject.parseObject(jsonStr);
            }
        } catch (JSONException e) {
            return;
        }

        try {
            Object extractObject = null;

            if (json instanceof JSONArray) {
                Iterator<Object> subJson = ((JSONArray) json).iterator();
                while (subJson.hasNext()) {
                    Object subExtractObject = subJson.next();
                    JSONObject subJsonObject = JSONObject.parseObject(subExtractObject.toString());
                    for (int i = 0; i < numCols; i++) {
                        if (retCols[i] == null) {
                            retCols[i] = cols[i];
                        }
                        extractObject = subJsonObject.get(paths[i]);
                        if (extractObject != null) {
                            retCols[i].set(extractObject.toString());
                        } else {
                            retCols[i] = null;
                        }
                    }
                    forward(retCols);
                }
                return;
            }
            if (json instanceof JSONObject) {
                for (int i = 0; i < numCols; i++) {
                    if (retCols[i] == null) {
                        retCols[i] = cols[i];
                    }
                    extractObject = ((JSONObject) json).get(paths[i]);
                    if (extractObject != null) {
                        retCols[i].set(extractObject.toString());
                    } else {
                        retCols[i] = null;
                    }
                }
                forward(retCols);
                return;
            } else {
                forward(nullCols);
                return;
            }
        } catch (Throwable e) {
            forward(nullCols);
            return;
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
