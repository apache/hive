package org.apache.hadoop.hive.ql.udf.generic;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.*;



import static org.junit.Assert.*;
import static org.reflections.Reflections.log;

public class TestUDFSplitMapPrivs extends TestCase {
    private final UDFSplitMapPrivs udf = new UDFSplitMapPrivs();
    private Object p0 = new Text("SELECT");
    private Object p1 = new Text("UPDATE");
    private Object p2 = new Text("CREATE");
    private Object p3 = new Text("DROP");
    private Object p4 = new Text("ALTER");
    private Object p5 = new Text("INDEX");
    private Object p6 = new Text("LOCK");
    private Object p7 = new Text("READ");
    private Object p8 = new Text("WRITE");
    private Object p9 = new Text("All");

    private DeferredObject splitDilimiter = new DeferredJavaObject(new Text(" "));



    @Test
    public void testBinaryStringSplitMapToPrivs() throws HiveException {

        ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        ObjectInspector[] initArgs = {valueOI0, valueOI1};

        udf.initialize(initArgs);
        DeferredObject args;
        DeferredObject[] evalArgs;

        args = new DeferredJavaObject(new Text("1 0 0 0 0 0 0 0 0 0"));
        evalArgs = new DeferredObject[] { args, splitDilimiter };
        runAndVerify(asList(p0), evalArgs);

        args = new DeferredJavaObject(new Text("1 1 0 0 0 0 0 0 0 0"));
        evalArgs = new DeferredObject[] { args, splitDilimiter };
        runAndVerify(asList(p0,p1),evalArgs);

        args = new DeferredJavaObject(new Text("1 1 1 0 0 0 0 0 0 0"));
        evalArgs = new DeferredObject[] { args, splitDilimiter };
        runAndVerify(asList(p0,p1,p2), evalArgs);

        args = new DeferredJavaObject(new Text("1 1 1 1 0 0 0 0 0 0"));
        evalArgs = new DeferredObject[] { args, splitDilimiter };
        runAndVerify(asList(p0,p1,p2,p3), evalArgs);

        args = new DeferredJavaObject(new Text("1 1 1 1 1 0 0 0 0 0"));
        evalArgs = new DeferredObject[] { args, splitDilimiter };
        runAndVerify(asList(p0,p1,p2,p3,p4), evalArgs);

        args = new DeferredJavaObject(new Text("1 1 1 1 1 1 0 0 0 0"));
        evalArgs = new DeferredObject[] { args, splitDilimiter };
        runAndVerify(asList(p0,p1,p2,p3,p4,p5), evalArgs);

        args = new DeferredJavaObject(new Text("1 1 1 1 1 1 1 0 0 0"));
        evalArgs = new DeferredObject[] { args, splitDilimiter };
        runAndVerify(asList(p0,p1,p2,p3,p4,p5,p6), evalArgs);

        args = new DeferredJavaObject(new Text("1 1 1 1 1 1 1 1 0 0"));
        evalArgs = new DeferredObject[] { args, splitDilimiter };
        runAndVerify(asList(p0,p1,p2,p3,p4,p5,p6,p7),evalArgs);

        args = new DeferredJavaObject(new Text("1 0 1 1 1 1 1 1 1 0"));
        evalArgs = new DeferredObject[] { args, splitDilimiter };
        runAndVerify(asList(p0,p2,p3,p4,p5,p6,p7,p8), evalArgs);

    }

    @Test
    public void BinaryStringMapingShouldFail() throws HiveException {

        ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        ObjectInspector[] initArgs = {valueOI0, valueOI1};

        udf.initialize(initArgs);
        DeferredObject args;
        DeferredObject[] evalArgs;

        args = new DeferredJavaObject(new Text("1 0 0 0 0 0 0 0 0 0"));
        evalArgs = new DeferredObject[] { args, splitDilimiter };
        runAndVerifyNotTrue(asList(p1), evalArgs);

        args = new DeferredJavaObject(new Text("1 1 0 0 0 0 0 0 0 0"));
        evalArgs = new DeferredObject[] { args, splitDilimiter };
        runAndVerifyNotTrue(asList(p0,p5),evalArgs);




    }





    private void runAndVerify(List<Object> expResult,
                              DeferredObject[] evalArgs) throws HiveException {

        ArrayList output = (ArrayList) udf.evaluate(evalArgs);
        assertEquals(expResult, output);
    }
    private void runAndVerifyNotTrue(List<Object> expResult,
                                     DeferredObject[] evalArgs) throws HiveException {

        ArrayList output = (ArrayList) udf.evaluate(evalArgs);
        assertNotSame(expResult, output);
    }

}