package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.time.ZonedDateTime;

// This function is not a deterministic function, but a runtime constant.
// The return value is constant within a query but can be different between queries.
@UDFType(deterministic = false, runtimeConstant = true)
@Description(name = "yesterday",
        value = "_FUNC_() - Returns yesterday's date at the start of query evaluation. The same as 'date_sub(current_date, 1)'"
                + " All calls of yesterday within the same query return the same value.")
@NDV(maxNdv = 1)
public class GenericUDFYesterday extends GenericUDF {

    protected DateWritableV2 yesterday;


    public DateWritableV2 getYesterday() {
        return yesterday;
    }

    public void setYesterday(DateWritableV2 yesterday) {
        this.yesterday = yesterday;
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        if (arguments.length != 0) {
            throw new UDFArgumentLengthException(
                    "The function YESTERDAY does not take any arguments, but found "
                            + arguments.length);
        }

        if (yesterday == null) {
            SessionState ss = SessionState.get();
            ZonedDateTime dateTime = ss.getQueryCurrentTimestamp().atZone(
                    ss.getConf().getLocalTimeZone()).minusDays(1L);
            Date dateVal = Date.valueOf(
                    dateTime.toString().substring(0, 10));
            yesterday = new DateWritableV2(dateVal);
        }

        return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        return this.yesterday;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "YESTERDAY()";
    }

    @Override
    public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
        super.copyToNewInstance(newInstance);
        // Need to preserve currentDate
        GenericUDFYesterday other = (GenericUDFYesterday) newInstance;
        if (this.yesterday != null) {
            other.yesterday = new DateWritableV2(this.yesterday);
        }
    }
}
