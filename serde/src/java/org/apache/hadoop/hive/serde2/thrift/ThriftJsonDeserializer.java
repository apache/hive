package org.apache.hadoop.hive.serde2.thrift;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.RegexSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.thrift.TReflectionUtils;
import org.apache.hadoop.hive.serde2.thrift.ThriftByteStreamTypedSerDe;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import javax.annotation.Nullable;

/**
 * Created by bala on 23/6/17.
 */
public class ThriftJsonDeserializer extends AbstractSerDe {

    public static final Logger LOG = LoggerFactory.getLogger(RegexSerDe.class.getName());
    Class<?> recordClass;
    ObjectMapper objectMapper;

    @Override
    public void initialize(@Nullable Configuration conf, Properties tbl) throws SerDeException {
        // We can get the table definition from tbl.

        // Read the configuration parameters
        try {
            String className = tbl
                    .getProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_CLASS);
            recordClass = conf.getClassByName(className);
            objectMapper = new ObjectMapper();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        throw new UnsupportedOperationException(
                "Regex SerDe doesn't support the serialize() method");

    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector)
            throws SerDeException {
        throw new UnsupportedOperationException(
                "Regex SerDe doesn't support the serialize() method");
    }

    @Override
    public SerDeStats getSerDeStats() {
        // no support for statistics
        return null;
    }

    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        Object mapOutput = null;
        try {
            BytesWritable rowText = (BytesWritable) blob;
            mapOutput = objectMapper.readValue(rowText.getBytes(), recordClass);
        } catch(Exception e) {
            e.printStackTrace();
            LOG.info(e.getMessage());
            LOG.info(e.toString());
            LOG.info(e.getStackTrace().toString());
        }
        return mapOutput;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return ObjectInspectorFactory.getReflectionObjectInspector(recordClass,
                getObjectInspectorOptions());
    }

    protected ObjectInspectorFactory.ObjectInspectorOptions getObjectInspectorOptions() {
        return ObjectInspectorFactory.ObjectInspectorOptions.THRIFT;
    }

}
