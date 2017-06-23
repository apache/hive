package org.apache.hadoop.hive.serde2.thrift;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.RegexSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import javax.annotation.Nullable;

/**
 * ThriftJsonDeserializer uses Jackson object mapper to deserialize data. It doesn't
 * support data serialization.
 *
 * It can deserialize the data using Jackson and extracts groups as columns.
 *
 * In deserialization stage, if a json record does not match the regex, then all columns
 * in the record will be NULL. If a record matches the regex but has less than
 * expected groups, the missing groups will be NULL. If a record matches the Contract class
 * but has more than expected groups, the additional groups are just ignored.
 *
 *
 *
 * Usage :
 * SET ROW FORMAT SERDE  'org.apache.hadoop.hive.serde2.thrift.ThriftJsonDeserializer'
 *
 * SET SERDEPROPERTIES (
 'serialization.class'='contract class',
 'serialization.format'='org.apache.thrift.protocol.TSimpleJSONProtocol'
 - To get the schema for tables.
 )
 *
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
            // Initializing the Jackson object mapper.
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
        } catch (Exception e) {
            LOG.error(e.getMessage());
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
