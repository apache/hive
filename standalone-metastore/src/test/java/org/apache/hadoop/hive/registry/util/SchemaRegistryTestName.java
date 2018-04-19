package org.apache.hadoop.hive.registry.util;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class SchemaRegistryTestName extends TestWatcher {

    protected String name;

    @Override
    protected void starting(Description d) {
        name=d.getMethodName().replaceAll("[\\[\\]]","-");
    }

    public String getMethodName() {
        return name;
    }
}