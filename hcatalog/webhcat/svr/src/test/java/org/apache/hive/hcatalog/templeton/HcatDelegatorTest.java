package org.apache.hive.hcatalog.templeton;

import junit.framework.TestCase;

public class HcatDelegatorTest extends TestCase {
    public void testGetCatalogStatementWithUse() {
        HcatDelegator.SchemaStatement cs = HcatDelegator.getSchemaStatement("use db1; show create table x;");
        assertEquals("db1", cs.schema);
        assertEquals("show create table x", cs.statement);
    }

    public void testGetCatalogStatementWithoutUse() {
        HcatDelegator.SchemaStatement cs = HcatDelegator.getSchemaStatement("show create table db1.x;");
        assertEquals(null, cs.schema);
        assertEquals("show create table db1.x", cs.statement);
    }
}
