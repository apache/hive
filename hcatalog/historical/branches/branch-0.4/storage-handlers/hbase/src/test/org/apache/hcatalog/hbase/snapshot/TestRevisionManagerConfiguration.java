package org.apache.hcatalog.hbase.snapshot;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestRevisionManagerConfiguration {

    @Test
    public void testDefault() {
        Configuration conf = RevisionManagerConfiguration.create();
        assertEquals("org.apache.hcatalog.hbase.snapshot.ZKBasedRevisionManager",
                     conf.get(RevisionManagerFactory.REVISION_MGR_IMPL_CLASS));
    }
}
