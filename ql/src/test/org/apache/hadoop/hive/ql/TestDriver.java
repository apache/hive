package org.apache.hadoop.hive.ql;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Created by smanciot on 22/11/2018.
 */
public class TestDriver {

    private final static String INSERT_INTO = "INSERT INTO wkf101351_43_1_ALL ";
    private final static String COLUMNS = "( sUserid, biPk, biUserpk, biStatuspk )";
    private final static String SELECT = "SELECT sUserid,biPk,biUserpk,biStatuspk FROM wkf101351_42_1;";

    @Test
    public void testHandleInsertCommands(){
        Assert.assertEquals(
                INSERT_INTO + COLUMNS.toLowerCase() + SELECT,
                Driver.handleInsertCommands(
                        INSERT_INTO + COLUMNS + SELECT
                )
        );
        Assert.assertEquals(
                SELECT,
                Driver.handleInsertCommands(
                        SELECT
                )
        );
    }

    @Test
    public void testHandleSelectCommands(){
        Assert.assertEquals(
                SELECT,
                Driver.handleSelectCommands(
                        "(" + SELECT + ")"
                )
        );
        Assert.assertEquals(
                SELECT,
                Driver.handleSelectCommands(
                        SELECT
                )
        );
    }
}
