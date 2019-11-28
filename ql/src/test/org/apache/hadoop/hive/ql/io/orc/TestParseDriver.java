package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.junit.Ignore;
import org.junit.Test;

public class TestParseDriver {
	ParseDriver parseDriver = new ParseDriver();

	@Test
	public void testParseManyParenthesis() throws Exception {
		parseDriver.parse(
				"  select (((((((((((((((((((((((((((((((((((((((((((((((((((((((('x')))))))))))))))))))))))))))))))))))))))))))))))))))))))) from processed_opendata_samples.nyse_stocks limit 1");
	}

	@Test
	public void testParse0() throws Exception {
		parseDriver.parse("select * from emps where ((empno+deptno/2), (deptno/3)) in ((2,0),(3,2))\n");
	}

	@Test
	public void testParse01() throws Exception {
		parseDriver.parse("select * from emps where (ax(1), int(2)) in ((2,0),(3,2))\n");
	}

	@Test
	public void testParse1() throws Exception {
		parseDriver.parse("select * from emps where (xint(empno+deptno/2), xint(deptno/3)) in ((2,0),(3,2))\n");
	}

	@Test
	public void testParse2() throws Exception {
		parseDriver.parse("select * from emps where (int(empno+deptno/2), int(deptno/3)) in ((2,0),(3,2))\n");
	}

	@Test
	public void testParse3() throws Exception {
		parseDriver.parse("select * from emps where 2 in (1,2)\n");
	}

	@Test
	public void testParse4() throws Exception {
		parseDriver.parse("	  select * from emps where (xempno,deptno) in ((1,2),(3,2))\n" + "\n");
	}

	@Test
	public void testParse5() throws Exception {
		parseDriver.parse("	  select * from emps where (xempno+1,deptno) in ((1,2),(3,2))\n" + "\n");
	}

	@Test
	public void testParse6() throws Exception {
		parseDriver.parse("explain select * from emps where ((empno*2)|1,deptno) in ((empno+1,2),(empno+2,2))");
	}

}
