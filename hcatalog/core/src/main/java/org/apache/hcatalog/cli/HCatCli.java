/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.cli;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.processors.DfsProcessor;
import org.apache.hadoop.hive.ql.processors.SetProcessor;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;

public class HCatCli {

    @SuppressWarnings("static-access")
    public static void main(String[] args) {

        try {
            LogUtils.initHiveLog4j();
        } catch (LogInitializationException e) {

        }

        CliSessionState ss = new CliSessionState(new HiveConf(SessionState.class));
        ss.in = System.in;
        try {
            ss.out = new PrintStream(System.out, true, "UTF-8");
            ss.err = new PrintStream(System.err, true, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            System.exit(1);
        }

        HiveConf conf = ss.getConf();

        HiveConf.setVar(conf, ConfVars.SEMANTIC_ANALYZER_HOOK, HCatSemanticAnalyzer.class.getName());

        SessionState.start(ss);

        Options options = new Options();

        // -e 'quoted-query-string'
        options.addOption(OptionBuilder
                .hasArg()
                .withArgName("exec")
                .withDescription("hcat command given from command line")
                .create('e'));

        // -f <query-file>
        options.addOption(OptionBuilder
                .hasArg()
                .withArgName("file")
                .withDescription("hcat commands in file")
                .create('f'));

        // -g
        options.addOption(OptionBuilder
                .hasArg().
                withArgName("group").
                withDescription("group for the db/table specified in CREATE statement").
                create('g'));

        // -p
        options.addOption(OptionBuilder
                .hasArg()
                .withArgName("perms")
                .withDescription("permissions for the db/table specified in CREATE statement")
                .create('p'));

        // -D
        options.addOption(OptionBuilder
                .hasArgs(2)
                .withArgName("property=value")
                .withValueSeparator()
                .withDescription("use hadoop value for given property")
                .create('D'));

        // [-h|--help]
        options.addOption(new Option("h", "help", false, "Print help information"));

        Parser parser = new GnuParser();
        CommandLine cmdLine = null;

        try {
            cmdLine = parser.parse(options, args);

        } catch (ParseException e) {
            printUsage(options, ss.err);
            System.exit(1);
        }
        // -e
        String execString = (String) cmdLine.getOptionValue('e');
        // -f
        String fileName = (String) cmdLine.getOptionValue('f');
        // -h
        if (cmdLine.hasOption('h')) {
            printUsage(options, ss.out);
            System.exit(0);
        }

        if (execString != null && fileName != null) {
            ss.err.println("The '-e' and '-f' options cannot be specified simultaneously");
            printUsage(options, ss.err);
            System.exit(1);
        }

        // -p
        String perms = (String) cmdLine.getOptionValue('p');
        if (perms != null) {
            validatePermissions(ss, conf, perms);
        }

        // -g
        String grp = (String) cmdLine.getOptionValue('g');
        if (grp != null) {
            conf.set(HCatConstants.HCAT_GROUP, grp);
        }

        // -D
        setConfProperties(conf, cmdLine.getOptionProperties("D"));

        if (execString != null) {
            System.exit(processLine(execString));
        }

        try {
            if (fileName != null) {
                System.exit(processFile(fileName));
            }
        } catch (FileNotFoundException e) {
            ss.err.println("Input file not found. (" + e.getMessage() + ")");
            System.exit(1);
        } catch (IOException e) {
            ss.err.println("Could not open input file for reading. (" + e.getMessage() + ")");
            System.exit(1);
        }

        // -h
        printUsage(options, ss.err);
        System.exit(1);
    }

    private static void setConfProperties(HiveConf conf, Properties props) {
        for (java.util.Map.Entry<Object, Object> e : props.entrySet())
            conf.set((String) e.getKey(), (String) e.getValue());
    }

    private static int processLine(String line) {
        int ret = 0;

        String command = "";
        for (String oneCmd : line.split(";")) {

            if (StringUtils.endsWith(oneCmd, "\\")) {
                command += StringUtils.chop(oneCmd) + ";";
                continue;
            } else {
                command += oneCmd;
            }
            if (StringUtils.isBlank(command)) {
                continue;
            }

            ret = processCmd(command);
            command = "";
        }
        return ret;
    }

    private static int processFile(String fileName) throws IOException {
        FileReader fileReader = null;
        BufferedReader reader = null;
        try {
            fileReader = new FileReader(fileName);
            reader = new BufferedReader(fileReader);
            String line;
            StringBuilder qsb = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                qsb.append(line + "\n");
            }

            return (processLine(qsb.toString()));
        } finally {
            if (fileReader != null) {
                fileReader.close();
            }
            if (reader != null) {
                reader.close();
            }
        }
    }

    private static int processCmd(String cmd) {

        SessionState ss = SessionState.get();
        long start = System.currentTimeMillis();

        cmd = cmd.trim();
        String firstToken = cmd.split("\\s+")[0].trim();

        if (firstToken.equalsIgnoreCase("set")) {
            return new SetProcessor().run(cmd.substring(firstToken.length()).trim()).getResponseCode();
        } else if (firstToken.equalsIgnoreCase("dfs")) {
            return new DfsProcessor(ss.getConf()).run(cmd.substring(firstToken.length()).trim()).getResponseCode();
        }

        HCatDriver driver = new HCatDriver();

        int ret = driver.run(cmd).getResponseCode();

        if (ret != 0) {
            driver.close();
            System.exit(ret);
        }

        ArrayList<String> res = new ArrayList<String>();
        try {
            while (driver.getResults(res)) {
                for (String r : res) {
                    ss.out.println(r);
                }
                res.clear();
            }
        } catch (IOException e) {
            ss.err.println("Failed with exception " + e.getClass().getName() + ":"
                + e.getMessage() + "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
            ret = 1;
        } catch (CommandNeedRetryException e) {
            ss.err.println("Failed with exception " + e.getClass().getName() + ":"
                + e.getMessage() + "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
            ret = 1;
        }

        int cret = driver.close();
        if (ret == 0) {
            ret = cret;
        }

        long end = System.currentTimeMillis();
        if (end > start) {
            double timeTaken = (end - start) / 1000.0;
            ss.err.println("Time taken: " + timeTaken + " seconds");
        }
        return ret;
    }

    private static void printUsage(Options options, OutputStream os) {
        PrintWriter pw = new PrintWriter(os);
        new HelpFormatter().printHelp(pw, 2 * HelpFormatter.DEFAULT_WIDTH,
            "hcat { -e \"<query>\" | -f \"<filepath>\" } [ -g \"<group>\" ] [ -p \"<perms>\" ] [ -D\"<name>=<value>\" ]",
            null, options, HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD,
            null, false);
        pw.flush();
    }

    private static void validatePermissions(CliSessionState ss, HiveConf conf, String perms) {
        perms = perms.trim();
        FsPermission fp = null;

        if (perms.matches("^\\s*([r,w,x,-]{9})\\s*$")) {
            fp = FsPermission.valueOf("d" + perms);
        } else if (perms.matches("^\\s*([0-7]{3})\\s*$")) {
            fp = new FsPermission(Short.decode("0" + perms));
        } else {
            ss.err.println("Invalid permission specification: " + perms);
            System.exit(1);
        }

        if (!HCatUtil.validateMorePermissive(fp.getUserAction(), fp.getGroupAction())) {
            ss.err.println("Invalid permission specification: " + perms + " : user permissions must be more permissive than group permission ");
            System.exit(1);
        }
        if (!HCatUtil.validateMorePermissive(fp.getGroupAction(), fp.getOtherAction())) {
            ss.err.println("Invalid permission specification: " + perms + " : group permissions must be more permissive than other permission ");
            System.exit(1);
        }
        if ((!HCatUtil.validateExecuteBitPresentIfReadOrWrite(fp.getUserAction())) ||
            (!HCatUtil.validateExecuteBitPresentIfReadOrWrite(fp.getGroupAction())) ||
            (!HCatUtil.validateExecuteBitPresentIfReadOrWrite(fp.getOtherAction()))) {
            ss.err.println("Invalid permission specification: " + perms + " : permissions must have execute permissions if read or write permissions are specified ");
            System.exit(1);
        }

        conf.set(HCatConstants.HCAT_PERMS, "d" + fp.toString());

    }


}
