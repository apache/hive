@echo off
@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements.  See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License.  You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.
SetLocal EnableDelayedExpansion

@rem Set the path

if not defined HIVE_BIN_PATH (
  set HIVE_BIN_PATH=%~dp0
)

if "%HIVE_BIN_PATH:~-1%" == "\" (
  set HIVE_BIN_PATH=%HIVE_BIN_PATH:~0,-1%
)

set HIVE_CONFIG_SCRIPT=%HIVE_BIN_PATH%\hive-config.cmd

if exist  %HIVE_CONFIG_SCRIPT% (
  CALL  %HIVE_CONFIG_SCRIPT% %*
)

set SERVICE=
set HELP=
set CATSERVICE=
set DEBUG=
set CURRENTARG=
set HIVEARGS=
rem set AUX_CLASSPATH=
set AUX_PARAM=

@rem parse the command line arguments
:ProcessCmdLine
	if [%1]==[] goto :FinishArgs

	set temp=%1
	set temp=%temp:~0, 7%

	if %temp%==--debug (
		set DEBUG=%*
		shift
		goto :ProcessCmdLine
	)

	if %1==--config (
		shift
		shift
		goto :ProcessCmdLine
	)

	if %1==--auxpath (
		shift
		shift
		goto :ProcessCmdLine
	)

	if %1==--service (
		set SERVICE=%2

		if [%3]==[catservicexml] (
			set CATSERVICE=_catservice
			shift
		)
		shift
		shift
		goto :ProcessCmdLine
	)

	if %1==--rcfilecat (
		set SERVICE=rcfilecat
		shift
		goto :ProcessCmdLine
	)

	if %1==--orcfiledump (
		set SERVICE=orcfiledump
		shift
		goto :ProcessCmdLine
	)

	if %1==--help (
		set HELP=_help
		shift
		goto :ProcessCmdLine
	)

	@rem parameter at %1 does not match any option, these are optional params
	goto :FinishArgs
:FinishArgs

if defined DEBUG (
	if defined HELP (
		call %HIVE_BIN_PATH%\ext\debug.cmd HELP
		goto :EOF
	)

	call %HIVE_BIN_PATH%\ext\debug.cmd %DEBUG%
)

if defined HIVE_MAIN_CLIENT_DEBUG_OPTS (
	set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS% %HIVE_MAIN_CLIENT_DEBUG_OPTS%
)

if not [%1]==[] (
	set CURRENTARG=%1
	call :MakeHiveArgs %*
)

if not defined SERVICE (
	if defined HELP (
		set SERVICE=help
	) else (
		set SERVICE=cli
	)
)

if not defined HIVE_HOME (
	echo "HIVE_HOME needs to be defined to point at the root of the hive install"
	exit /b 1
)

if not defined HIVE_CONF_DIR (
	set HIVE_CONF_DIR=%HIVE_HOME%\conf
)

if exist %HIVE_CONF_DIR%/hive-env.cmd CALL %HIVE_CONF_DIR%/hive-env.cmd

@rem sort out classpath and make sure dependencies exist
set CLASSPATH=%HIVE_CONF_DIR%

set HIVE_LIB=%HIVE_HOME%\lib

@rem needed for execution
if not exist %HIVE_LIB%\hive-exec-*.jar (
	echo "Missing Hive Execution Jar: %HIVE_LIB%/hive-exec-*.jar"
	exit /b 1
)

if not exist %HIVE_LIB%\hive-metastore-*.jar (
	echo "Missing Hive MetaStore Jar"
	exit /b 1
)

@rem cli specific code
if not exist %HIVE_LIB%\hive-cli-*.jar (
	echo "Missing Hive CLI Jar"
	exit /b 1
)

set CLASSPATH=%CLASSPATH%;%HIVE_LIB%\*

@rem maybe we should just make users set HADOOP_HOME env variable as a prereq
@rem in the next iteration, use "where" command to find directory of hadoop install from path
if not defined HADOOP_HOME (
	echo "HADOOP_HOME needs to be defined to point at the hadoop installation"
	exit /b 1
)

@rem supress the HADOOP_HOME warnings in 1.x.x
set HADOOP_HOME_WARN_SUPPRESS=true

set HADOOP=%HADOOP_HOME%\bin\hadoop.cmd
if not exist %HADOOP% (
	echo "Missing hadoop installation: %HADOOP_HOME% must be set"
	exit /b 1
)

@rem can only run against hadoop 1.0.0 as prereq for this iteration - can't figure out the regex/awk script to determine compatibility

@rem add auxilary jars such as serdes
if not defined HIVE_AUX_JARS_PATH goto :AddAuxLibDir

setLocal EnableDelayedExpansion
:auxJarLoop
	for /f "delims=," %%a in ("!HIVE_AUX_JARS_PATH!") do (
		set auxjar=%%a
		if exist %%a (
			if exist "%%a\nul" (
				@rem %%a is a dir
				pushd %%a
				for /f %%b IN ('dir /b *.jar') do (
					set AUX_CLASSPATH=!AUX_CLASSPATH!;%%a\%%b
					call :AddToAuxParam %%a\%%b
				)
				popd
			) else (
				@rem %%a is a file
				set AUX_CLASSPATH=!AUX_CLASSPATH!;%%a
				call :AddToAuxParam %%a
			)
		)
	)
	:striploop
	set stripchar=!HIVE_AUX_JARS_PATH:~0,1!
	set HIVE_AUX_JARS_PATH=!HIVE_AUX_JARS_PATH:~1!
	if "!HIVE_AUX_JARS_PATH!" EQU "" goto auxJarLoopEnd
	if "!stripchar!" NEQ "," goto striploop
	goto auxJarLoop

:auxJarLoopEnd

if defined HIVE_AUX_JARS_PATH (
	echo "setting aux param %HIVE_AUX_JARS_PATH%"
	set AUX_CLASSPATH=%HIVE_AUX_JARS_PATH%
	set AUX_PARAM=file://%HIVE_AUX_JARS_PATH%
)


:AddAuxLibDir
@rem adding jars from auxlib directory
if exist %HIVE_HOME%\auxlib (
	pushd %HIVE_HOME%\auxlib
	for /f %%a IN ('dir /b *.jar') do (
		set AUX_CLASSPATH=%AUX_CLASSPATH%;%%a
		call :AddToAuxParam %%a
	)
	popd
)

@rem pass classpath to hadoop
set HADOOP_CLASSPATH=%HADOOP_CLASSPATH%;%CLASSPATH%;%AUX_CLASSPATH%

@rem also pass hive classpath to hadoop
if defined HIVE_CLASSPATH (
  set HADOOP_CLASSPATH=%HADOOP_CLASSPATH%;%HIVE_CLASSPATH%
)

@rem set hbase components
if defined HBASE_HOME (
  if not defined HBASE_CONF_DIR (
    if exist %HBASE_HOME%\conf (
      set HBASE_CONF_DIR=%HBASE_HOME%\conf
    )
  )
  if defined HBASE_CONF_DIR (
    call :AddToHadoopClassPath %HBASE_CONF_DIR%	
  ) 
  if exist %HBASE_HOME%\lib (
    call :AddToHadoopClassPath %HBASE_HOME%\lib\*
  ) 
)

if defined AUX_PARAM (
        set HIVE_OPTS=%HIVE_OPTS% -hiveconf hive.aux.jars.path="%AUX_PARAM%"
	set AUX_JARS_CMD_LINE="-libjars %AUX_PARAM%"
)

@rem Get ready to run the services
set SERVICE_COUNT=0
set TORUN=""
call :AddServices
For /L %%i in (1,1,%SERVICE_COUNT%) do (
	if "%SERVICE%" == "!VAR%%i!" (
		set TORUN=!VAR%%i!
	)
)

if %TORUN% == "" (
	echo "Service %SERVICE% not available"
	exit /b 1
)


if defined HELP (
	call %HIVE_BIN_PATH%\ext\%TORUN%.cmd %TORUN%%HELP% %*
	goto :EOF
)

@rem generate xml for the service, also append hadoop dependencies to the classpath
if defined CATSERVICE (
  if exist  %HADOOP_HOME%\libexec\hadoop-config.cmd (
	  call %HADOOP_HOME%\libexec\hadoop-config.cmd
	) else (
	  call %HADOOP_HOME%\libexec\hadoop-config.cmd
	)
	call %HIVE_BIN_PATH%\ext\%TORUN%.cmd %TORUN%%CATSERVICE% %*
	goto :EOF
)

call %HIVE_BIN_PATH%\ext\%TORUN%.cmd %TORUN% %*



goto :EOF
@rem done body of script


@rem start utility functions here

@rem strip off preceding arguments like --service so that subsequent args can be passed on
:MakeHiveArgs
	set _count=0
	set _shift=1
	set HIVEARGS=

	if not defined CURRENTARG (
		goto :EndLoop
	)
	:HiveArgsLoop
		if [%1]==[] (
			goto :EndLoop
		)
		if not %1==%CURRENTARG% (
			shift
			goto :HiveArgsLoop
		)

		if not defined HIVEARGS (
			set HIVEARGS=%1
		) else (
			set HIVEARGS=%HIVEARGS% %1
		)
		shift
		set CURRENTARG=%1
		goto :HiveArgsLoop
	:EndLoop
goto :EOF

@rem makes list of available services
:AddServices
	set /a SERVICE_COUNT = %SERVICE_COUNT% + 1
	set VAR%SERVICE_COUNT%=cli

	set /a SERVICE_COUNT = %SERVICE_COUNT% + 1
	set VAR%SERVICE_COUNT%=help

	set /a SERVICE_COUNT = %SERVICE_COUNT% + 1
	set VAR%SERVICE_COUNT%=hiveserver

	set /a SERVICE_COUNT = %SERVICE_COUNT% + 1
	set VAR%SERVICE_COUNT%=hiveserver2

	set /a SERVICE_COUNT = %SERVICE_COUNT% + 1
	set VAR%SERVICE_COUNT%=hwi

	set /a SERVICE_COUNT = %SERVICE_COUNT% + 1
	set VAR%SERVICE_COUNT%=jar

	set /a SERVICE_COUNT = %SERVICE_COUNT% + 1
	set VAR%SERVICE_COUNT%=lineage

	set /a SERVICE_COUNT = %SERVICE_COUNT% + 1
	set VAR%SERVICE_COUNT%=metastore

	set /a SERVICE_COUNT = %SERVICE_COUNT% + 1
	set VAR%SERVICE_COUNT%=rcfilecat

	set /a SERVICE_COUNT = %SERVICE_COUNT% + 1
	set VAR%SERVICE_COUNT%=orcfiledump

	set /a SERVICE_COUNT = %SERVICE_COUNT% + 1
	set VAR%SERVICE_COUNT%=schematool
goto :EOF

:AddToAuxParam
if not defined AUX_PARAM (
	set AUX_PARAM=file:///%1
	) else (
	set AUX_PARAM=%AUX_PARAM%,file:///%1
	)
)
goto :EOF

:AddToHadoopClassPath
if defined HADOOP_CLASSPATH (
  set HADOOP_CLASSPATH=%HADOOP_CLASSPATH%;%1
) else (
    set HADOOP_CLASSPATH=%1
  )  
)
goto :EOF
