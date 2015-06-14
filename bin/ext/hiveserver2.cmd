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

set CLASS=org.apache.hive.service.server.HiveServer2
pushd %HIVE_LIB%
for /f %%a IN ('dir /b hive-service-*.jar') do (
  set JAR=%HIVE_LIB%\%%a
)
popd

if defined HBASE_HOME (
	dir %HBASE_HOME%\lib > nul
	if %errorlevel%==0 (
		pushd %HBASE_HOME%\lib
		set HIVE_HBASE_PATH=
		for /f %%a IN ('dir /b hbase-server-**-hadoop2.jar') do (
		  call :AddToHiveHbasePath  %HBASE_HOME%\lib\%%a
		)
		for /f %%a IN ('dir /b hbase-client-**-hadoop2.jar') do (
		  call :AddToHiveHbasePath  %HBASE_HOME%\lib\%%a
		)
		for /f %%a IN ('dir /b hbase-protocol-**-hadoop2.jar') do (
		  call :AddToHiveHbasePath  %HBASE_HOME%\lib\%%a
		) 
		for /f %%a IN ('dir /b htrace-core-**.jar') do (
		  call :AddToHiveHbasePath  %HBASE_HOME%\lib\%%a
		) 
		for /f %%a IN ('dir /b hbase-common-**-hadoop2.jar') do (
		  call :AddToHiveHbasePath  %HBASE_HOME%\lib\%%a
		) 
		for /f %%a IN ('dir /b hbase-hadoop-compat-**-hadoop2.jar') do (
		  call :AddToHiveHbasePath  %HBASE_HOME%\lib\%%a
		)
    ) 
    popd
	if defined HBASE_CONF_DIR (
		dir %HBASE_CONF_DIR% > nul
		if %errorlevel%==0 (
			call :AddToHiveHbasePath  %HBASE_CONF_DIR%
		)
	)
  )
)

@rem add auxilary jars such as serdes
if not defined HIVE_AUX_JARS_PATH goto :AddMiscAuxLibDir

setLocal EnableDelayedExpansion
:auxJarLoop
    for /f "delims=," %%a in ("!HIVE_AUX_JARS_PATH!") do (
        set auxjar=%%a
        if exist %%a (
            if exist "%%a\nul" (
                @rem %%a is a dir
                pushd %%a
                for /f %%b IN ('dir /b *.jar') do (
                    call :AddToAuxJavaParam %%a\%%b
                )
                popd
            ) else (
                @rem %%a is a file
                call :AddToAuxJavaParam %%a
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

:AddMiscAuxLibDir
@rem adding jars from hcatalog\share\hcatalog directory
if exist %HIVE_HOME%\hcatalog\share\hcatalog (
    pushd %HIVE_HOME%\hcatalog\share\hcatalog
    for /f %%a IN ('dir /b *.jar') do (
        call :AddToAuxJavaParam %HIVE_HOME%\hcatalog\share\hcatalog\%%a
    )
    popd
)

if [%1]==[hiveserver2_help] goto :hiveserver2_help

if [%1]==[hiveserver2_catservice] goto :hiveserver2_catservice

:hiveserver2

  @rem hadoop 20 or newer - skip the aux_jars option and hiveconf
  call %HIVE_BIN_PATH%\ext\util\execHiveCmd.cmd %CLASS%
goto :EOF

:hiveserver2_help
  set HIVEARGS=-h
  goto :hiveserver2
goto :EOF

:hiveserver2_catservice
@echo ^<service^>
@echo   ^<id^>HiveServer2^</id^>
@echo   ^<name^>HiveServer2^</name^>
@echo   ^<description^>Hadoop HiveServer2 Service^</description^>
@echo   ^<executable^>%JAVA_HOME%\bin\java^</executable^>
@echo   ^<arguments^>%JAVA_HEAP_MAX% -XX:MaxPermSize=512m %HADOOP_OPTS% -classpath %CLASSPATH%;%HIVE_HBASE_PATH%;%HIVE_HOME%\hcatalog\share\hcatalog\* %CLASS% -hiveconf hive.hadoop.classpath=%HIVE_LIB%\*;%HIVE_HOME%\hcatalog\share\hcatalog\* -hiveconf hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory -hiveconf hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator -hiveconf hive.metastore.uris=" " -hiveconf hive.aux.jars.path=%AUX_JAVA_PARAM% %HIVE_OPTS%^</arguments^>
@echo ^</service^>
goto :EOF

:AddToHiveHbasePath
if not defined HIVE_HBASE_PATH (
   set HIVE_HBASE_PATH=%1
   ) else (
   set HIVE_HBASE_PATH=%HIVE_HBASE_PATH%;%1
   )
)
goto :EOF

:AddToAuxJavaParam
if not defined AUX_JAVA_PARAM (
    set AUX_JAVA_PARAM=file:///%1
    ) else (
    set AUX_JAVA_PARAM=%AUX_JAVA_PARAM%,file:///%1
    )
)
goto :EOF