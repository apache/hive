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

set CLASS=org.apache.hadoop.hive.hwi.HWIServer
pushd %HIVE_LIB%
for /f %%a IN ('dir /b hive-hwi-*.jar') do (
	set JAR=%HIVE_LIB%\%%a
)
popd

if [%1]==[hwi_help] goto :hwi_help

if [%1]==[hwi_catservice] goto :hwi_catservice

:hwi
  @rem set the hwi jar and war files
	pushd %HIVE_LIB%
	for /f %%a IN ('dir /b hive-hwi-*') do (
		call :ProcessFileName %%a
	)
	popd

  @rem hadoop 20 or newer - skip the aux_jars option and hiveconf
	call %HIVE_BIN_PATH%\ext\util\execHiveCmd.cmd %CLASS%
goto :EOF

@rem process the hwi files
:ProcessFileName
	set temp=%1
	set temp=%temp:~-3%

	if %temp%==jar set HWI_JAR_FILE=lib\%1

	if %temp%==war set HWI_WAR_FILE=lib\%1
goto :EOF

:hwi_help
  echo "Usage ANT_LIB=XXXX hive --service hwi"
goto :EOF

:hwi_catservice
@echo ^<service^>
@echo   ^<id^>HWI^</id^>
@echo   ^<name^>HWI^</name^>
@echo   ^<description^>Hadoop HWI Service^</description^>
@echo   ^<executable^>%JAVA_HOME%\bin\java^</executable^>
@echo   ^<arguments^>%JAVA_HEAP_MAX% %HADOOP_OPTS% %AUX_PARAM% -classpath %CLASSPATH% %CLASS% %HIVE_OPTS%^</arguments^>
@echo ^</service^>
goto :EOF
