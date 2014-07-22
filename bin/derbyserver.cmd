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
@rem
setlocal enabledelayedexpansion

set hadoop-config-script=%HADOOP_HOME%\libexec\yarn-config.cmd
call %hadoop-config-script%

pushd %HIVE_HOME%\lib
for /f %%a IN ('dir /b derby*.jar') do (
	call :SetClasspath %HIVE_HOME%\lib\%%a
)
popd

set CLASS=org.apache.derby.drda.NetworkServerControl

if [%1]==[catservicexml] goto :derbyservice_catservice

:derbyserver

  if "%1" == "--config" (
    shift
    set HADOOP_CONF_DIR=%2
    shift

    if exist %HADOOP_CONF_DIR%\hadoop-env.cmd (
      call %HADOOP_CONF_DIR%\hadoop-env.cmd
    )
  )

  call %JAVA% %JAVA_HEAP_MAX% %HADOOP_OPTS% -classpath %CLASSPATH% %CLASS% start -h 0.0.0.0 -noSecurityManager
goto :EOF

:SetClasspath
	set CLASSPATH=%CLASSPATH%;%1
goto :EOF

:derbyservice_catservice
@echo ^<service^>
@echo   ^<id^>derbyserver^</id^>
@echo   ^<name^>derbyserver^</name^>
@echo   ^<description^>Derby Service^</description^>
@echo   ^<executable^>%JAVA_HOME%\bin\java^</executable^>
@echo   ^<arguments^>%JAVA_HEAP_MAX% %HADOOP_OPTS% -classpath %CLASSPATH% %CLASS% start -h 0.0.0.0 -noSecurityManager^</arguments^>
@echo ^</service^>
goto :EOF
endlocal
