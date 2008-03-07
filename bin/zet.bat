@echo off

IF "%JAVA_HOME%X"=="X" echo please set JAVA_HOME to the location of your java installation
IF "%JAVA_HOME%X"=="X" exit

IF "%ZORILLA_HOME%X"=="X" echo please set ZORILLA_HOME to the location of your zorilla installation
IF "%ZORILLA_HOME%X"=="X" exit

:: Create the path with the JAR files
SET ZORILLA_CLASSPATH=

FOR %%i IN ("%ZORILLA_HOME%\zoni\lib\*.jar") DO CALL "%ZORILLA_HOME%\bin\AddToClassPath.bat" %%i
FOR %%i IN ("%ZORILLA_HOME%\node\lib\*.jar") DO CALL "%ZORILLA_HOME%\bin\AddToClassPath.bat" %%i
FOR %%i IN ("%ZORILLA_HOME%\node\lib\ibis\*.jar") DO CALL "%ZORILLA_HOME%\bin\AddToClassPath.bat" %%i


%JAVA_HOME%\bin\java -cp %CLASSPATH%;%ZORILLA_CLASSPATH% -Dlog4j.configuration=file:"%ZORILLA_HOME%"\log4j.properties ibis.zorilla.apps.Zet %*
