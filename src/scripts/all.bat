
set SCRIPTS="C:\Projects\LearningSpark\src\scripts"

START /B CMD /C CALL %SCRIPTS%\"run_mongod1.bat"
START /B CMD /C CALL %SCRIPTS%\"run_mongod2.bat"
START /B CMD /C CALL %SCRIPTS%\"run_config1.bat"
START /B CMD /C CALL %SCRIPTS%\"run_mongos.bat"