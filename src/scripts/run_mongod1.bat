
set CONFIG="C:\Projects\LearningSpark\src\config"

set MONGOBIN="c:\Program Files\MongoDB 2.6 Standard\bin"
set DBROOT="c:\mongoinst\data"
set DB1=%DBROOT%\db1
set DB2=%DBROOT%\db2

%MONGOBIN%\mongod.exe --config %CONFIG%\mongod1.cfg