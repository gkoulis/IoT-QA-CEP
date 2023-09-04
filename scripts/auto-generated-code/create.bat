F:

cd \projects\PhD\DGk-PhD-Monorepo\src\iotvm-extensions-specifications\thrift

.\thrift-0.18.1.exe -r --gen py --out ..\..\iotvm-extensions .\base.thrift
.\thrift-0.18.1.exe -r --gen py --out ..\..\iotvm-extensions .\fabrication_backcasting.thrift
.\thrift-0.18.1.exe -r --gen py --out ..\..\iotvm-extensions .\fabrication_forecasting.thrift
.\thrift-0.18.1.exe -r --gen py --out ..\..\iotvm-extensions .\sensing_recording.thrift

.\thrift-0.18.1.exe -r --gen java --out ..\..\iotvm-eventengine\src\main\java .\base.thrift
.\thrift-0.18.1.exe -r --gen java --out ..\..\iotvm-eventengine\src\main\java .\fabrication_backcasting.thrift
.\thrift-0.18.1.exe -r --gen java --out ..\..\iotvm-eventengine\src\main\java .\fabrication_forecasting.thrift
.\thrift-0.18.1.exe -r --gen java --out ..\..\iotvm-eventengine\src\main\java .\sensing_recording.thrift

cd \projects\PhD\DGk-PhD-Monorepo\src\iotvm-gateway
call mvn clean
call mvn compile
call mvn spotless:apply

cd \projects\PhD\DGk-PhD-Monorepo\src\iotvm-eventengine
call mvn clean
call mvn compile
call mvn spotless:apply

pause
