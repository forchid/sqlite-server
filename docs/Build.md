# How to build SQLite server
Require OracleJDK or openJDK 7+, Apache Maven 3.6.1+, and modern OS such as CentOS 6.x/7.x, 
macOS 10.13.x, or Windows 8.1 above.
First we must set the JAVA_HOME environment variable, then add MAVEN_HOME/bin append to PATH.

## Build on CentOS 7
```bash
tar -xzvf ./sqlited-x.y.z.tar.gz
cd sqlite-server-sqlited-x.y.z
chmod +x bin/*.sh
./bin/build.sh test jar
```

## Build on Windows 8.1
```shell
cd sqlite-server-sqlited-x.y.z
.\bin\build.bat test jar
```
