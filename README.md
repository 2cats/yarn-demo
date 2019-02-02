# A Simple Yarn Application Demo
## Run
```bash
mvn clean package
HADOOP_HOME="YOUR HADOOP HOME"
APP_JAR="target/yarn-demo-1.0.0.jar"
COMMAND="date >> /tmp/yarn-demo.log"
NUM_CONTAINERS=4
"$HADOOP_HOME"/bin/hadoop jar "$APP_JAR" com.tcats.yarndemo.Client \
      "$COMMAND" "$NUM_CONTAINERS" "$APP_JAR" 
```