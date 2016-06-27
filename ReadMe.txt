   _____  _  _
  / ____|| || |
 | (___  | || |_
  \___ \ |__   _|
  ____) |   | |
 |_____/    |_|

 You just created a new S4 project in /home/miyuru/projects/pamstream/S4/s4-LinearRoad!

 It follows a maven-like structure:
 - the build file is at the root of the project
 - sources are in src/main/java

 We use gradle for building this project.

 To build the project:
 - "./gradlew" (from the root of the project, this calls the gradle script from the s4 installation)

 An "s4" script has been created at the root of the project's directory. It calls the s4 script from your S4 installation.


 To execute the application in a new S4 cluster (see the S4 wiki https://cwiki.apache.org/confluence/display/S4/ for more information):
 1. start a ZooKeeper instance "./s4 zkServer" (./s4 zkServer -help provides a list of options)
 2. define a logical cluster for your application "./s4 newCluster -cluster=<nameOfTheCluster> -nbTasks=<number of partitions> -flp=<a port number for the first node, other nodes use an increment on this initial port>"
 3. start a node and attach it to the cluster "./s4 node -cluster=<nameOfTheCluster>"
 4. package the application "./s4 s4r -a=<app class> -b=`pwd`/build.gradle <package name>
 5. deploy (configure) the application "./s4 deploy -s4r=`pwd`/build/libs/<package name>.s4r -c=<nameOfTheCluster> -appName=<app name in Zookeeper>"
 6. check the status of the S4 cluster "s4 status"

 If you want to inject events from application 2 into application 1 on cluster 1:
 - application 1 must define an input stream with a name (say: stream1)
 - application 2 must define an output stream with the same name (stream1)

 If you want to use a simple adapter process, listening to an external source, converting incoming data into S4 events, and sending that to S4 apps, you can define
 your own app that extends the AdapterApp class, for instance:
 ./s4 deploy -appClass=hello.HelloInputAdapter -p=s4.adapter.output.stream=names -cluster=c2 -appName=adapter
 
 Then in order to start a node and automatically using the classpath of the current project, use the "s4 adapter" command:
 - ./s4 adapter -cluster=c2

#Starting the S4 cluster
./s4 zkServer -t -port 2182

#Running the S4 Linear Road
First we need to pack the compiled content into an S4 archive

./gradlew build --info
./s4 s4r -a=org.linear.s4.RunLinearRoad -b=/home/miyuru/workspace-java/s4-LinearRoad/build.gradle s4-LinearRoad

./s4 deploy -appClass=org.linear.s4.input.InputEventInjectorAdapter -p=s4.adapter.output.stream=names -cluster=testCluster2 -appName=adapter -zk localhost:2182
./s4 deploy -s4r=/home/miyuru/workspace-java/s4-LinearRoad/build/libs/s4-LinearRoad.s4r -appName=InputEventInjectorAdapter

#Also we need to setup the node to listen to.
./s4 node -c=testCluster1 -zk localhost:2182

