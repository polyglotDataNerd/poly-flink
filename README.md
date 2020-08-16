# Flink

**Apache Flink**

Apache Flink Examples EMR

Dependencies:
* [AWS S3](https://aws.amazon.com/s3/)
* [Apache Flink 1.11](https://ci.apache.org/projects/flink/flink-docs-stable/)
* [Scala 2.12](https://www.scala-lang.org/download/2.12.10.html)

TODO:

Batch
-

Stream
-  
 

Table
-

To interact with Scala Shell on EMR ssh into the cluster and type commands running r5d.xlarge.
    
        aws s3 cp s3://bigdata-utility/jars/flink-sql-orc_2.12-1.11.0.jar .
        
        /usr/lib/flink/bin/start-scala-shell.sh yarn -s 10 -jm 1024m -tm 4096m --addclasspath /home/hadoop/flink-sql-orc_2.12-1.11.0.jar
        
 * -s: number of slots = number of cores
 * -jm: job manager memory
 * -tm: taskmanager memory

CLI Interaction
 * [Flink CLI](https://ci.apache.org/projects/flink/flink-docs-stable/ops/cli.html)