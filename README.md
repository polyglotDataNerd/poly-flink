# Flink

**Apache Flink**

Apache Flink Examples EMR

Dependencies:
* [AWS S3](https://aws.amazon.com/s3/)
* [Apache Flink 1.10](https://ci.apache.org/projects/flink/flink-docs-stable/)
* [Scala 2.12](https://www.scala-lang.org/download/2.12.10.html)

Intention
-

Frequency
-  
 

Interaction
-

To interact with Scala Shell on EMR ssh into the cluster and type commands running r5d.xlarge.
    
        /usr/lib/flink/bin/start-scala-shell.sh yarn -s 4 -jm 10728m -tm 10728m
        
 * -s: number of slots = number of cores
 * -jm: job manager memory
 * -tm: taskmanager memory

        