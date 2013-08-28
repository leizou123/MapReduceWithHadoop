
Step 1: Enable SSH to localhost

Step 1: Enable SSH to localhost
Go to System Preferences > Sharing.
Make sure “Remote Login” is checked.

$ ssh-keygen -t rsa
$ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
$ ssh localhost


Step 2: Install Hadoop
Download the latest Hadoop core (Hadoop hadoop-1.2.1.tar.gz) . 

$ tar xvfz hadoop-1.2.1.tar.gz 
$ mv hadoop-1.2.1 /usr/local/

Most likely, you need the root privilege to do the mv . 
$ sudo mv hadoop-1.2.1 /usr/local/ 


Step 3: Configure Hadoop

Change directory to the  where Hadoop is installed and configure the Hadoop. 

$ cd /usr/local/hadoop-1.2.1
$ mkdir /usr/local/hadoop-1.2.1/dfs/

Add the following to conf/core-site.xml: 

<property>
    <name>fs.default.name</name>
    <value>hdfs://localhost:9000</value>
</property>


Add the following to conf/hdfs-site.xml: 

<configuration>

<property>
	<name>dfs.name.dir</name>
        <value>/usr/local/hadoop-1.2.1/dfs/name</value>
        <description>Path on the local filesystem where the NameNode stores the namespace and transactions logs persistently</description>
</property>


<property>
        <name>dfs.data.dir</name>
        <value>/usr/local/hadoop-1.2.1/dfs/data</value>
        <description>Comma separated list of paths on the local filesystem of a DataNode where it should store its blocks.</description>
</property>

<property>
    	<name>dfs.replication</name>
    	<value>1</value>
</property>

</configuration>


Add the following to conf/mapred-site.xml: 

<property>
    <name>mapred.job.tracker</name>
    <value>localhost:9001</value>
</property>


Add the following to conf/hadoop-env.sh: 

export HADOOP_OPTS="-Djava.security.krb5.realm= -Djava.security.krb5.kdc="
export JAVA_HOME=/Library/Java/Home



Step 4: Configure environment variables for Hadoop

Edit ~/.bash_profile, to add the followings : 

## set Hadoop environment 
export HADOOP_HOME=/usr/local/hadoop-1.2.1
export HADOOP_CONF_DIR=/usr/local/hadoop-1.2.1/conf
export PATH=$PATH:$HADOOP_HOME/bin

Step 5: Format Hadoop filesystem

Run the hadoop command to format the HDFS system. 

$ hadoop namenode -format

Step 6: Start Hadoop

Run the start-all.sh command to start the Hadoop. 

$ start-all.sh

To verify that all Hadoop processes are running:

$ jps
9460 Jps
9428 TaskTracker
9344 JobTracker
9198 DataNode
9113 NameNode
9282 SecondaryNameNode


To run a sample Hadoop MapReduce job (calculating Pi) that comes from hadoop-examples-1.2.1.jar:
$ hadoop jar /usr/local/hadoop-1.2.1/./hadoop-examples-1.2.1.jar pi 10 100

To stop hadoop:
$ stop-all.sh 



Web interface for Hadoop NameNode: http://localhost:50070/
Web interface for Hadoop JobTracker: http://localhost:50030/


Run MapReduce job to count words and produce sorted output 

Use mvn to start a Hadoop MapReduce Java project:
$ mvn archetype:generate -DgroupId=com.lei.hadoop -DartifactId=MapReduceWithHadoop -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false 


$ cd MapReduceWithHadoop/


Add the following dependency to pom.xml: 
<dependency>
	<groupId>org.apache.hadoop</groupId>
	<artifactId>hadoop-core</artifactId>
	<version>1.2.1</version>
</dependency>


To build the target jar: 
$ mvn clean compile package 


To generate eclipse project: 
$ mvn eclipse:eclipse 


To see the content of the input 
$ cat wordcount_input/wordcount.txt 



To run MapReduce job that counts words in text files at local folder wordcount_input, and generate result in sorted format: 
$ hadoop jar ./target/MapReduceWithHadoop-1.0-SNAPSHOT.jar com.lei.hadoop.countword.CountWordsV2 ./wordcount_input ./wordcount_output 


To see the result from output folder wordcount_output: 
$ tail wordcount_output/part-00000
5	Apache
5	a
6	distributed
8	data
8	to
8	of
10	and
10	Hadoop
11	for
12	A




