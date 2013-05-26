mapr-spout
==========

A Storm spout that tails a file (or collection of files).

The basic idea is that you supply a parser for events that can read from an input stream and this spout will read from files whenever new data gets added.  The cool thing is that if this spout gets killed and restarted, it will know how to deal with it correctly and will pick up where it left off.

The way that this works is that there are many objects, each of which is observing a single directory.  Each time nextTuple() is called, one of the directory observers is asked to check its directory.  The current file in that directory is parsed until we hit a limit of the number of tuples to read in one call or the end of the file.  If we hit the end of the file, then we move to the next file in the directory of interest.  When we hit the limit on the number of files to parse from a single directory, that observer is moved to the end of the queue of observers.  This allows many messages to be pulled from a single directory while catching up but ultimately provides fair sharing between all live directories.  This strategy also decreases the overhead of checking files for modifications in an overload situation.

Every so often, the directory trees below the known roots is scanned to see if there are new directories that need observers.  If there are, then the appropriate new observers are created and inserted at the head of the queue so that they will catch up quickly.  This means that there can be a substantial delay before any messages are processed from a new directory (say 30s or so), but this will minimize the cost of scanning for new directories.

When running in reliable mode, tuples are held in memory until they are acknowledged.  

For right now, there is no driver for the spout.  All that is in place and tested are the DirectoryScanner and the SpoutState classes.

Preliminaries
==========

Install maven, git and java:

    sudo apt-get update
    sudo apt-get -y install maven
    sudo apt-get -y isntall openjdk-7-jdk
    sudo apt-get -y install git
    sudo apt-get -y install protobuf-compiler

Use

    sudo update-alternatives --config java

to select java 7.  If you want to set it without interaction, try:

    sudo update-alternatives --set java /usr/lib/jvm/java-7-openjdk-amd64/jre/bin/java

Start zookeeper
==========

The easiest way to get ZK running on many systems like ubuntu is to simply install the zookeeperd
package and start the service.

    sudo apt-get -y install zookeeperd

After you do this, zookeeper should be running on the standard port of 2181.  Zookeeper is
not needed for the SimpleCatcher used in the demo so you probably can just skip this step.

Download source and compile
==========

First compile and install mapr-spout:

    git clone git://github.com/boorad/mapr-spout.git
    cd mapr-spout
    mvn install -DskipTests

You can run the tests if you like.  They should take about a minute to run and should
complete successfully.  Note that there are some scaring looking log outputs along the
way as various failure modes are tested.

Note also that the first time you compile mapr-spout all kinds of dependencies will be
downloaded.  This should go much faster the second time around.

How to Run a Simple Catcher Server
==========

To run the simplest kind of catcher server, try this

    java -cp target/mapr-spout-0.1-SNAPSHOT-jar-with-dependencies.jar com.mapr.franz.simple.SimpleCatcher

This will run a catcher cluster that consists of just this server. The catcher will be
accessible on port 5900 by default and will store data into the directory
`/tmp/mapr-spout`. You can over-ride the port using the `-port PORT_NUM` option and can
specify the base directory using `-base DIRECTORY`.

A catcher without a traffic source is boring.  So you may want to run the included traffic simulator.  

    java -cp target/mapr-spout-0.1-SNAPSHOT-jar-with-dependencies.jar com.mapr.franz.Traffic localhost

This command requires a host name where you are running a catcher.  If you are running a SimpleCatcher, 
then there is only one hostname to be had, but if you are running a full cluster you can tell the 
traffic generator about any of them and it should discover the others.  There are a number of options
that can be set on the traffic generator, but they all have reasonable defaults.

One particularly interesting thing about generating traffic is that the SimpleCatcher has a magic topic
called `-metrics-`.  This topic contains messages recorded at invervals of 1 second, 10 seconds, 1 minute 
and 5 minutes.  Each of these messages contains the time, the time interval and the number of unique topics 
and total number of messages for the time interval being recorded.  These messages are formatted using
the `DataPoint` message from `Metric.proto`.

Data Collection Architecture
==========

The server supports two operations, `Hello` and `Log`. The `Hello` operation returns a
list of all known servers in the catcher cluster. The `Log` operation logs data for a
particular topic. If the topic is not being handled by the server that the request is
sent to, then that server will forward the request to the correct server. In any case,
the server will return a response that contains the correct server. It is expected that
the client will cache the topic to server mappings.

When a server fails or is taken out of service, the servers will rearrange which servers
handle which topics. When this is done, a considerable number of messages may have to be
forwarded, but the clients should quickly cache the new topic to server mappings and the
amount of forwarded messages should quickly decline.

Messages are encapsulated in a proto-buf envelope that records the topic and the time it
was received. Both the `Log` request format and the on disk format were designed so that
it should be possible to copy the message payload directly from the socket to off-heap
memory as the request is received and directly from this off-heap to disk as it is
stored. Current servers do not make use of this optimization, however.

The catcher uses the Kafka convention of having a directory per topic and naming the
files in that directory by the offset of the beginning of that file from the beginning
of all messages received. When a file gets large enough, the server starts a new file.
When a server first starts handling a topic, possibly because of the failure of another
server, a gap is left to ensure if the previous server reappears and belatedly writes
some messages that it is unlikely to write enough to overlap the beginning of the file
that is being written by the new server.

Queued data can be stored in any POSIX-like file system. For failure tolerance, a
distributed file system such as a MapR cluster mounted via NFS can be used. Changes to
files written via one NFS server may not be seen for a few seconds by a reader reading
from another NFS server. If you use the `noac` option when mounting the NFS partition,
this delay can be made much shorter (as little as a few tens of milliseconds) at the
cost of considerably more chatting with the NFS server and between the NFS server and
the MapR cluster.

Missing bits
==========

There are two kinds of distributed catcher server that are under construction. One type
uses Zookeeper to assign topics and to determine which servers are live. They other type
uses Hazelcast for the same job. The Zookeeper approach avoids split brain problems at
the catcher cluster level, but requires an external Zookeeper cluster to work well. The
Hazelcast system is much easier to configure and start since it is self-organizing.
Since split brain issues will be handled by the distributed file system in any case, the
Hazelcast approach will probably eventually be preferred. Neither clustered catcher
system is ready for production at this time.
