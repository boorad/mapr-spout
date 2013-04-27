mapr-spout
==========

A Storm spout that tails a file (or collection of files).

The basic idea is that you supply a parser for events that can read from an input stream and this spout will read from files whenever new data gets added.  The cool thing is that if this spout gets killed and restarted, it will know how to deal with it correctly and will pick up where it left off.

The way that this works is that there are many objects, each of which is observing a single directory.  Each time nextTuple() is called, one of the directory observers is asked to check its directory.  The current file in that directory is parsed until we hit a limit of the number of tuples to read in one call or the end of the file.  If we hit the end of the file, then we move to the next file in the directory of interest.  When we hit the limit on the number of files to parse from a single directory, that observer is moved to the end of the queue of observers.  This allows many messages to be pulled from a single directory while catching up but ultimately provides fair sharing between all live directories.  This strategy also decreases the overhead of checking files for modifications in an overload situation.

Every so often, the directory trees below the known roots is scanned to see if there are new directories that need observers.  If there are, then the appropriate new observers are created and inserted at the head of the queue so that they will catch up quickly.  This means that there can be a substantial delay before any messages are processed from a new directory (say 30s or so), but this will minimize the cost of scanning for new directories.

When running in reliable mode, tuples are held in memory until they are acknowledged.  

For right now, there is no driver for the spout.  All that is in place and tested are the DirectoryScanner and the SpoutState classes.

How to Run a Catcher Server
==========

To run a catcher, you need a running Zookeeper.  Suppose you have downloaded zookeeper and are running a single node on port 2081.  To compile and running a Catcher, you need to do this

    mvn package -DskipTests
    java -cp target/mapr-spout-0.1-SNAPSHOT-jar-with-dependencies.jar com.mapr.franz.server.Server localhost 9004 localhost:2081

This will run a catcher that uses the Zookeeper server running on the localhost to store configuration information about the "cluster" of Catchers consisting of just this server.  The catcher will be accessible on port 9004.

Data Collection Architecture
==========

Data collection consists of appending to files and rolling to new files for existing directories.  When messages are received that should be stored in new directories, those directories should be created and then business as usual should proceed.

For convenience, it is nice to emulate API's like that of Kafka.  In such an emulation, it is probably good to have a single directory per topic and topics should be assigned to particular API end-points.  The API servers should monitor Zookeeper to see when the collection of API servers changes.  

Messages that arrive at the wrong API server for a particular topic should be forwarded to the correct server and the reply to the client should contain the correct API server to use.  This will allow clients to rapidly adapt to changes in the API serving farm without having to access Zookeeper directly.  It will also allow clients to robust to network misconfigurations that prevent them from contacting all of the API servers.

- data collection will be via an API similar to (if not identical to) Kafka's.  The client should be able to interrogate Zookeeper to find the current live data collector

- there will be a fail-over data collector process that appends to the data files

- disorderly fail-over of the collector will result in a few seconds of data loss.  Orderly fail-over should not result in any data loss

- processing of data will be via a Storm spout or a map-reduce batch process that reads a snapshot of the data

- the system will work on local files but will require MapR for failure tolerance


Missing bits
==========

The queueing of directory servers is missing.  So is the data collection API and servers.

I haven't written any parsers yet.  Such an exercise might expose some interesting problems.

Likewise, we should have a couple of different strategies to handle the situation when there are lots of pending 
tuples.  One strategy is to simply drop tuples if we have too many pending tuples.  Another strategy is to only emit 
tuples when there are no more than a critical number of tuples pending.  

The first strategy will not quench the volume of tuples entering the system and seems really error prone.

The second strategy is much more conservative in that it implements a viable form of source quenching, but it might 
be prone to stalling if one of many bolts gets very slow.  Hopefully, the heartbeats will let Storm handle this by 
killing such pathological bolts.

Another potential problem is that on restart, we replay all files that had a pending tuple from the point where the 
pending tuple started.  That could conceivably replay lots of tuples that have been acked.  Whether this is really 
a problem is an open question.
