mapr-spout
==========

A Storm spout that tails a file (or collection of files)

The basic idea is that you supply a parser for events that can read from an input stream and this spout will read from files whenever new data gets added.  The cool thing is that if this spout gets killed and restarted, it will know how to deal with it correctly and will pick up where it left off.

The way that this works is that each time nextTuple() is called, the current file is parsed until we hit a limit 
of the number of tuples to read in one call or the end of the file.  If we hit the end of the file, then we move to the 
next file in the directory of interest.

When running in reliable mode, tuples are held in memory until they are acknowledged.  

For right now, there is no driver for the spout.  All that is in place and tested are the DirectoryScanner and the 
SpoutState classes.

Data Collection Architecture
==========

- data collection will be via an API similar to (if not identical to) Kafka's.  The client should be able to interrogate Zookeeper to find the current live data collector

- there will be a fail-over data collector process that appends to the data files

- disorderly fail-over of the collector will result in a few seconds of data loss.  Orderly fail-over should not result in any data loss

- processing of data will be via a Storm spout or a map-reduce batch process that reads a snapshot of the data

- the system will work on local files but will require MapR for failure tolerance


Missing bits
==========

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
