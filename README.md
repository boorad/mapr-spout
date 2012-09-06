mapr-spout
==========

A Storm spout that tails a file (or collection of files)

The basic idea is that you supply a parser for events that can read from an input stream and this spout will read from files whenever new data gets added.  The cool thing is that if this spout gets killed and restarted, it will know how to deal with it correctly and will pick up where it left off.
