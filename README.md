Usage
-----

You need go 1.12 or later installed.

To build:

make; bin/yacht


What this program does
----------------------

On start, it connects to an existing Cassandra instance, 
creates 'yacht' keyspace, and on shutdown deletes the keyspace.
