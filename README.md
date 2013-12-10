riakycsb
========

1. git pull
2. export the code to riakycsb.jar. You may need some library jars file. Download them here:
https://www.dropbox.com/sh/x51gvnrsegqwiqo/SXXErjpdyQ
3. To run ycsb with riak client:

java -cp path/to/ycsb/core/core-0.1.4.jar:/path/to/lib/jars/*:/path/to/riakycsb/jar/riakycsb.jar com.yahoo.ycsb.Client -load -db com.son.riakycsb.RiakClient -P workloads/workloada -s -p hosts="127.0.0.1:10018,127.0.0.1:10028,127.0.0.1:10038"

the command above will execute the -load phase. To execute the transaction phase, use -t instead

hosts is the list of addresses and ports of the servers.
