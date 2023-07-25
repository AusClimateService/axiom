Performance
===========

Diagnosing performance issues on running DRS processing tasks involves connecting to the dask dashboard of the running process. The dashboard link will be supplied in the logs after the dask client has been instantiated, look for the line containing "dashboard".

```shell
grep 'dashboard' /path/to/log
```

To actually connect to this dashboard will depend on where you are actually running Axiom. If you are on your local machine, then you can do directly to the url provided in the logs. If you are running on a cluster/supercomputer (i.e. Gadi) you will need to forward the port of the compute node back to your local machine first.


```shell
# On your local machine...
# $PORT will be listed at the end of the URL in the logs
# $HOST is the node ID from Gadi, you can use qstat -f to find it
ssh -N -L $PORT:gadi-cpu-clx-$HOST.gadi.nci.org.au:$PORT $NCI_USER@gadi.nci.org.au
# Navigate to http://localhost:$PORT
```

Troubleshooting
---------------

*Why are my workers dying?*

More often than not this is the result of running out of memory, which is probably the most common challenge with Dask. Some things to try are:

- Throw more memory at the problem.
- Reduce the number of workers to 1 in your drs.json configuration file.
- Try threads vs processes and vice version in your cluster configuration (drs.json).
- Connect to the dashboard and check the graph for bottlenecks. Some operations are memory-inefficient (i.e. where clauses) so you might be able to refactor them out.

Why is the processing job taking so long?