Fault Tolerance
===============

Axiom has built-in fault tolerance and recoverability in the form of sandboxed processing of each variable. This ensures that exceptions do not cause the system to crash, allowing subsequent variables to continue processing if a prior variable should fail to process for whatever reason.

A list of regular expressions is maintained in the ``drs.json`` configuration file (``recoverable_errors``) which allow the system to query stacktraces and cope with any transient errors due to HPC configuration and where a simple re-running of the task will fix the problem without human intervention.