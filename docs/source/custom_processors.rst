Custom Processors
=================

Axiom has been designed to be as general as possible, stripping out as much proprietary/legacy code as possible to provide a generic interface for DRS processing. However, there are some situations where a higher level of processing is required in order to make previously untested data pass through the system without error.

Rather than continuing to modify the central codebase to address every edge-case, a simple plugin system has been developed in the form of pre- and post-processors. These processors are invoked at key points in the processing system, namely once data is loaded and just prior to when it is written out, and allow the user to make just-in-time modifications to the data prior to entering generic routines. One common use-case is to rename model variables into standard names such that Axiom can adequately process them, but far more complicated modifications can also be made.

Custom processors should be used sparingly and with caution, as the code contained within them is user-provided and usually untested in the Axiom DRS subsystem. It is preferable to write custom configurations that address your specific need, or ensure model data is written as close to what Axiom expects as possible.

How to write custom processors
------------------------------