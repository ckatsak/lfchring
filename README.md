# lfchring

Package lfchring provides an efficient lock-free consistent hashing ring
data structure, designed for frequent reading by multiple readers and less
frequent updates by a single writer.

It features efficient handling of a static number of virtual ring nodes per
distinct ring node, as well as auto-managed data replication information
(using a static replication factor), and an easy-to-use interface.

It also offers to the users flexibility to choose their own hash function,
and there is no dependency other than the standard library.
