# lfchring


[![Go Report Card](https://goreportcard.com/badge/github.com/ckatsak/lfchring)](https://goreportcard.com/report/github.com/ckatsak/lfchring)
[![GoDoc](https://godoc.org/github.com/ckatsak/lfchring?status.svg)](https://godoc.org/github.com/ckatsak/lfchring)
[![GoCover](http://gocover.io/_badge/github.com/ckatsak/lfchring)](http://gocover.io/github.com/ckatsak/lfchring)


Package lfchring provides a *wait-free* consistent hashing ring immutable
in-memory data structure, designed for very efficient frequent reading by
**multiple readers** and less frequent updates by a **single writer**.

It features efficient handling of a static number of virtual ring nodes per
distinct ring node, as well as auto-managed data replication information
(using a static replication factor).
It also allows users to pass the hash function of their choice, further
improving its flexibility.

The API is simple, easy to use, and is documented in
[godoc](https://godoc.org/github.com/ckatsak/lfchring).

It has no external dependencies.
