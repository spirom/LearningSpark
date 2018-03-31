# Data Source API V2

This is an exploration of the **_experimental_** data source V2 API introduced in Spark 2.3.0.

## Basics

<table>
<tr><th>File</th><th>What's Illustrated</th></tr>

<tr>
<td><a href="SimpleRowStoreSource.scala">SimpleRowStoreSource.scala</a></td>
<td>
<p>Simplest possible row-oriented data source with read and write support.
Note that this is *very* primitive and just illustrates the infrastructure.
All reading and writing are done directly from the data object on the driver,
which is not realistic for anything large. A more realistic example would access
the the data storage system remotely from the executors.</p>
</td>
</tr>

</table>
