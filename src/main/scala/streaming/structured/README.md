# Structured Streaming Examples

Structured Streaming was introduced in Spark 2.0. Note that the utility classes
from the basic streaming examples (predating structured streaming)
are referenced here as well.

## Basic Examples

<table>
<tr><th>File</th><th>What's Illustrated</th></tr>

<tr>
<td><a href="Basic.scala">Basic.scala</a></td>
<td>
<p>A very simple example of structured streaming from a sequence of CSV files,
where the newly received records with each batch are dumped tot he console.</p>
</td>
</tr>

<tr>
<td><a href="BasicAggregation.scala">BasicAggregation.scala</a></td>
<td>
<p>Introduced the idea of one streaming DataFrame aggregating another.
With every batch, this time the entire updated aggregation is dumped to the
console.</p>
</td>
</tr>

</table>
