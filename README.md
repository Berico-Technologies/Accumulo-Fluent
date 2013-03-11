# Fluent-Accumulo
===============

A fluent alternative to the Apache Accumulo 1.4.2 client.

### Example

#### The new "Fluent" way
```java
new Cirrus("myinstance", "user", "passwd")
    .table("table")
       .mutate()
         .withRow("row1")
         .put("myColFam:myColQual:public", "myValue")
         .done();
```

#### Old way (From Accumulo example)

```java
String instanceName = "myinstance";
String zooServers = "zooserver-one,zooserver-two"
Instance inst = new ZooKeeperInstance(instanceName, zooServers);
Connector conn = inst.getConnector("user", "passwd");

Text rowID = new Text("row1");
Text colFam = new Text("myColFam");
Text colQual = new Text("myColQual");
ColumnVisibility colVis = new ColumnVisibility("public");
long timestamp = System.currentTimeMillis();

Value value = new Value("myValue".getBytes());

Mutation mutation = new Mutation(rowID);
mutation.put(colFam, colQual, colVis, timestamp, value);

long memBuf = 1000000L; // bytes to store before sending a batch
long timeout = 1000L; // milliseconds to wait before sending
int numThreads = 10;

BatchWriter writer =
    conn.createBatchWriter("table", memBuf, timeout, numThreads)

writer.add(mutation);

writer.close();
```
