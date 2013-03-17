# Fluent-Accumulo
===============

A fluent alternative to the Apache Accumulo 1.4.2 client.

The underlying interface is called **Cirrus**.  Cirrus is the highest type of cloud that forms in the sky,
a fitting name for a high-level abstraction to the "Accumulo API".

* Author:  Richard Clayton (Berico Technologies)
* License: Apache 2.0
* Status:  In-Development (not even an Alpha product)

> A special thanks to Keith Turner (https://github.com/keith-turner); his "Typo" project is used to Serialize/Deserialize Java primitives/objects in Fluent-Accumulo.

### Example

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

#### The new "Fluent" way

Functionally equivalent to the example above.

```java
new Cirrus("myinstance", "user", "passwd", "zooserver-one", "zooserver-two")
    .table("table")
       .mutate()
         .put("row1", "myColFam:myColQual:public").value("myValue");
```

#### More Features

The fluent API supports a number of interesting operations that will help reduce the amount of tedium in working with the Accumulo Client:

```java
Cirrus cirrus = new Cirrus("titan-test", "root", "password");

// Just for clarification.
long timestamp = 12345678l;

cirrus
  .table("us_cities")
  .mutate()
    // Add a single row by specifying Row ID, Column Family,
    // Column Qualifier, Value and Timestamps
    .put("usa.va.reston", "meta", "commonname", "SECRET").value("Reston", timestamp)
    // But you can also use Column Expressions to avoid writing separate strings
    // (and obviously the timestamp is optional).
    .put("usa.va.fairfax", "meta:commonname:SECRET").value("Fairfax")
    // Or you can operate on a single row, adding a number of cells
    .withRow("usa.va.manassas")
      // Strings!
      .column("meta", "commonname").value("Manassas")
      // Ints
      .column("meta", "zipcode", "SECRET").value(20110)
      // Doubles!
      .column("meta", "latitude", "SECRET").value(38.7514)
      .column("meta", "longitude", "SECRET").value(77.4764)
      // Dates!
      .column("meta", "last update").value(new Date())
      // UUIDs!
      .column("meta", "city.id").value(UUID.randomUUID())
      // BigInts!
      .column("meta", "population.ants").value(new BigInteger("12345678901234567890"))
      // And you can add a timestamp with values.
      .column("meta", "areacode", "SECRET").value(703, timestamp)
      // And take advantage of column expressions.
      .column("meta:state").value("Virginia")
      // And you can delete cells.
      .delete("meta", "some-cell")
      // As well as use Column Expressions on Deletes
      .delete("meta:some-other-cell")
      // All mutations are queued, executed only when done() is called.
      .done();
```
### Using Accumulo-Fluent via Maven

Reference the Berico Technologies Nexus Repository:

```xml
<repository>
  <id>nexus.bericotechnologies.com</id>
  <name>Berico Technologies Nexus</name>
  <url>http://nexus.bericotechnologies.com/content/groups/public</url>
  <releases><enabled>true</enabled></releases>
  <snapshots><enabled>true</enabled></snapshots>
</repository>
```

And you can refer to the dependency as:

```xml
<dependency>
  <groupId>com.berico</groupId>
  <artifactId>accumulo-fluent</artifactId>
  <version>1.0.1-SNAPSHOT</version>
</dependency>
```
