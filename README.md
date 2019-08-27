# CovenantSQL-Java-Connector

CovenantSQL-Java-Connector is a Type 4 Java JDBC driver for [CovenantSQL](https://covenantsql.io) database querying.

## Adding CovenantSQL-Java-Connector to your build 

To add a dependency using Maven, use the following:

```xml
        <dependency>
            <groupId>io.covenantsql</groupId>
            <artifactId>cql-java-connector</artifactId>
            <version>1.0.1</version>
        </dependency>
```

To add a dependency using Gradle:

```gradle
repositories {
    maven {
      url 'https://raw.github.com/CovenantSQL/cql-java-driver/mvn-repo'
    }
}

dependencies {
  compile 'io.covenantsql:covenantsql-java-connector:1.0-SNAPSHOT'
}
```

## Use CovenantSQL-Java-Connector with MyBatis

Configure the dataSource like the following example:

```xml
<dataSource type="POOLED">
  <property name="driver" value="io.covenantsql.connector.CovenantDriver"/>
  <property name="url" value="jdbc:covenantsql://${host}:${port}/${database}"/>
  <property name="driver.key_path" value="${key_path}"/>
  <property name="driver.cert_path" value="${cert_path}"/>
  <property name="driver.sslmode" value="${sslmode}"/>
  <property name="driver.ssl" value="${ssl}"/>
</dataSource>
```

Explanation:
 1. Use `io.covenantsql.connector.CovenantDriver` as the driver class.
 2. Replace `host` variable with the adapter host address.
 3. Replace `port` variable with the adapter port.
 4. Replace `database` variable with the adapter.
 5. Replace `key_path` variable with the https certificate private key path.
 6. Replace `cert_path` variable with the https certificate file path.
 7. Replace `sslmode` variable to use none/strict mode for https certificate check.
 8. Set `ssl` variable to true/false to enable/disable https adapter connection.
 
You can see a runnable example [Here](./example/src/main/java/io/covenantsql/connector/example/mybatis).   
