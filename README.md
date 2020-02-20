# n-orm.mongo
[![Build Status](https://travis-ci.org/fondemen/n-orm.mongo.svg?branch=master)](https://travis-ci.org/fondemen/n-orm.mongo)
[![codecov](https://codecov.io/gh/fondemen/n-orm.mongo/branch/master/graph/badge.svg)](https://codecov.io/gh/fondemen/n-orm.mongo)

Automatically exported from code.google.com/p/n-orm.mongo

This is the [MongoDB](https://www.mongodb.org) connector for [n-orm](https://github.com/fondemen/n-orm.core).
This driver is based on the java mongo driver v2.

## `store.properties` ##
To store your objects using Mongo, you need to specify a `store.properties` file as the following:

```
class=com.googlecode.n_orm.mongo.MongoStore
host=localhost
port=27017
db=mydb
user=theuser # optional
password=tH3p45sW0rD # optional
```
Javadoc is available [here](https://fondemen.github.io/n-orm.mongo/mongoDB/apidocs/).

### Where to place `store.properties` ? ###

The `store.properties` file will be looked up in the classpath for each [persisting](https://fondemen.github.io/n-orm.core/storage/apidocs/index.html?com/googlecode/n_orm/Persisting.html) class, first in the same package, then in the package above, etc. For instance, for a classpath set to `srcfolder1:src/folder2:jar1.jar`, the store file for class a.b.C will be searched in the following places:
  1. `srcfolder1/a/b/store.properties`
  1. `src/folder2/a/b/store.properties`
  1. `a/b/store.properties` from jar file `jar1.jar`
  1. `srcfolder1/a/store.properties`
  1. `src/folder2/a/store.properties`
  1. `a/store.properties` from jar file `jar1.jar`
  1. `srcfolder1/store.properties`
  1. `src/folder2/store.properties`
  1. `store.properties` from jar file `jar1.jar`
  
The first found file is the right file.

## Maven integration ##

See the [getting started](https://github.com/fondemen/n-orm.core/wiki/GettingStarted#using-n-orm-with-maven) article, and the [pom](https://github.com/fondemen/n-orm.sample/blob/mongo/pom.xml) for the sample project.

Instead of importing the `store` artifact, you can use the `mongo` one:
```
<dependency>
  <groupId>com.googlecode.n_orm</groupId>
  <artifactId>mongo</artifactId>
  <version>0.0.1-SNAPSHOT</version>
  <type>jar</type>
  <scope>compile</scope>
</dependency>
```

### Example `pom.xml` ###

You can ispire yourself from the [sample project](https://github.com/fondemen/n-orm.sample/tree/mongo).

````
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
 
    <groupId>my.group</groupId>
    <artifactId>myproject</artifactId>
    <version>0.0.1-SNAPSHOT</version>
 
    <name>myproject</name>
 
    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <java.version>1.6</java.version>
        <n-orm.version>0.0.1-SNAPSHOT</n-orm.version>
        <aspectj.version>1.7.3</aspectj.version>
        <plugin.aspectj.version>1.4</plugin.aspectj.version>
    </properties>
 
    <dependencies>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.10</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.googlecode.n_orm</groupId>
            <artifactId>storage</artifactId>
            <version>${n-orm.version}</version>
            <type>jar</type>
        </dependency>
        <dependency>
            <groupId>com.googlecode.n_orm</groupId>
            <artifactId>mongo</artifactId>
            <version>${n-orm.version}</version>
        </dependency>
        <dependency>
            <groupId>org.aspectj</groupId>
            <artifactId>aspectjrt</artifactId>
            <version>${aspectj.version}</version>
            <type>jar</type>
            <scope>compile</scope>
        </dependency>
    </dependencies>
 
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.1</version>
                <configuration>
                    <source>${java.version}</source>
                    <target>${java.version}</target>
                    <excludes>
                        <!-- excluding business model as it must be compiled using AspectJ -->
                        <exclude>**/*.java</exclude>
                        <!-- no need to exclude tests as no persisting class is defined there -->
                    </excludes>
                </configuration>
            </plugin>
 
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>aspectj-maven-plugin</artifactId>
                <version>${plugin.aspectj.version}</version>
                <configuration>
                    <source>${java.version}</source>
                    <target>${java.version}</target>
                    <complianceLevel>${java.version}</complianceLevel>
                    <aspectLibraries>
                        <aspectLibrary>
                            <groupId>com.googlecode.n_orm</groupId>
                            <artifactId>storage</artifactId>
                        </aspectLibrary>
                    </aspectLibraries>
                </configuration>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>       <!-- use this goal to weave all your main classes -->
                            <goal>test-compile</goal>  <!-- use this goal to weave all your test classes -->
                        </goals>
                    </execution>
                </executions>
                <dependencies>
                    <dependency>
                        <groupId>org.aspectj</groupId>
                        <artifactId>aspectjtools</artifactId>
                        <version>${aspectj.version}</version>
                    </dependency>
                </dependencies>
            </plugin>
        </plugins>
    </build>
    <repositories>
        <repository>
            <id>org.sonatype.oss.public</id>
            <name>OSS public</name>
            <url>http://oss.sonatype.org/content/groups/public</url>
        </repository>
    </repositories>
 
</project>
````
