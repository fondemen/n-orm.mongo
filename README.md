# n-orm.mongo
Automatically exported from code.google.com/p/n-orm.mongo

This is the [MongoDB](https://www.mongodb.org) connector for [n-orm](https://github.com/fondemen/n-orm).

Here is an example `store.properties`:

````
class=com.googlecode.n_orm.mongo.MongoStore
host=localhost
port=27017
db=mydb
```

Here is an example `pom.xml`:

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
            <version>0.0.1-SNAPSHOT</version>
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
