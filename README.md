# Datastore
[Georgia Clinical and Translational Science Alliance (Georgia CTSA)](http://www.georgiactsa.org), [Emory University](http://www.emory.edu), Atlanta, GA

## What does it do?
It is a utility library that wraps Berkeley DB's key-value store.

## Version history
Latest release: [![Latest release](https://maven-badges.herokuapp.com/maven-central/org.eurekaclinical/datastore/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.eurekaclinical/datastore)

### Version 2.0
Minor cleanup.

### Version 1.0
* Initial version, used for caching in Eureka! Clinical.

## Build requirements
* [Oracle Java JDK 8](http://www.oracle.com/technetwork/java/javase/overview/index.html)
* [Maven 3.2.5 or greater](https://maven.apache.org)

## Runtime requirements
* [Oracle Java JRE 8](http://www.oracle.com/technetwork/java/javase/overview/index.html)

## Building it
The project uses the maven build tool. Typically, you build it by invoking `mvn clean install` at the command line. For simple file changes, not additions or deletions, you can usually use `mvn install`. See https://github.com/eurekaclinical/dev-wiki/wiki/Building-Eureka!-Clinical-projects for more details.

## Maven dependency
```
<dependency>
    <groupId>org.eurekaclinical</groupId>
    <artifactId>datastore</artifactId>
    <version>version</version>
</dependency>
```

## Developer documentation
* [Javadoc for latest development release](http://javadoc.io/doc/org.eurekaclinical/datastore) [![Javadocs](http://javadoc.io/badge/org.eurekaclinical/datastore.svg)](http://javadoc.io/doc/org.eurekaclinical/datastore)

## Getting help
Feel free to contact us at help@eurekaclinical.org.
