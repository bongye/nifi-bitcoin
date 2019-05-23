# nifi-bitcoin
Apache NiFi Custom Processor Development Training by Jeff Markham

## Getting Started
These guides are based on IntelliJ. NiFi version 1.9.2.

New Project > Maven > Create from archetype > Add archetype ...
* groupId = org.apache.nifi
* artifactId = nifi-processor-bundle-archetype
* version = 1.9.2 (NiFi version)
* repository = http://repo.maven.apache.org/maven2/archetype-catalog.xml
* Need to set artifactBaseName as bitcoin

## Cloud Instance Setup
* Install JDK
* Setup ACG to open port 8080(for NiFi), 5050(for pgadmin4)
* Download NiFi - http://apache.mirror.cdnetworks.com/nifi/1.9.2/nifi-1.9.2-bin.tar.gz
* Unzip NiFi
* Copy nifi-bitcoin-nar.jar to nifi-1.9.2/lib
* Run NiFi - nifi-1.9.2/bin/nifi.sh start
* Find BitcoinHistoryProcessor among nifi processors.
