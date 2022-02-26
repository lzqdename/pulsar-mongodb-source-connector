# Mongo Source/Sink for Apache Pulsar

This is mongo source/sink connectors for Pulsar, enjoy!

## Documentation(source)

The params of Mongo Source :

 mongoUri 
 - The uri of mongodb that the connector connects to 
 - (see: https://docs.mongodb.com/manual/reference/connection-string/)
 
 database
 - the database name to be watched
 - null ,it means to watch all databases
 - not null , it means to just watch a database
 
 collection
 - the collection to be watched
 - if database is null ,  collection will be ignored
 - if database is not null , collection is not null , it means that we are just interested in a single collection
 - if database is not null , collection is null, it means to watch all collections of the database

 batchSize
 - The batch size of read from the database used for the mongodb cursor
 - default 100
 
 copyExisting
 - whether or not to copy existing mongo data
 - run at the first time , this value works
 - if the copy task does not run successfully , then it will continue to copy from head at the next time
 - once copy task run successfully , this value will be ignored forever
 - default false
 
 copyExistingNamespaceRegex
 - the regix used to filter namespace in copy existing document , used to filter the namespaces
 - default empty string

 copyExistingMaxThreads
 - thread count used for copy executorService , the final value will depends on the cpu and mongo namespaces also
 - default 8

 globalQueueSize
 - the queue size used for this source to put mongodb document
 - default 2000

## Documentation(sink)

TODO

## Download

TODO

## Build

please execute commands as follows to build the nar file

    git clone https://github.com/lzqdename/pulsar-mongodb-source-connector.git
    cd pulsar-mongodb-source-connector
    mvn clean package -Dmaven.test.skip=true

then , you can find the nar file at pulsar-mongodb-source-connector/target/pulsar-io-mongo-[the version].nar

## How to run source

1) create test environment for mongodb , for example (on Centos 8) :

    ```bash
    mkdir -p /root/mongodb && cd /root/mongodb
    mkdir dbpath0 dbpath1 dbpath2
    wget https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-rhel80-5.0.4.tgz
    tar -zvxfmongodb-linux-x86_64-rhel80-5.0.4.tgz
    /root/mongodb/mongodb-linux-x86_64-rhel80-5.0.4/bin/mongod --fork --replSet myreplSet --bind_ip=0.0.0.0 --port=27017 --dbpath=/root/mongodb/dbpath0 --logpath=/root/mongodb/mongodb0.log --logappend
    /root/mongodb/mongodb-linux-x86_64-rhel80-5.0.4/bin/mongod --fork --replSet myreplSet --bind_ip=0.0.0.0 --port=27018 --dbpath=/root/mongodb/dbpath1 --logpath=/root/mongodb/mongodb1.log --logappend
    /root/mongodb/mongodb-linux-x86_64-rhel80-5.0.4/bin/mongod --fork --replSet myreplSet --bind_ip=0.0.0.0 --port=27019 --dbpath=/root/mongodb/dbpath2 --logpath=/root/mongodb/mongodb2.log --logappend
    ```  
   please enter shell
   
    ```bash
    /root/mongodb/mongodb-linux-x86_64-rhel80-5.0.4/bin/mongo
    ```
   after enter shell, type as follows and press enter key:

    ```bash
    config_myreplSet={
    _id:"myreplSet",
    members:
     [
      {_id:0,host:"localhost:27017",priority:4},
      {_id:1,host:"localhost:27018",priority:2},
      {_id:2,host:"localhost:27019",arbiterOnly:true}
     ]
    }
    ;
    ```
   then , execute as follows :
   
    ```bash
    rs.initiate(config_myreplSet)
    ```
    
   check the status : 

    ```bash
    rs.status()
    ```
   create db and insert document into collection mytable , for example :

    ```bash
    use test;
    db.createCollection("mytable");
    db.mytable.insert({"text":"hello world"})
    db.mytable.find()
    ```

   bingo , the test environment has been build successfully !
   
2) start standalone pulsar

    ```bash
    mkdir -p /root/pulsar && cd /root/pulsar
    wget https://dlcdn.apache.org/pulsar/pulsar-2.9.1/apache-pulsar-2.9.1-bin.tar.gz
    tar -zvxf apache-pulsar-2.9.1-bin.tar.gz
    mv apache-pulsar-2.9.1 pulsar-2.9.1-bin
    /root/pulsar/pulsar-2.9.1-bin/bin/pulsar standalone
    ```
   if you want to start in daemon mode , you can try

    ```bash
    nohup /root/pulsar/pulsar-2.9.1-bin/bin/pulsar standalone 1>/dev/null  2>&1 &
    ```
3) start pulsar io - mongo source task in 【localrun】 mode just for test

    ```bash
    mkdir -p /root/pulsar/pulsar-2.9.1-bin/connectors
    ```

   move the nar file to /root/pulsar/pulsar-2.9.1-bin/connectors
   
   now ,let us prepare config file, please create file - /root/pulsar/pulsar-2.9.1-bin/connectors/mongo-source-config.yaml
   and then, the content is as follows :
   

   
    ```bash
    tenant: public
    namespace: default
    name: pulsar-mongo-source    
    parallelism: 1
    topicName: mongo-source-topic-test
    archive: /root/pulsar/pulsar-2.9.1-bin/connectors/pulsar-io-mongo-2.9.1.nar
    #used by your mongo source
    configs: {"mongoUri":"mongodb://localhost:27017,localhost:27018,localhost:27019","database":null,"collection":null,"batchSize":1000}
    ```

   very important ! 
   1) the parallelism can just be 1 , or error will happen
   2) after task runs , the resume token will be saved periodically , the key of the resume token in the state store is decided by the value joined by tenant/namespace/name , and the resume token will be read when starting , so if you want to start a source task with different resume token , you can use another config file , more details see MongoSource.java
   
   ok , let us start the localrun pulsar io mongo source task

    ```bash
    export PULSAR_HOME=/root/pulsar/pulsar-2.9.1-bin
    /root/pulsar/pulsar-2.9.1-bin/bin/pulsar-admin sources localrun --source-config-file /root/pulsar/pulsar-2.9.1-bin/connectors/mongo-source-config.yaml --state-storage-service-url bk://127.0.0.1:4181/ --broker-service-url pulsar://localhost:6650/  
    ```
4) start pulsar consume client

    ```bash
    /root/pulsar/pulsar-2.9.1-bin/bin/pulsar-client consume mongo-source-topic-test -s "first-subscription" -n 0
    ```

5) handle mongo document see 1)

6) observe the shell output of 4)

   for example

   copy
   
    ```bash
    ----- got message -----
    key:[{"_id": {"$oid": "62112e29ad9d5ad972f2bbad"}}], properties:[], content:{"clusterTime":7068881897035661795,"fullDocument":"{\"_id\": {\"$oid\": \"62112e29ad9d5ad972f2bbad\"}, \"text\": \"123456\"}","ns":{"databaseName":"test","collectionName":"mytable1","fullName":"test.mytable1"},"operation":"copy"}
    ```
       
   
   insert
   
    ```bash
    ----- got message -----
    key:[{"_id": {"$oid": "6219b897fd2cdc8aa3ac8791"}}], properties:[], content:{"clusterTime":7068884048814276609,"fullDocument":"{\"_id\": {\"$oid\": \"6219b897fd2cdc8aa3ac8791\"}, \"text\": \"insert\"}","ns":{"databaseName":"test","collectionName":"mytable","fullName":"test.mytable"},"operation":"insert"}
    ```
    
   update
    
    ```bash
    ----- got message -----
    key:[{"_id": {"$oid": "6219bb2efd2cdc8aa3ac8792"}}], properties:[], content:{"clusterTime":7068886947917201409,"fullDocument":"{\"_id\": {\"$oid\": \"6219bb2efd2cdc8aa3ac8792\"}, \"text\": \"123\", \"title\": \"456\"}","ns":{"databaseName":"test","collectionName":"mytable","fullName":"test.mytable"},"operation":"update"}
    ```
    
   delete
   
    ```bash
    ----- got message -----
    key:[{"_id": {"$oid": "620fc75879d0dfe57de5e4c9"}}], properties:[], content:{"clusterTime":7068895902924013569,"ns":{"databaseName":"test","collectionName":"mytable","fullName":"test.mytable"},"operation":"delete"}
    ``` 
    
   drop
    
    ```bash
    ----- got message -----
    key:[], properties:[], content:{"clusterTime":7068897174234333185,"ns":{"databaseName":"test","collectionName":"mytable","fullName":"test.mytable"},"operation":"drop"}
    ```
    
   dropDatabase
   
    ```bash
    ----- got message -----
    key:[], properties:[], content:{"clusterTime":7068899124149485570,"operation":"dropDatabase"}
    ```  
       
   rename

    ```bash
    ----- got message -----
    key:[], properties:[], content:{"clusterTime":7068888300831899649,"destNamespace":{"databaseName":"test","collectionName":"orders2022","fullName":"test.orders2022"},"ns":{"databaseName":"test","collectionName":"mytable","fullName":"test.mytable"},"operation":"rename"}

    ```  
   
all done , enjoy !

## How to run sink

TODO

## Maintainers

lzqdename 837500869@qq.com

## Support / Feedback / Bugs / Feature Requests

if you have questions , please post new issues in this project , and we will answer it as soon as possible .
