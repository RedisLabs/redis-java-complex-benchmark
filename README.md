redis-java-complex-benchmark
============================

redis java benchmark is a tool used to load redis with the complex command zunionstore. 
its a java maven project so it can easily be used in any IDE that support maven such as eclipse and InteliJ. 
 
 
this benchmark load redis with zunionstore command, it is currently set to union 4 zsets each with 20k items. 
the  load and the number of zset to union is configurable with parameters. 
   
Getting started. 

1. package this project with maven 
2. initialize redis with the zset 
    for example java -jar saasbenchmark-1.0-SNAPSHOT-jar-with-dependencies.jar init <redis-server>  <redis-port> 120 1 300
3. run the benchamrk 

    java -jar saasbenchmark-1.0-SNAPSHOT-jar-with-dependencies.jar 
        run 
        <redis-server>  
        <redis-port> 
        120 - time to run the becnhmark in seconds   
        1   - number of threads for this load 
        300 - number of items to be loaded 