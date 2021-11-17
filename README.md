# kafka-demo
A usage for c++ kafka client.
# how to use

`clang -v` or `gcc -v`, c++ environment.

install kafka and zk, reference to https://kafka.apachecn.org/quickstart.html.

`yum install librdkafka-devel`, install c++ kafka client.

edit kafka configuration file - `config/server.properties` - add
```properties
listeners=PLAINTEXT://:9092
```
compile source file
 ```c++
 
 clang++ consumer.cpp -o consumer.out -std=c++11 -lrdkafka++
 clang++ producer.cpp -o producer.out -std=c++11 -lrdkafka++
 ```
start zk and kafka.
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```
run producer.out
```bash
./producer.out 127.0.0.1:9092 <topic-name>
./consumer.out
```
 
