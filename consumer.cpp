#include "librdkafka/rdkafkacpp.h"
#include <iostream>
#include <list>
#include <string>
using namespace std;

void msg_consume(RdKafka::Message* msg) {
    std::cout << "msg::topic_name: " << msg->topic_name().c_str() << endl;
    if (msg->err() == RdKafka::ERR_NO_ERROR) {
        std::cout << "Read msg at offset " << msg->offset() << std::endl;
        if (msg->key()) {
                std::cout << "Key: " << *msg->key() << std::endl;
        }
        printf("%.*s\n", static_cast<int>(msg->len()), static_cast<const char*>(msg->payload()));
    } else if (msg->err() == RdKafka::ERR__TIMED_OUT) {
        printf("error[%s]\n", "ERROR__TIMED_OUT");
    } else {
        printf("error[%s]\n", "other");
    }
}

int main(int argc, char** argv) {
    std::string err_string;
    int32_t partition = RdKafka::Topic::PARTITION_UA;
    partition = 0;

    std::string broker_list = "localhost:9092";
    RdKafka::Conf* global_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf* topic_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    int64_t start_offset = RdKafka::Topic::OFFSET_BEGINNING;
    global_conf->set("metadata.broker.list", broker_list, err_string);

    RdKafka::Consumer* consumer = RdKafka::Consumer::create(global_conf, err_string);
    if (!consumer) {
        printf("failed to create consumer, %s\n", err_string.c_str());
        return -1;
    }
    printf("create consumer %s \n", consumer->name().c_str());

    std::string topic_name = "kafka-test-wayne2";
    RdKafka::Topic* topic = RdKafka::Topic::create(consumer, topic_name, topic_conf, err_string);
    if (!topic) {
        printf("try create topic[%s] failed, %s\n", topic_name.c_str(), err_string.c_str());
        return -1;
    }
    printf("create topic[%s] successd. \n", topic_name.c_str());

    RdKafka::ErrorCode resp = consumer->start(topic, partition, start_offset);
    if (resp != RdKafka::ERR_NO_ERROR) {
        printf("Failed to start consumer: %s\n", RdKafka::err2str(resp).c_str());
        return -1;
    }

    while (true) {
        RdKafka::Message* msg = consumer->consume(topic, partition, 2000);
        printf("topic[%s], partition[%d], start consume\n", topic_name.c_str(), partition);
        msg_consume(msg);
        delete msg;
    }

    consumer->stop(topic, partition);
    consumer->poll(1000);

    delete topic;
    delete consumer;

    return 0;
}