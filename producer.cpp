#include <iostream>
#include <string>
#include <cstdio>
#include "librdkafka/rdkafkacpp.h"

static volatile sig_atomic_t run = 1;

class MyDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
    void dr_cb(RdKafka::Message& message) {
        if (message.err()) {
            std::cerr << "Message delivery failed: " << message.errstr()
                      << std::endl;
        } else {
            std::cerr << "Message delivered to topic " << message.topic_name()
                      << " [" << message.partition() << "] at offset "
                      << message.offset() << std::endl;
        }
    }
};

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <brokers> <topic>\n";
        exit(1);
    }

    std::string brokers = argv[1];
    std::string topic   = argv[2];

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    std::string errstr;

    if (conf->set("bootstrap.servers", brokers, errstr) !=
        RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        exit(1);
    }

    MyDeliveryReportCb my_dr_cb;
    if (conf->set("dr_cb", &my_dr_cb, errstr) !=
        RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        exit(1);
    }

    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        exit(1);
    }

    delete conf;

    std::cout << "% Type message value and hit enter "
              << "to produce message." << std::endl;

    for (std::string line; run && std::getline(std::cin, line);) {
        if (line.empty()) {
            producer->poll(0);
            continue;
        }

        retry:
            RdKafka::ErrorCode err = producer->produce(topic, RdKafka::Topic::PARTITION_UA,
                                                       RdKafka::Producer::RK_MSG_COPY,
                                                       const_cast<char *>(line.c_str()), line.size(),
                                                       nullptr, 0, 0, nullptr, nullptr);
            if (err != RdKafka::ERR_NO_ERROR) {
                std::cerr << "% Failed to produce to topic[" << topic << "]: "
                          << RdKafka::err2str(err) << std::endl;
                
                if (err == RdKafka::ERR__QUEUE_FULL) {
                    producer->poll(1000);
                    goto retry;
                }
            } else {
                std::cerr << "% Enqueued msg (" << line.size() << " bytes) "
                          << "for topic " << topic << std::endl;
            }

        producer->poll(0);
    }

    std::cerr << "% Flushing final messages..." << std::endl;
    producer->flush(10 * 1000);

    if (producer->outq_len() > 0) {
        std::cerr << "% " << producer->outq_len()
                    << " message(s) were not delivered" << std::endl;
    }

    delete producer;

    return 0;
}