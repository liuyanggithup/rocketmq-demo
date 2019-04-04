package com.seventeen.rocketmq.beandemo.producer;

import com.seventeen.rocketmq.beandemo.common.RocketMQConfig;
import com.seventeen.rocketmq.beandemo.common.SimpleMQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * 该方式优点是可以配置多个nameserv集群 多个Producer
 */
@Component
public class DemoProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleMQProducer.class);
    //rocketmq.name-server = 127.0.0.1:9876
    //rocketmq.producer.group = test-producerGroup

    @Value("${pay-rocketmq.name-server}")
    private String namesrv1;

    @Value("${pay-rocketmq.producer.group}")
    private String groupname1;

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public SimpleMQProducer simpleMQProducer() {

        RocketMQConfig rocketMQConfig = new RocketMQConfig();
        rocketMQConfig.setGroupName(groupname1 + "11111");
        rocketMQConfig.setNamesrvAddr(namesrv1);
        SimpleMQProducer simpleMQProducer = new SimpleMQProducer();
        simpleMQProducer.setRocketMQConfig(rocketMQConfig);
        return simpleMQProducer;
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public SimpleMQProducer simpleMQProducer2() {

        RocketMQConfig rocketMQConfig = new RocketMQConfig();
        rocketMQConfig.setGroupName(groupname1 + "22222");
        rocketMQConfig.setNamesrvAddr(namesrv1);
        SimpleMQProducer simpleMQProducer = new SimpleMQProducer();
        simpleMQProducer.setRocketMQConfig(rocketMQConfig);
        return simpleMQProducer;
    }

}
