package com.seventeen.rocketmq.springbootstartdemo.controller;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.Random;

/**
 * @auther: xia_xun
 * @Date: 2019/3/6
 * @description:
 */
@RestController
public class TestController {
    
    
    public static final Logger logger = LoggerFactory.getLogger(TestController.class);

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    @RequestMapping("/test")
    public String test(){

        syncSend();
        asyncSend();
        withPayload();
        testTransaction();

        return "success";
    }

    public void syncSend() {
        SendResult sendResult = rocketMQTemplate.syncSend("test-test-topic:xyz", "Hello, World!");
        logger.info("syncSend sendResult:" + JSON.toJSONString(sendResult));
    }


    public void asyncSend() {
        rocketMQTemplate.asyncSend(
                "test-test-topic:xyz",
                "异步消息测试message!",
                new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        logger.info("syncSend sendResult:" + JSON.toJSONString(sendResult));
                    }

                    @Override
                    public void onException(Throwable e) {
                        logger.info("syncSend error:" + e.getMessage());
                    }
                });
    }


    public void withPayload() {

        Person person = new Person();
        person.setAge(12);
        person.setName("孙悟饭");

        SendResult sendResult =
                rocketMQTemplate.syncSend("test-test-topic:withPayload", JSON.toJSONString(person));
        System.out.printf("syncSend2 to topic %s sendResult=%s %n", "test-test-topic:withPayload", sendResult);

        sendResult =
                rocketMQTemplate.syncSend(
                        "test-test-topic:withPayload",
                        MessageBuilder.withPayload("Hello, World! I'm from spring message").build());
        System.out.printf("syncSend2 to topic %s sendResult=%s %n", "test-test-topic:withPayload", sendResult);
    }

    /**
     * rocketMQ 4.3后支持事务消息
     * @throws MessagingException
     */
    public void testTransaction() throws MessagingException {
        Random rand = new Random(25);
        Message msg =
                MessageBuilder.withPayload("Hello RocketMQ ")
                        .setHeader(RocketMQHeaders.KEYS, "KEY_" + rand.nextInt())
                        .build();
        SendResult sendResult =
                rocketMQTemplate.sendMessageInTransaction(
                        "myTxProducerGroup", "test-test-topic:xyz", msg, null);
        System.out.printf(
                "------ send Transactional msg body = %s , sendResult=%s %n",
                msg.getPayload(), sendResult.getSendStatus());
    }
}

@Data
class Person {
    String name;
    int age;
}