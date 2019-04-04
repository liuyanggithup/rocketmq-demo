package com.seventeen.rocketmq.springbootstartdemo;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.Random;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringbootStartDemoApplicationTests {

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    @Test
    public void syncSend() {

        //KEYS:幂等判断的key,可以用流水单号等，保证唯一性
        Message msg =
                MessageBuilder.withPayload("dddd")
                        .setHeader(RocketMQHeaders.KEYS, System.currentTimeMillis())
                        .build();


        SendResult sendResult = rocketMQTemplate.syncSend("test-topic2:xyz", msg);
        System.out.println("syncSend sendResult:" + JSON.toJSONString(sendResult));

    }


    @Test
    public void asyncSend() throws Exception{


        //KEYS:幂等判断的key,可以用流水单号等，保证唯一性
        Message msg =
                MessageBuilder.withPayload("Hello RocketMQ ")
                        .setHeader(RocketMQHeaders.KEYS, "123456" )
                        .build();

        rocketMQTemplate.asyncSend(
                "test-topic1:withPayload",
                msg,
                new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.println("syncSend sendResult:" + JSON.toJSONString(sendResult));
                    }

                    @Override
                    public void onException(Throwable e) {
                        System.out.println("syncSend error:" + e.getMessage());
                    }
                });

        Thread.sleep(10000);

    }

    @Test
    public void withPayload() {

        Person person = new Person();
        person.setAge(12);
        person.setName("孙悟饭");

        SendResult sendResult =
                rocketMQTemplate.syncSend("test-topic:withPayload", JSON.toJSONString(person));
        System.out.printf("syncSend2 to topic %s sendResult=%s %n", "test-topic:withPayload", sendResult);

        sendResult =
                rocketMQTemplate.syncSend(
                        "test-topic:withPayload",
                        MessageBuilder.withPayload("Hello, World! I'm from spring message").build());
        System.out.printf("syncSend2 to topic %s sendResult=%s %n", "test-topic:withPayload", sendResult);
    }

    /**
     * rocketMQ 4.3后支持事务消息
     * @throws MessagingException
     */
    @Test
    public void testTransaction() throws MessagingException {
        Random rand = new Random(25);
        Message msg =
                MessageBuilder.withPayload("Hello RocketMQ ")
                        .setHeader(RocketMQHeaders.KEYS, "KEY_" + rand.nextInt())
                        .build();
        SendResult sendResult =
                rocketMQTemplate.sendMessageInTransaction(
                        "myTxProducerGroup", "test-topic:xyz", msg, null);
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