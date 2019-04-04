package com.seventeen.rocketmq.beandemo;

import com.seventeen.rocketmq.beandemo.common.SimpleMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

@RunWith(SpringRunner.class)
@SpringBootTest
public class BeanDemoApplicationTests {

    @Resource
    private SimpleMQProducer simpleMQProducer;


    @Resource
    private SimpleMQProducer simpleMQProducer2;

    /**
     * 优点是更灵活
     *
     * @throws Exception
     */
    @Test
    public void simpleSend() throws Exception {

        Message msg = new Message("TEST" /* Topic */,
                "xyz" /* Tag */,System.currentTimeMillis()+"",
                ("Hello RocketMQ ").getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
        );
        /*
         * Call send message to deliver message to one of brokers.
         */
        SendResult sendResult = simpleMQProducer.send(msg);
        SendResult sendResult2 = simpleMQProducer2.send(msg);

        System.out.printf("%s%n", sendResult);
        System.out.printf("%s%n", sendResult2);
    }


}
