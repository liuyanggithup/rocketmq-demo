package com.seventeen.rocketmq.beandemo.consumer;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class SimpleRocketMQListener implements MessageListenerConcurrently {


    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                    ConsumeConcurrentlyContext context) {


        for (MessageExt messageExt:msgs){
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), messageExt);
        }


        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }


}