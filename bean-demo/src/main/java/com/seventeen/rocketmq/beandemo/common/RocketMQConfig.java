package com.seventeen.rocketmq.beandemo.common;


import lombok.Data;

@Data
public class RocketMQConfig {

    private String namesrvAddr;

    private String groupName;

}
