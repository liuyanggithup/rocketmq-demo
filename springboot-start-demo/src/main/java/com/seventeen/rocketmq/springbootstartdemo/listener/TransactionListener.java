package com.seventeen.rocketmq.springbootstartdemo.listener;

import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** 事务消息监听，保障本地事务和消息同时成功或失败 */
@RocketMQTransactionListener(txProducerGroup = "myTxProducerGroup")
public class TransactionListener implements RocketMQLocalTransactionListener {
  public static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
  private AtomicInteger transactionIndex = new AtomicInteger(0);

  private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<String, Integer>();

  /**
   * 执行本地事务
   *
   * @param msg
   * @param arg
   * @return
   */
  @Override
  public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {

    /**
     * 注意考虑幂等性
     *
     * 多次调用本地事务处理结果一致，返回正确的状态值
     *
     * 不需要考虑localTrans 和 transactionIndex的逻辑，只是为了demo模拟
     *
     * status代表本地事务提交结果，本地事务提交成功返回COMMIT
     *
     *
     */

    String transId = (String) msg.getHeaders().get(RocketMQHeaders.TRANSACTION_ID);
    logger.info("#### executeLocalTransaction is executed, msgTransactionId={}", transId);
    int value = transactionIndex.getAndIncrement();
    int status = value % 3;
    localTrans.put(transId, status);



    if (status == 0) {
      // Return local transaction with success(commit), in this case,
      // this message will not be checked in checkLocalTransaction()
      logger.info(
          "    # COMMIT # Simulating msg %s related local transaction exec succeeded! ###",
          msg.getPayload());
      return RocketMQLocalTransactionState.COMMIT;
    }

    if (status == 1) {
      // Return local transaction with failure(rollback) , in this case,
      // this message will not be checked in checkLocalTransaction()
      logger.info(
          "    # ROLLBACK # Simulating %s related local transaction exec failed!",
          msg.getPayload());
      return RocketMQLocalTransactionState.ROLLBACK;
    }

    logger.info("    # UNKNOW # Simulating %s related local transaction exec UNKNOWN! \n");
    return RocketMQLocalTransactionState.UNKNOWN;
  }

  /**
   * 检查本地事务
   *
   * @param msg
   * @return
   */
  @Override
  public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
    String transId = (String) msg.getHeaders().get(RocketMQHeaders.TRANSACTION_ID);
    RocketMQLocalTransactionState retState = RocketMQLocalTransactionState.COMMIT;
    Integer status = localTrans.get(transId);

    /**
     *
     * status是demo判断本地事务是否执行成功的标记，不需要考虑localTrans 和 transactionIndex的逻辑，只是为了demo模拟
     *
     * 业务代码要做可幂等回查
     *
     */

    if (null != status) {
      switch (status) {
        case 0:
          retState = RocketMQLocalTransactionState.UNKNOWN;
          break;
        case 1:
          retState = RocketMQLocalTransactionState.COMMIT;
          break;
        case 2:
          retState = RocketMQLocalTransactionState.ROLLBACK;
          break;
      }
    }
    logger.info(
        "------ !!! checkLocalTransaction is executed once,"
            + " msgTransactionId={} TransactionState={} status={}",
        transId, retState, status);
    return retState;
  }
}
