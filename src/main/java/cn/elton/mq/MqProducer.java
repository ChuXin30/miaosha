package cn.elton.mq;

import cn.elton.dao.StockLogDOMapper;
import cn.elton.dataobject.StockLogDO;
import cn.elton.error.BuinessException;
import cn.elton.service.OrderService;
import com.alibaba.fastjson.JSON;
import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import sun.dc.pr.PRError;

import javax.annotation.PostConstruct;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

@Component
public class MqProducer {
    private DefaultMQProducer producer;
    private TransactionMQProducer transactionMQProducer;

    @Value("${mq.nameserver.addr}")
    private String nameAddr;

    @Value("${mq.topicname}")
    private String topicName;

    @Autowired
    private OrderService orderService;

    @Autowired
    private StockLogDOMapper stockLogDOMapper;

    @PostConstruct
    public void init() throws MQClientException {
        producer = new DefaultMQProducer("produce_group");
        producer.setNamesrvAddr(nameAddr);
        producer.start();

        transactionMQProducer = new TransactionMQProducer("transaction_producer_group");
        transactionMQProducer.setNamesrvAddr(nameAddr);
        transactionMQProducer.start();

        transactionMQProducer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object arg) {

                Integer itemId = (Integer) ((Map)arg).get("itemId");
                Integer amount = (Integer)((Map)arg).get("amount");
                Integer userId = (Integer)((Map)arg).get("userId");
                Integer promoId = (Integer)((Map)arg).get("promoId");
                String  stockLogId = (String) ((Map)arg).get("stockLogId");

                try {
                    orderService.createOrder(userId,itemId,amount,promoId,stockLogId);
                } catch (BuinessException e) {
                    e.printStackTrace();
                    StockLogDO stockLogDO = stockLogDOMapper.selectByPrimaryKey(stockLogId);
                    stockLogDO.setStatus(3);
                    stockLogDOMapper.updateByPrimaryKeySelective(stockLogDO);

                    return  LocalTransactionState.ROLLBACK_MESSAGE;
                }
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                String jsonString = new String(msg.getBody());
                Map<String , Object> map = JSON.parseObject(jsonString, Map.class);
                Integer itemId = (Integer) map.get("itemId");
                Integer amount = (Integer)map.get("amount");
                String  stockLogId = (String) map.get("stockLogId");
                StockLogDO stockLogDO = stockLogDOMapper.selectByPrimaryKey(stockLogId);
                if(stockLogDO == null ){
                    return LocalTransactionState.UNKNOW;
                }
                if(stockLogDO.getStatus().intValue() == 2){
                    return  LocalTransactionState.COMMIT_MESSAGE;
                }else if( stockLogDO.getStatus().intValue() == 1 ){
                    return LocalTransactionState.UNKNOW;
                }
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }

        });
    }

    //事务型同步库存扣减消息
    public boolean transactionAsyncReduceStock(Integer userId , Integer promoId, Integer itemId, Integer amount , String stockLogId){
        Map<String , Object> bodyMap = new HashMap<>();
        bodyMap.put("itemId",itemId);
        bodyMap.put("amount",amount);
        bodyMap.put("stockLogId",stockLogId);


        Map<String , Object> argsMap = new HashMap<>();
        argsMap.put("itemId",itemId);
        argsMap.put("amount",amount);
        argsMap.put("userId",userId);
        argsMap.put("promoId",promoId);
        argsMap.put("stockLogId",stockLogId);

        Message message = new Message(topicName , "increase",
                JSON.toJSON(bodyMap).toString().getBytes(Charset.forName("UTF-8")) );
        TransactionSendResult sendResult=  null;
        try {
            sendResult =  transactionMQProducer.sendMessageInTransaction(message,argsMap);
        } catch (MQClientException e) {
            return  false;
        }
        if(sendResult.getLocalTransactionState() == LocalTransactionState.ROLLBACK_MESSAGE) {
            return false;
        }else if(sendResult.getLocalTransactionState() == LocalTransactionState.COMMIT_MESSAGE){
            return true;
        }else {
            return  false;
        }
    }

    public boolean asyncReduceStock(Integer itemId, Integer amount)  {
        Map<String , Object> bodyMap = new HashMap<>();
        bodyMap.put("itemId",itemId);
        bodyMap.put("amount",amount);

        Message message = new Message(topicName , "increase",
                JSON.toJSON(bodyMap).toString().getBytes(Charset.forName("UTF-8")) );
        try {
            producer.send(message);
        } catch (MQClientException e) {
            return  false;
        } catch (RemotingException e) {
            return  false;
        } catch (MQBrokerException e) {
            return  false;
        } catch (InterruptedException e) {
            return  false;
        }
        return true;
    }
}
