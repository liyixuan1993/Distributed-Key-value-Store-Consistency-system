package com.company;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/*
Map<String, BasicSystem.Message> messageMap = new HashMap<>();
Map<Integer, BasicSystem.Message> seqMap = new HashMap<>();
messageMap contains <messagekey,message>,messagekey is a string that can identify which message 
we are sending.
seqMap contains seqenuceNumber and Message,seqenuceNumber is from 0 to n. It is in fact a buffer,
which contains the messages that has been received but not delivered.

frist we initialize the system and let the first system be the sequencer.
when multicast the message by total ordering,the private data is MessageLabel,which contains
sender id,uuid,and messageId,messageId is used for order in seqMap,everytime sequencer put a new 
message into messageMap,it increments sequencerCount and send the order(a message which type is 
order)to others(including itself),when other process receive that msg,so we get the sequenceNumber,
and put it into seqMap.
In sum, in onDelivery,we have two type of message. first is total_message,which is real message.
in this case,the sequencer will check whether this message is new, if new,it will increment 
sequencerCount, and sent that message with sequencerCount with type total_order to everyone.
when we receive type total_order,we will add the message into seqMap with key sequencerCount,
so when we deliver the seqMap,we just start from 0 to n one by one.

then we go to seqMap to deliver the msg. we initialize seqenuceNumber to be 0 at first,and get that
msg in seqMap,if we get it,we should further increment seqenuceNumber and check if it exist the msg
in seqMap. In sum,if we have the message with that seqenuceNumber,we further check next seqenuceNumber,
if don't,we just end the loop and wait for another message.

*/
public final class TotalOrderingMulticaster extends Multicaster {
    private boolean isSequencer = false;
    // for sequencer use only
    private int sequencerCount = 0;

    private int seqenuceNumber = 0;

    private String sequencerId = new String();

    private static class MessageLabel implements Serializable {
        String sender;
        String uuid;
        int messageId;
        public MessageLabel(String sender, String uuid, int messageId) {
            this.sender = sender;
            this.uuid = uuid;
            this.messageId = messageId;
        }
    }

    public TotalOrderingMulticaster(BasicSystem basicSystem, Console console) {
        super(basicSystem, console);
        int count = 0;
        for(String k : nodeSet) {
            if(basicSystem.existNode(k))
                count++;
        }
        if(count <= 1)
            isSequencer = true;
    }

    private Map<String, BasicSystem.Message> messageMap = new HashMap<>();
    private Map<Integer, BasicSystem.Message> seqMap = new HashMap<>();

    private boolean tryDeliverMessage(int messageId) {
        BasicSystem.Message message = seqMap.get(messageId);
        if(message != null && message.data != null) {
            MessageLabel messageLabel = (MessageLabel) message.privateData;
            String messagekey = messageLabel.sender + ":" + messageLabel.uuid;

            console.onDelivery(messageLabel.sender, message.data);
            messageMap.remove(messagekey);
            seqMap.remove(messageId);
            return true;
        }
        return false;
    }

    private void cleanMessageMap() {
        while(tryDeliverMessage(seqenuceNumber)) {
            seqenuceNumber++;
        }
    }

    @Override
    public synchronized boolean onMessage(BasicSystem.Message message) {
        if(!super.onMessage(message) && message.type.startsWith("total_")) {
            MessageLabel messageLabel = (MessageLabel) message.privateData;
            String messagekey = messageLabel.sender + ":" + messageLabel.uuid;
            if(message.type.endsWith("message")) {
                if(isSequencer && !messageMap.containsKey(messagekey)) {
                    BasicSystem.Message orderMessage = new BasicSystem.Message("total_order",
                            new MessageLabel(message.id, messageLabel.uuid, sequencerCount), null);
                    seqMap.put(sequencerCount++, null);
                    basicMulticast(orderMessage);
                }
                if(messageMap.get(messagekey) == null)
                    messageMap.put(messagekey, message);
                else
                    messageMap.get(messagekey).data = message.data;
                cleanMessageMap();
            } else if(message.type.endsWith("order")) {
                sequencerId = message.id;
                if(messageMap.get(messagekey) != null)
                    messageMap.get(messagekey).privateData = messageLabel;
                else
                    messageMap.put(messagekey, message);
                seqMap.put(messageLabel.messageId, messageMap.get(messagekey));
                cleanMessageMap();
            }
        }
        return false;
    }

    @Override
    public void multicast(String data) {
        BasicSystem.Message message = new BasicSystem.Message("total_message",
                new MessageLabel(id, UUID.randomUUID().toString(), -1), data);
        basicMulticast(message);
    }
}
