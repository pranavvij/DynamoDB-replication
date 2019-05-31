package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

public class Message implements Serializable, Comparable<Message> {
    String key, value, port;
    long timestamp;

    Message() {
        this.key = "";
        this.value = "";
        this.port = "";
    }

    @Override
    public int compareTo(Message message) {
        return new Long(this.timestamp).compareTo(new Long(message.timestamp));
    }
}
