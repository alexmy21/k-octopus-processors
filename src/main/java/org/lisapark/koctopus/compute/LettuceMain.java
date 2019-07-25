/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lisapark.koctopus.compute;

import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import java.util.List;

/**
 *
 * @author alexmy
 */
public class LettuceMain {

    public static void main(String[] args) {
        RedisClient client = RedisClient.create("redis://localhost");
        StatefulRedisConnection<String, String> connection = client.connect();
        RedisStreamCommands<String, String> streamCommands = connection.sync();

        List<StreamMessage<String, String>> messages = streamCommands
                .xread(XReadArgs.Builder.count(5),
                        StreamOffset.from("org.lisapark.koctopus.compute.source.TestSourceRedis:535aaf83-da25-49cc-87a1-dd0a64c9c764", "0"));

        if (messages.size() > 0) { // a message was read
            System.out.println(messages);
        } else { // no message was read

        }
        
         List<StreamMessage<String, String>> messages_1 = streamCommands
                .xread(XReadArgs.Builder.count(5),
                        StreamOffset.from("org.lisapark.koctopus.compute.source.TestSourceRedis:535aaf83-da25-49cc-87a1-dd0a64c9c764", "5"));

        if (messages_1.size() > 0) { // a message was read
            System.out.println(messages_1);
        } else { // no message was read

        }
    }
}
