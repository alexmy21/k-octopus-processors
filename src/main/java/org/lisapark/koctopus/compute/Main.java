/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lisapark.koctopus.compute;

import org.lisapark.koctopus.compute.sink.ConsoleSinkRedis;
import org.lisapark.koctopus.compute.source.TestSourceRedis;
import org.lisapark.koctopus.core.ProcessingException;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.runtime.redis.RedisRuntime;
import org.openide.util.Exceptions;

/**
 *
 * @author alexmy
 */
public class Main {

    public static void main(String[] args) {
//        RedisClient client = RedisClient.create("redis://localhost");

        RedisRuntime runtime = new RedisRuntime("redis://localhost", System.out, System.err);
        
        try {
            TestSourceRedis source = TestSourceRedis.newTemplate();
            source.compile().startProcessingEvents(runtime);
            
            ConsoleSinkRedis console = ConsoleSinkRedis.newTemplate();
            console.getInput().connectSource(source);
            console.compile().processEvent(runtime, null);
        } catch (ValidationException | ProcessingException ex) {
            Exceptions.printStackTrace(ex);
        }
    }
}
