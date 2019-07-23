/* 
 * Copyright (C) 2019 Lisa Park, Inc. (www.lisa-park.net)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.lisapark.koctopus.compute;

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.lisapark.koctopus.core.Output;
import org.lisapark.koctopus.core.ProcessingException;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.graph.NodeVocabulary;
import org.lisapark.koctopus.core.graph.Vocabulary;
import org.lisapark.koctopus.core.parameter.Parameter;
import org.lisapark.koctopus.core.runtime.redis.RedisRuntime;
import org.lisapark.koctopus.core.sink.external.ExternalSink;
import org.lisapark.koctopus.core.source.external.ExternalSource;
import org.openide.util.Exceptions;
import spark.Request;
import spark.Response;

/**
 *
 * @author alexmy
 */
public class Controller {

    private static final String DEFAULT_TRANSPORT_URL = "redis://localhost";

    enum Status {
        SUCCESS(200),
        ERROR(400);
        private final int statusCode;

        Status(int statusCode) {
            this.statusCode = statusCode;
        }

        public int getStatusCode() {
            return this.statusCode;
        }
    }

    public static String process(Request req, Response res) throws ValidationException, ProcessingException {
        String requestJson = req.body();
        String result = null;

        res.type("application/json;charset=utf8");
        res.header("content-type", "application/json;charset=utf8");
        res.raw();

        if (!ServiceUtils.validateInput(requestJson)) {
            res.status(Status.ERROR.getStatusCode());
        } else {
            try {
                res.status(Status.SUCCESS.getStatusCode());
                Gnode gnode = (Gnode) new Gnode().fromJson(requestJson);
                String transportUrl = gnode.getTransportUrl() == null ? DEFAULT_TRANSPORT_URL : gnode.getTransportUrl();
                RedisRuntime runtime = new RedisRuntime(transportUrl, System.out, System.err);
                
                String type;
                switch (gnode.getLabel()) {
                    case Vocabulary.SOURCE:
                        type = gnode.getType();                         
                        ExternalSource sourceIns = (ExternalSource) Class.forName(type).newInstance();
                        ExternalSource source = (ExternalSource) sourceIns.newInstance(gnode);
                        result = new Gson().toJson(sourceResponse(source, transportUrl));
                        source.compile().startProcessingEvents(runtime);

                        break;
                    case Vocabulary.PROCESSOR:

                        break;
                    case Vocabulary.SINK:
                        type = gnode.getType();
                        ExternalSink sinkIns = (ExternalSink) Class.forName(type).newInstance();
                        ExternalSink sink = (ExternalSink) sinkIns.newInstance(gnode);
                        result = new Gson().toJson(sinkResponse(sink, transportUrl));
                        sink.compile().processEvent(runtime, null);

                        break;
                    default:
                        res.status(Status.ERROR.getStatusCode());
                        break;
                }
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
                Exceptions.printStackTrace(ex);
            }

        }
        return result;
    }
    
    private static Map<String, String> sourceResponse(ExternalSource source, String transportUrl){
        Map<String, String> map = new HashMap<>();
        map.put("transportUrl", transportUrl);
        map.put("className", source.getClass().getCanonicalName());
        map.put("Id", source.getId().toString());
        
        return map;
    }
    
    private static Map<String, String> sinkResponse(ExternalSink sink, String transportUrl){
        Map<String, String> map = new HashMap<>();
        map.put("transportUrl", transportUrl);
        map.put("className", sink.getClass().getCanonicalName());
        map.put("Id", sink.getId().toString());
        
        return map;
    }
}
