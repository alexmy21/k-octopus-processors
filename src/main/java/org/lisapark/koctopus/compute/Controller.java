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

import java.util.HashMap;
import java.util.Map;
import org.lisapark.koctopus.core.ProcessingException;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.processor.AbstractProcessor;
import org.lisapark.koctopus.core.runtime.BaseController;
import org.lisapark.koctopus.core.runtime.redis.RedisRuntime;
import org.lisapark.koctopus.core.sink.external.ExternalSink;
import org.lisapark.koctopus.core.source.external.ExternalSource;
import spark.Request;
import spark.Response;

/**
 *
 * @author alexmy
 */
public class Controller extends BaseController {

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

    /**
     *
     * @param req
     * @param res
     * @return
     * @throws ValidationException
     * @throws ProcessingException
     */
    public static String process(Request req, Response res) throws ValidationException, ProcessingException {
        String requestJson = req.body();
        String result = null;
        res.type("application/json;charset=utf8");
        res.header("content-type", "application/json;charset=utf8");
        res.raw();
        if (!ServiceUtils.validateInput(requestJson)) {
            res.status(Status.ERROR.getStatusCode());
        } else {            
            Gnode gnode = (Gnode) new Gnode().fromJson(requestJson);
            String trnsUrl = gnode.getTransportUrl() == null ? DEFAULT_TRANSPORT_URL : gnode.getTransportUrl();
            RedisRuntime runtime = new RedisRuntime(trnsUrl, System.out, System.err);
            result = process(requestJson, runtime);
            if(result == null){
                res.status(Status.ERROR.getStatusCode());
            } else {
                res.status(Status.SUCCESS.getStatusCode());
            }
        }
        return result;
    }
}
