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

import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.graph.Vocabulary;
import spark.Request;
import spark.Response;

/**
 *
 * @author alexmy
 */
public class Controller {

    static String command;

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

    public static String process(Request req, Response res, String command) {

        String requestJson = req.body();
        command = command.replaceAll("-", "_").toUpperCase();

        String result = null;

        res.type("application/json;charset=utf8");
        res.header("content-type", "application/json;charset=utf8");
        res.raw();

        if (!ServiceUtils.validateInput(requestJson)) {
            res.status(Status.ERROR.getStatusCode());
        } else {
            res.status(Status.SUCCESS.getStatusCode());
            
            Gnode gnode = new Gnode().fromJson(requestJson);            
            switch(gnode.getLabel()){
                case Vocabulary.SOURCE:
                    
                    break;
                case Vocabulary.PROCESSOR:
                    
                    break;
                case Vocabulary.SINK:
                    
                    break;
                default:
                    res.status(Status.ERROR.getStatusCode());
                    break;
            }            

//            Processor proc_mp = new ProcessorMap().newInstance();
//            proc_mp.setCommand(command);
//            proc_mp.setInput(requestJson);
//            outputmap = (OutputMap) proc_mp.process();
//            result = outputmap == null ? "[\"No data\"]" : outputmap.toJsonString();
        }
        return result;
    }
}
