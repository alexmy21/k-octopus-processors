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

import org.lisapark.koctopus.repo.util.ServiceUtils;
import org.lisapark.koctopus.core.ProcessingException;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.runtime.BaseController;
import spark.Request;
import spark.Response;

/**
 *
 * @author alexmy
 */
public class HttpController extends BaseController {

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
    
    public HttpController(){
        super();
    }

    /**
     *
     * @param req
     * @param res
     * @return
     * @throws ValidationException
     * @throws ProcessingException
     * @throws java.lang.InterruptedException
     */
    public String startProcessing(Request req, Response res) throws ValidationException, ProcessingException, InterruptedException {
        String requestJson = req.body();
        String result = null;
        res.type("application/json;charset=utf8");
        res.header("content-type", "application/json;charset=utf8");
        res.raw();
        if (!ServiceUtils.validateInput(requestJson)) {
            res.status(Status.ERROR.getStatusCode());
        } else { 
            result = process(requestJson);
            if(result == null){
                res.status(Status.ERROR.getStatusCode());
            } else {
                res.status(Status.SUCCESS.getStatusCode());
            }
        }
        return result;
    }
}
