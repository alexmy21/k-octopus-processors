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
package org.lisapark.koctopus.processors;

import java.util.logging.Logger;
import spark.Request;
import spark.Response;

/**
 *
 * @author alexmy
 */

public class LivenessCheck {

    static final Logger LOG = Logger.getLogger(LivenessCheck.class.getName());

    /**
     *
     * @param req
     * @param res
     * @return
     */
    public int check(Request req, Response res) {
        String requestJson = req.body();

        LOG.info(requestJson);

        res.type("application/json;charset=utf8");
        res.header("content-type", "application/json;charset=utf8");
        res.raw();

        return Status.SUCCESS.getStatusCode();
    }
}
