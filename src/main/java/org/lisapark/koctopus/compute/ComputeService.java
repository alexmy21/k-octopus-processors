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

import java.util.logging.Level;
import java.util.logging.Logger;
import static spark.Spark.*;

/**
 *
 * @author alexmy
 */
public class ComputeService {
    
    static final Logger LOG = Logger.getLogger(ComputeService.class.getName());

    public static void main(String[] args) {
        
        // Set Server port
        int _port = 4567;
        String endPoint = "/k-octopus/";
        if (args.length > 0) {
            _port = Integer.valueOf(args[0]);
        }
        port(_port);
          
        get(endPoint + "health", (req, res) -> {
            LOG.log(Level.INFO, "{0}:{1}", new Object[]{endPoint, "health"});
            return new LivenessCheck().check(req, res);
        });
        
        // Map requests
        post(endPoint + "compute", (req, res) -> {
            LOG.log(Level.INFO, "{0}:{1}", new Object[]{endPoint, "compute"});
            return new HttpEndPoint().startProcessing(req, res);
        });
      
    }
}
