package org.lisapark.koctopus.compute;

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


import com.google.gson.Gson;
import org.lisapark.koctopus.compute.processor.sma.SmaRedis;
import org.lisapark.koctopus.compute.sink.ConsoleSinkRedis;
import org.lisapark.koctopus.compute.source.TestSourceRedis;
import org.lisapark.koctopus.core.ProcessingModel;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.graph.Graph;
import org.lisapark.koctopus.core.graph.GraphUtils;
import org.lisapark.koctopus.core.graph.api.INode;

/**
 *
 * @author alexmy
 */
public class Main {

    public static void main(String[] args) {
        
        ProcessingModel model = createProcessingModel();
        Graph graph = GraphUtils.compileGraph(model);       
        System.out.println(new Gson().toJson(graph.getParams()));
        
        System.out.println(new Gson().toJson((Gnode)graph, Gnode.class));
        
        System.out.println(graph.toJson());
        Graph graphCopy = new Graph().fromJson(graph.toJson().toString());
        System.out.println(graphCopy.toJson());
    }

    private static ProcessingModel createProcessingModel() {

        ProcessingModel model = new ProcessingModel("test");

        model.setName("Test");
        model.setTransportUrl("redis://localhost");

        TestSourceRedis source = TestSourceRedis.newTemplate();
        model.addExternalEventSource(source);

        SmaRedis sma = SmaRedis.newTemplate();
        sma.getInput().connectSource(source);
        model.addProcessor(sma);

        ConsoleSinkRedis sink = ConsoleSinkRedis.newTemplate();
        sink.getInput().connectSource(sma);
        model.addExternalSink(sink);

        return model;
    }    
}
