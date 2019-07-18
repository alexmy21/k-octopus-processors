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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.lisapark.koctopus.compute.processor.sma.Sma;
import org.lisapark.koctopus.compute.sink.ConsoleSinkRedis;
import org.lisapark.koctopus.compute.source.TestSourceRedis;
import org.lisapark.koctopus.core.AbstractComponent;
import org.lisapark.koctopus.core.Input;
import org.lisapark.koctopus.core.ProcessingModel;
import org.lisapark.koctopus.core.event.Attribute;
import org.lisapark.koctopus.core.graph.Edge;
import org.lisapark.koctopus.core.graph.Graph;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.graph.Vocabulary;
import org.lisapark.koctopus.core.parameter.Parameter;
import org.lisapark.koctopus.core.processor.Processor;
import org.lisapark.koctopus.core.sink.external.ExternalSink;
import org.lisapark.koctopus.core.source.Source;
import org.lisapark.koctopus.core.source.external.ExternalSource;

/**
 *
 * @author alexmy
 */
public class Main {

    public static void main(String[] args) {
        
        Graph graph = compile(createProcessingModel());

        System.out.println(graph.toJson());
        
        Graph graphCopy = new Graph().fromJson(graph.toJson().toString());
        
        System.out.println(graphCopy.toJson());

//        RedisRuntime runtime = new RedisRuntime("redis://localhost", System.out, System.err);
//        
//        try {
//            TestSourceRedis source = TestSourceRedis.newTemplate();
//            source.compile().startProcessingEvents(runtime);
//            
//            ConsoleSinkRedis console = ConsoleSinkRedis.newTemplate();
//            console.getInput().connectSource(source);
//            console.compile().processEvent(runtime, null);
//        } catch (ValidationException | ProcessingException ex) {
//            Exceptions.printStackTrace(ex);
//        }
    }

    private static ProcessingModel createProcessingModel() {

        ProcessingModel model = new ProcessingModel("test");

        model.setName("Test");
        model.setTransportUrl("redis://localhost");

        TestSourceRedis source = TestSourceRedis.newTemplate();
        model.addExternalEventSource(source);

        Sma sma = Sma.newTemplate();
        sma.getInput().connectSource(source);
        model.addProcessor(sma);

        ConsoleSinkRedis sink = ConsoleSinkRedis.newTemplate();
        sink.getInput().connectSource(sma);
        model.addExternalSink(sink);

        return model;
    }

    private static Graph compile(ProcessingModel model) {
        Graph graph = new Graph();

        graph.setId(model.getId().toString());
        graph.setLabel(model.getName());
        graph.setType(Vocabulary.PROCESSING_GRAPH);
        graph.setDirected(Boolean.TRUE);

        Map<String, Object> graphMetadata = new HashMap<>();
        graphMetadata.put(Vocabulary.TRANSPORT_URL, model.getTransportUrl());

        graph.setProperties(graphMetadata);

        List<Gnode> nodes = new ArrayList<>();
        List<Edge> edges = new ArrayList<>();

        Set<ExternalSource> sources = model.getExternalSources();
        sources.stream().forEach((ExternalSource source) -> {
            Gnode sourceNode = new Gnode();
            sourceNode.setId(source.getId().toString());
            sourceNode.setLabel(Vocabulary.SOURCE);
            sourceNode.setType(source.getClass().getCanonicalName());

            Map<String, Object> sourceProps = new HashMap<>();
            Set<Parameter> params = source.getParameters();
            params.stream().forEach((Parameter param) -> {                
                sourceProps.put(String.valueOf(param.getId()), param.getValue());
            });
            sourceNode.setProperties(sourceProps);

            Map<String, Object> sourcePropsOut = new HashMap<>();
            List<Attribute> attrs = source.getOutput().getAttributes();
            attrs.stream().forEach((Attribute attr) -> {
                sourcePropsOut.put(attr.getName(), attr.getType().getCanonicalName());
            });
            sourceNode.setPropertiesOut(sourcePropsOut);

            nodes.add(sourceNode);
        });

        Set<Processor> processors = model.getProcessors();
        processors.stream().forEach((Processor proc) -> {
            Gnode procNode = new Gnode();
            procNode.setId(proc.getId().toString());
            procNode.setLabel(Vocabulary.PROCESSOR);
            procNode.setType(proc.getClass().getCanonicalName());

            Map<String, Object> procMetadata = new HashMap<>();
            Set<Parameter> params = proc.getParameters();
            params.stream().forEach((Parameter param) -> {
                procMetadata.put(String.valueOf(param.getId()), param.getValue());
            });
            procNode.setProperties(procMetadata);

            Map<String, Object> procPropsIn = new HashMap<>();
            List<Input> inputs = proc.getInputs();
            inputs.stream().forEach((Input input) -> {
                Source source = input.getSource();
                List<Attribute> attrs = source.getOutput().getAttributes();
                attrs.stream().forEach((Attribute attr) -> {
                    procPropsIn.put(attr.getName(), attr.getType().getCanonicalName());
                });
                // Create edge
                Edge edge = new Edge();
                edge.setLabel(Vocabulary.MODEL);
                edge.setRelation(input.getName());
                edge.setDirected(true);
                edge.setSource(source.getClass().getCanonicalName() + ":" + source.getId().toString());
                edge.setTarget(proc.getClass().getCanonicalName() + ":" + proc.getId().toString());
                edges.add(edge);
            });
            procNode.setPropertiesIn(procPropsIn);

            Map<String, Object> procPropsOut = new HashMap<>();
            List<Attribute> attrs = proc.getOutput().getAttributes();
            attrs.stream().forEach((Attribute attr) -> {
                procPropsOut.put(attr.getName(), attr.getType().getCanonicalName());
            });
            procNode.setPropertiesOut(procPropsOut);

            nodes.add(procNode);
        });

        Set<ExternalSink> sinks = model.getExternalSinks();
        sinks.stream().forEach((ExternalSink sink) -> {
            Gnode sinkNode = new Gnode();
            sinkNode.setId(sink.getId().toString());
            sinkNode.setLabel(Vocabulary.SINK);
            sinkNode.setType(sink.getClass().getCanonicalName());

            Map<String, Object> sinkMetadata = new HashMap<>();
            Set<Parameter> params = sink.getParameters();
            params.stream().forEach((Parameter param) -> {
                sinkMetadata.put(String.valueOf(param.getId()), param.getValue());
            });
            sinkNode.setProperties(sinkMetadata);

            Map<String, Object> sinkPropsIn = new HashMap<>();
            List<? extends Input> inputs = sink.getInputs();
            inputs.stream().forEach((Input input) -> {
                Source source = input.getSource();
                List<Attribute> attrs = source.getOutput().getAttributes();
                attrs.stream().forEach((Attribute attr) -> {
                    sinkPropsIn.put(attr.getName(), attr.getType().getCanonicalName());
                });
                // Create edge
                Edge edge = new Edge();
                edge.setLabel(Vocabulary.MODEL);
                edge.setRelation(input.getName());
                edge.setDirected(true);
                edge.setSource(source.getClass().getCanonicalName() + ":" + source.getId().toString());
                edge.setTarget(sink.getClass().getCanonicalName() + ":" + sink.getId().toString());
                edges.add(edge);
            });
            sinkNode.setPropertiesIn(sinkPropsIn);

            nodes.add(sinkNode);
        });

        graph.setNodes(nodes);
        graph.setEdges(edges);

        return graph;
    }
}
