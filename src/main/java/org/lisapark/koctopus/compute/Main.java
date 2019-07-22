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

import org.lisapark.koctopus.core.graph.NodeVocabulary;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.lisapark.koctopus.compute.processor.sma.Sma;
import org.lisapark.koctopus.compute.sink.ConsoleSinkRedis;
import org.lisapark.koctopus.compute.source.TestSourceRedis;
import org.lisapark.koctopus.core.Input;
import org.lisapark.koctopus.core.ProcessingModel;
import org.lisapark.koctopus.core.event.Attribute;
import org.lisapark.koctopus.core.graph.Edge;
import org.lisapark.koctopus.core.graph.Graph;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.graph.GraphUtils;
import org.lisapark.koctopus.core.graph.Vocabulary;
import org.lisapark.koctopus.core.parameter.Parameter;
import org.lisapark.koctopus.core.processor.Processor;
import org.lisapark.koctopus.core.sink.external.ExternalSink;
import org.lisapark.koctopus.core.source.Source;
import org.lisapark.koctopus.core.source.external.ExternalSource;
import org.lisapark.koctopus.util.Pair;

/**
 *
 * @author alexmy
 */
public class Main {

    public static void main(String[] args) {

        Graph graph = compileGraph(createProcessingModel());

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

    private static Graph compileGraph(ProcessingModel model) {
        Graph graph = new Graph();

        graph.setId(model.getId().toString());
        graph.setLabel(model.getName());
        graph.setType(Vocabulary.PROCESSING_GRAPH);
        graph.setDirected(Boolean.TRUE);

        Multimap<String, String> graphMetadata = HashMultimap.create();
        graphMetadata.put(Vocabulary.TRANSPORT_URL, model.getTransportUrl());

        Gson graphGson = GraphUtils.gsonGnodeMeta(graphMetadata);
        String jsonGraphMetadata = graphGson.toJson(graphMetadata);
        graph.setParams(jsonGraphMetadata);

        List<Gnode> nodes = new ArrayList<>();
        List<Edge> edges = new ArrayList<>();

        // Sources
        //======================================================================
        Set<ExternalSource> sources = model.getExternalSources();
        sources.stream().forEach((ExternalSource source) -> {
            Gnode sourceGnode = new Gnode();
            sourceGnode.setId(source.getId().toString());
            sourceGnode.setLabel(Vocabulary.SOURCE);
            sourceGnode.setType(source.getClass().getCanonicalName());
            sourceGnode.setTransportUrl(model.getTransportUrl());

            Multimap<String, Pair<String, String>> sourceParams = HashMultimap.create();
            Set<Parameter> params = source.getParameters();
            params.stream().forEach((Parameter param) -> {
                // TODO: Extend list of parameters with Layout
                String paramId = String.valueOf(param.getId());
                sourceParams.put(paramId, new Pair<>(NodeVocabulary.NAME, param.getName()));
                sourceParams.put(paramId, new Pair<>(NodeVocabulary.TYPE, param.getType().getCanonicalName()));
                
                String paramValue = param.getValue() == null ? null : param.getValue().toString();
                sourceParams.put(paramId, new Pair<>(NodeVocabulary.VALUE, paramValue));
            });
            Gson sourceGson = GraphUtils.gsonGnodeMeta(sourceParams);
            String jsonSourceParams = sourceGson.toJson(sourceParams);
            sourceGnode.setParams(jsonSourceParams);

            Multimap<String, Pair<String, String>> sourceOutput = HashMultimap.create();
            List<Attribute> attrs = source.getOutput().getAttributes();
            attrs.stream().forEach((Attribute attr) -> {
                // TODO: Rxtend list of parameters with Layout
                sourceOutput.put(attr.getName(), new Pair<>(NodeVocabulary.TYPE, attr.getType().getCanonicalName()));
            });
            Gson sourceOutputGson = GraphUtils.gsonGnodeMeta(sourceOutput);
            String jsonSourceOutput = sourceOutputGson.toJson(sourceOutput);
            sourceGnode.setOutput(jsonSourceOutput);

            nodes.add(sourceGnode);
        });

        // Processors
        //======================================================================
        Set<Processor> processors = model.getProcessors();
        processors.stream().forEach((Processor proc) -> {
            Gnode procGnode = new Gnode();
            procGnode.setId(proc.getId().toString());
            procGnode.setLabel(Vocabulary.PROCESSOR);
            procGnode.setType(proc.getClass().getCanonicalName());
            procGnode.setTransportUrl(model.getTransportUrl());

            // Setting params
            Multimap<String, Pair<String, String>> procParams = HashMultimap.create();
            Set<Parameter> params = proc.getParameters();
            params.stream().forEach((Parameter param) -> {
                // TODO: Extend list of parameters with Layout
                String paramId = String.valueOf(param.getId());
                procParams.put(paramId, new Pair<>(NodeVocabulary.NAME, param.getName()));
                procParams.put(paramId, new Pair<>(NodeVocabulary.TYPE, param.getType().getCanonicalName()));
                
                String paramValue = param.getValue() == null ? null : param.getValue().toString();
                procParams.put(paramId, new Pair<>(NodeVocabulary.VALUE, paramValue));
            });
            Gson procGson = GraphUtils.gsonGnodeMeta(procParams);
            String jsonSourceParams = procGson.toJson(procParams);
            procGnode.setParams(jsonSourceParams);

            // Setting inputs
            Multimap<String, Pair<String, String>> procInput = HashMultimap.create();
            List<Input> inputs = proc.getInputs();
            inputs.stream().forEach((Input input) -> {
                Source source = input.getSource();
                List<Attribute> attrs = source.getOutput().getAttributes();
                attrs.stream().forEach((Attribute attr) -> {
                    procInput.put(attr.getName(), new Pair<>(NodeVocabulary.TYPE, attr.getType().getCanonicalName()));
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
            Gson procInputGson = GraphUtils.gsonGnodeMeta(procInput);
            String jsonSourceInput = procInputGson.toJson(procInput);
            procGnode.setInput(jsonSourceInput);
            
            // Setting outputs
            Multimap<String, Pair<String, String>> procOutput = HashMultimap.create();
            List<Attribute> attrs = proc.getOutput().getAttributes();
            attrs.stream().forEach((Attribute attr) -> {
                // TODO: Rxtend list of parameters with Layout
                procOutput.put(attr.getName(), new Pair<>(NodeVocabulary.TYPE, attr.getType().getCanonicalName()));
            });
            Gson procOutputGson = GraphUtils.gsonGnodeMeta(procOutput);
            String jsonSourceOutput = procOutputGson.toJson(procOutput);
            procGnode.setOutput(jsonSourceOutput);
            
            nodes.add(procGnode);
        });

        // Sinks
        //======================================================================
        Set<ExternalSink> sinks = model.getExternalSinks();
        sinks.stream().forEach((ExternalSink sink) -> {
            Gnode sinkGnode = new Gnode();
            sinkGnode.setId(sink.getId().toString());
            sinkGnode.setLabel(Vocabulary.SINK);
            sinkGnode.setType(sink.getClass().getCanonicalName());
            sinkGnode.setTransportUrl(model.getTransportUrl());

            // Setting params
            Multimap<String, Pair<String, String>> sinkParams = HashMultimap.create();
            Set<Parameter> params = sink.getParameters();
            params.stream().forEach((Parameter param) -> {
                // TODO: Extend list of parameters with Layout
                String paramId = String.valueOf(param.getId());
                sinkParams.put(paramId, new Pair<>(NodeVocabulary.NAME, param.getName()));
                sinkParams.put(paramId, new Pair<>(NodeVocabulary.TYPE, param.getType().getCanonicalName()));
                
                String paramValue = param.getValue() == null ? null : param.getValue().toString();
                sinkParams.put(paramId, new Pair<>(NodeVocabulary.VALUE, paramValue));
            });
            Gson sinkGson = GraphUtils.gsonGnodeMeta(sinkParams);
            String jsonSinkParams = sinkGson.toJson(sinkParams);
            sinkGnode.setParams(jsonSinkParams);
            
            // Setting inputs
            Multimap<String, Pair<String, String>> sinkInput = HashMultimap.create();
            List<? extends Input> inputs = sink.getInputs();
            inputs.stream().forEach((Input input) -> {
                Source source = input.getSource();
                List<Attribute> attrs = source.getOutput().getAttributes();
                attrs.stream().forEach((Attribute attr) -> {
                    sinkInput.put(attr.getName(), new Pair<>(NodeVocabulary.TYPE, attr.getType().getCanonicalName()));
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
            Gson sinkInputGson = GraphUtils.gsonGnodeMeta(sinkInput);
            String jsonSinkInput = sinkInputGson.toJson(sinkInput);
            sinkGnode.setInput(jsonSinkInput);
            
            nodes.add(sinkGnode);
        });

        graph.setNodes(nodes);
        graph.setEdges(edges);

        return graph;
    }
}
