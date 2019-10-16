/* 
 * Copyright (C) 2013 Lisa Park, Inc. (www.lisa-park.net)
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
package org.lisapark.koctopus.processors.sink;

import com.fasterxml.uuid.Generators;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Iterator;
import org.lisapark.koctopus.core.AbstractNode;
import org.lisapark.koctopus.core.Input;
import org.lisapark.koctopus.core.Persistable;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.event.Event;
import org.lisapark.koctopus.core.sink.SinkContext;
import org.lisapark.koctopus.core.source.Source;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;
import org.lisapark.koctopus.core.event.Attribute;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.parameter.Parameter;
import org.lisapark.koctopus.core.transport.TransportReference;
import org.lisapark.koctopus.core.sink.external.CompiledExternalSink;
import org.lisapark.koctopus.core.sink.external.ExternalSink;
import org.lisapark.koctopus.core.transport.Transport;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class ConsoleSinkOld extends AbstractNode implements ExternalSink {
    private static final String DEFAULT_NAME = "Console";
    private static final String DEFAULT_DESCRIPTION = "Console Output";
    private static final String DEFAULT_INPUT = "Input";    
    
    private static final int ATTRIBUTE_LIST_PARAMETER_ID = 1;
    private static final String ATTRIBUTE_LIST = "Show Attributes";
    private static final String ATTRIBUTE_LIST_DESCRIPTION = 
            "List comma separated attribute names that you would like to show on Console. Empty - will show all attributes.";
    
    private final Input<Event> input;
    
    protected Map<String, TransportReference> sourceref = new HashMap<>();

    private ConsoleSinkOld(UUID id, String name, String description) {
        super(id, name, description);
        input = Input.eventInputWithId(1);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private ConsoleSinkOld(UUID id, ConsoleSinkOld copyFromNode) {
        super(id, copyFromNode);
        input = copyFromNode.getInput().copyOf();
    }

    private ConsoleSinkOld(ConsoleSinkOld copyFromNode) {
        super(copyFromNode);
        this.input = copyFromNode.input.copyOf();
    }
    
    @SuppressWarnings("unchecked")
    public void setAttributeList(String attributeList) throws ValidationException {
        getParameter(ATTRIBUTE_LIST_PARAMETER_ID).setValue(attributeList);
    }

    public String getAttributeList() {
        return getParameter(ATTRIBUTE_LIST_PARAMETER_ID).getValueAsString();
    }

    public Input<Event> getInput() {
        return input;
    }

    @Override
    public List<Input<Event>> getInputs() {
        return ImmutableList.of(input);
    }

    @Override
    public boolean isConnectedTo(Source source) {

        return input.isConnectedTo(source);
    }

    @Override
    public void disconnect(Source source) {
        if (input.isConnectedTo(source)) {
            input.clearSource();
        }
    }

    @Override
    public ConsoleSinkOld newInstance() {
        return new ConsoleSinkOld(Generators.timeBasedGenerator().generate(), this);
    }

    @Override
    public ConsoleSinkOld newInstance(Gnode gnode) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ConsoleSinkOld copyOf() {
        return new ConsoleSinkOld(this);
    }

    public static ConsoleSinkOld newTemplate() {
        UUID sinkId = Generators.timeBasedGenerator().generate();
        ConsoleSinkOld consoleSink = new ConsoleSinkOld(sinkId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        
        consoleSink.addParameter(
                Parameter.stringParameterWithIdAndName(ATTRIBUTE_LIST_PARAMETER_ID, ATTRIBUTE_LIST)
                .description(ATTRIBUTE_LIST_DESCRIPTION)
                );
        
        return consoleSink;
    }

    @Override
    public CompiledExternalSink compile() throws ValidationException {
        return new CompiledConsole(copyOf());
    }

    @Override
    public Map<String,TransportReference> getReferences() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setReferences(Map<String,TransportReference> sourceref) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <T extends ExternalSink> CompiledExternalSink compile(T sink) throws ValidationException {
        return new CompiledConsole((ConsoleSinkOld) sink);
    }

    static class CompiledConsole extends CompiledExternalSink {
        
        private final ConsoleSinkOld consoleSink; 
        
        protected CompiledConsole(ConsoleSinkOld processor) {
            super(processor);
            this.consoleSink = processor;
        }

        @Override
        public synchronized void processEvent(SinkContext ctx, Map<Integer, Event> eventsByInputId) {
            Event event = eventsByInputId.get(1);
            if (event != null) {
                
                UUID sourceId = consoleSink.getInput().getSource().getId();
                String sourceClass = consoleSink.getInput().getSource().getClass().getCanonicalName();
                
                String attributeList = consoleSink.getAttributeList();
                if(attributeList == null || attributeList.isEmpty()){
                    // Print out everything
                    List<Attribute> attributes = consoleSink.getInput().getSource().getOutput().getAttributes();
                    
                    attributeList = attributes.stream().map(attr -> attr.getName()).collect(Collectors.joining(","));                    
                    String outputString = formatOutput(event, attributeList);
                    ctx.getStandardOut().println(outputString);
                } else {
                    // Print out only selected attributes
                    String outputString = formatOutput(event, attributeList);                    
                    // Print out only if there are some attributes are not null
                    if(outputString != null){
                        ctx.getStandardOut().println(outputString);
                    }
                }
            } else {
                ctx.getStandardOut().println("event is null");
            }
        }

        private String formatOutput(Event event, String attributeList) {
            
            Map<String, Object> data = event.getData();            
            StringBuilder outputString = new StringBuilder();            
            String[] attList = attributeList.split(",");
            
            for (String attr : attList) {
                if(data.get(attr) != null){
                    
                    if (outputString.length() > 1) {
                        outputString.append(", ");
                    }
                    
                    outputString.append(attr).append("=").append(data.get(attr));
                    if (data.get(attr) instanceof Map) {
                        Map map = Maps.newHashMap((Map) data.get(attr));
                        outputString = extractMap(map, attr, outputString);
                    }
                }
            }            

            if(outputString.length() > 0){
                return "{" + outputString.toString() + "}";
            } else {
                return null;
            }
        }

        public StringBuilder extractMap(Map<String, Object> map, String attr, StringBuilder outputString) {

            for (Iterator it = map.entrySet().iterator(); it.hasNext();) {
                outputString.append(", ");
                Entry entry = (Entry) it.next();
                outputString.append(entry.getKey()).append("=").append(entry.getValue());
                if (entry.getValue() instanceof Map) {
                    Map _map = Maps.newHashMap((Map) entry.getValue());
                    outputString = extractMap(_map, entry.getKey().toString(), outputString);
                }
            }

            return outputString;
        }

        @Override
        public Object processEvent(Transport runtime) {
            // TODO. remove when transition completed
            throw new UnsupportedOperationException("Not supported yet."); 
            //To change body of generated methods, choose Tools | Templates.
        }
    }
}
