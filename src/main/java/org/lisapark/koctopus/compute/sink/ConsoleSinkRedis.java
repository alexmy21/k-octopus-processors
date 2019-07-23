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
package org.lisapark.koctopus.compute.sink;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.lettuce.core.StreamMessage;
import java.util.Iterator;
import org.lisapark.koctopus.core.AbstractNode;
import org.lisapark.koctopus.core.Input;
import org.lisapark.koctopus.core.Persistable;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.event.Event;
import org.lisapark.koctopus.core.runtime.SinkContext;
import org.lisapark.koctopus.core.source.Source;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.lisapark.koctopus.compute.source.TestSourceRedis;
import static org.lisapark.koctopus.compute.source.TestSourceRedis.newTemplate;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.graph.GraphUtils;
import org.lisapark.koctopus.core.parameter.Parameter;
import org.lisapark.koctopus.core.runtime.StreamProcessingRuntime;
import org.lisapark.koctopus.core.sink.external.CompiledExternalSink;
import org.lisapark.koctopus.core.sink.external.ExternalSink;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class ConsoleSinkRedis extends AbstractNode implements ExternalSink {

    private static final String DEFAULT_NAME = "Console for Redis";
    private static final String DEFAULT_DESCRIPTION = "Console Output from Redis";
    private static final String DEFAULT_INPUT = "Input";

    private static final int ATTRIBUTE_LIST_PARAMETER_ID = 1;
    private static final String ATTRIBUTE_LIST = "Show Attributes";
    private static final String ATTRIBUTE_LIST_DESCRIPTION
            = "List comma separated attribute names that you would like to show on Console. Empty - will show all attributes.";

    private final Input<Event> input;

    private ConsoleSinkRedis(UUID id, String name, String description) {
        super(id, name, description);
        input = Input.eventInputWithId(1);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private ConsoleSinkRedis(UUID id, ConsoleSinkRedis copyFromNode) {
        super(id, copyFromNode);
        input = copyFromNode.getInput().copyOf();
    }

    private ConsoleSinkRedis(ConsoleSinkRedis copyFromNode) {
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
    public ConsoleSinkRedis newInstance() {
        return new ConsoleSinkRedis(UUID.randomUUID(), this);
    }

    @Override
    public ConsoleSinkRedis newInstance(Gnode gnode) {
        String uuid = gnode.getId() == null ? UUID.randomUUID().toString() : gnode.getId();
        ConsoleSinkRedis testSource = newTemplate(UUID.fromString(uuid));
        GraphUtils.buildSink(testSource, gnode);
        
        return testSource;
    }

    @Override
    public ConsoleSinkRedis copyOf() {
        return new ConsoleSinkRedis(this);
    }

    public static ConsoleSinkRedis newTemplate() {
        UUID sinkId = UUID.randomUUID();        
        return newTemplate(sinkId);
    }
    
    public static ConsoleSinkRedis newTemplate(UUID sinkId) {
        ConsoleSinkRedis consoleSink = new ConsoleSinkRedis(sinkId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

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

    static class CompiledConsole extends CompiledExternalSink {

        private final ConsoleSinkRedis consoleSink;

        protected CompiledConsole(ConsoleSinkRedis processor) {
            super(processor);
            this.consoleSink = processor;
        }

        @Override
        public synchronized void processEvent(StreamProcessingRuntime runtime, Map<Integer, Event> eventsByInputId) {

            String sourceClassName = consoleSink.getInput().getSource().getClass().getCanonicalName();
            UUID sourceId = consoleSink.getInput().getSource().getId();

            int pageSize = 10;
            String offset = "0";
            int counter = 1000;
            while (true) {
                List<StreamMessage<String, String>> list = runtime.readFromStream(sourceClassName, sourceId, offset, pageSize);

                if (list.size() > 0) { // a message was read                    
                    list.forEach(msg -> {
                        if (msg != null) {
                            runtime.getStandardOut().println(msg);
                        } else {
                            runtime.getStandardOut().println("event is null");
                        }
                    });
                    offset = list.get(list.size() - 1).getId();
                } else {
                    break;
                }
            }
        }

        @Override
        public void processEvent(SinkContext ctx, Map<Integer, Event> eventsByInputId) {

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
}
