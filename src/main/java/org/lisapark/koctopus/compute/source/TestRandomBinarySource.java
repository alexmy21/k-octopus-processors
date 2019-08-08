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
package org.lisapark.koctopus.compute.source;

import com.fasterxml.uuid.Generators;
import com.google.common.collect.Maps;
import org.lisapark.koctopus.core.Output;
import org.lisapark.koctopus.core.Persistable;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.event.Attribute;
import org.lisapark.koctopus.core.event.Event;
import org.lisapark.koctopus.core.event.EventType;
import org.lisapark.koctopus.core.parameter.Constraints;
import org.lisapark.koctopus.core.parameter.Parameter;
import org.lisapark.koctopus.core.runtime.ProcessingRuntime;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.lisapark.koctopus.core.ProcessingException;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.source.external.CompiledExternalSource;
import org.lisapark.koctopus.core.source.external.ExternalSource;
import org.openide.util.Exceptions;
import org.lisapark.koctopus.core.runtime.StreamingRuntime;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class TestRandomBinarySource extends ExternalSource {

    private static final String DEFAULT_NAME = "Test Random Binary";
    private static final String DEFAULT_DESCRIPTION = "Generate random binary source "
            + "data according to the provided attribute list.";

    private static final int NUMBER_OF_EVENTS_PARAMETER_ID = 1;
    private static final int BREAK_POINT_PARAMETER_ID = 2;

    public TestRandomBinarySource(UUID id, String name, String description) {
        super(id, name, description);
    }

    private TestRandomBinarySource(UUID id, TestRandomBinarySource copyFromSource) {
        super(id, copyFromSource);
    }

    public TestRandomBinarySource(TestRandomBinarySource copyFromSource) {
        super(copyFromSource);
    }

    public Integer getNumberOfEvents() {
        return getParameter(NUMBER_OF_EVENTS_PARAMETER_ID).getValueAsInteger();
    }

    
    public Double getBreakPoint() {
        return (Double) getParameter(BREAK_POINT_PARAMETER_ID).getValue();
    }

    @Override
    public TestRandomBinarySource copyOf() {
        return new TestRandomBinarySource(this);
    }

    @Override
    public TestRandomBinarySource newInstance() {
        UUID sourceId = Generators.timeBasedGenerator().generate();
        return new TestRandomBinarySource(sourceId, this);
    }

    @Override
    public TestRandomBinarySource newInstance(Gnode gnode) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public static TestRandomBinarySource newTemplate() {
        UUID sourceId = Generators.timeBasedGenerator().generate();

        TestRandomBinarySource testSource = new TestRandomBinarySource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        testSource.setOutput(Output.outputWithId(1).setName("Output"));
        testSource.addParameter(
                Parameter.integerParameterWithIdAndName(NUMBER_OF_EVENTS_PARAMETER_ID, "Number of Events").
                        description("Number of test events to generate.").
                        defaultValue(10).
                        constraint(Constraints.integerConstraintWithMinimumAndMessage(1,
                        "Number of events has to be greater than zero.")));
        
        testSource.addParameter(
                Parameter.doubleParameterWithIdAndName(BREAK_POINT_PARAMETER_ID, "Break Point").
                        description("Number in the interval [0, 1]. If random generated namber"
                                + " is greater than this number output is 1, otherwise - 0.").
                        defaultValue(.5).
                        required(true));
        
        return testSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        return new CompiledTestSource(copyOf());
    }

    @Override
    public <T extends ExternalSource> CompiledExternalSource compile(T source) throws ValidationException {
        return new CompiledTestSource((TestRandomBinarySource)source);
    }

    static class CompiledTestSource implements CompiledExternalSource {

        private final TestRandomBinarySource source;

        /**
         * Running is declared volatile because it may be access my different threads
         */
        private volatile boolean running;
        private final long SLIEEP_TIME = 1L;

        public CompiledTestSource(TestRandomBinarySource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) {
            Thread thread = Thread.currentThread();
            running = true;

            EventType eventType = source.getOutput().getEventType();
            List<Attribute> attributes = eventType.getAttributes();
            int numberEventsCreated = 0;

            while (!thread.isInterrupted() && running && numberEventsCreated < source.getNumberOfEvents()) {
                
                int x = (Math.random() < source.getBreakPoint())? 0 : 1;
                Event e = createEvent(attributes, x);

                runtime.sendEventFromSource(e, source);
                
                numberEventsCreated++;
                
                try {
                    Thread.sleep(SLIEEP_TIME);
                } catch (InterruptedException ex) {
                    Exceptions.printStackTrace(ex);
                }
            }
        }

        private Event createEvent(List<Attribute> attributes, int random) {
            Map<String, Object> attributeData = Maps.newHashMap();

            attributes.forEach((attribute) -> {
                attributeData.put(attribute.getName(), attribute.createSampleData(random));
            });

            return new Event(attributeData);
        }

        @Override
        public void stopProcessingEvents() {
            running = false;
        }

        @Override
        public Object startProcessingEvents(StreamingRuntime runtime) throws ProcessingException {
            return null;
        }
    }
}
