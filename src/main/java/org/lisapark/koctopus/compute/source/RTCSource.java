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

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import org.lisapark.koctopus.core.Output;
import org.lisapark.koctopus.core.Persistable;
import org.lisapark.koctopus.core.ProcessingException;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.event.Event;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.parameter.Parameter;
import org.lisapark.koctopus.core.runtime.ProcessingRuntime;
import org.lisapark.koctopus.core.source.external.CompiledExternalSource;
import org.lisapark.koctopus.core.source.external.ExternalSource;
import org.openide.util.Exceptions;
import org.lisapark.koctopus.core.runtime.StreamingRuntime;

/**
 *
 * @author alex (alexmy@lisa-park.com)
 */
@Persistable
public class RTCSource extends ExternalSource {

    private static final String DEFAULT_NAME = "RTC Source";
    private static final String DEFAULT_DESCRIPTION = "Run Time Container Source Processor. Initiates Chained Model Work Flow.";
    private static final int RTC_SIGNAL_NAME_PARAMETER_ID = 1;
    private static final int RTC_SIGNAL_VALUE_PARAMETER_ID = 2;

    public RTCSource(UUID id, String name, String description) {
        super(id, name, description);
    }

    private RTCSource(UUID id, RTCSource copyFromSource) {
        super(id, copyFromSource);
    }

    public RTCSource(RTCSource copyFromSource) {
        super(copyFromSource);
    }

    public String getStartSignalName() {
        return getParameter(RTC_SIGNAL_NAME_PARAMETER_ID).getValueAsString();
    }

    public Boolean getStartSignalValue() {
        return (Boolean) getParameter(RTC_SIGNAL_VALUE_PARAMETER_ID).getValue();
    }

    @Override
    public RTCSource copyOf() {
        return new RTCSource(this);
    }

    @Override
    public RTCSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new RTCSource(sourceId, this);
    }

    @Override
    public RTCSource newInstance(Gnode gnode) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public static RTCSource newTemplate() {
        UUID sourceId = UUID.randomUUID();

        RTCSource rtcSource = new RTCSource(sourceId, "RTC Source", "Generates run token to start all Octopus model's"
                + " containers that are connected to the source.");
        rtcSource.setOutput(Output.outputWithId(1).setName("Output data"));

        rtcSource.addParameter(Parameter.stringParameterWithIdAndName(RTC_SIGNAL_NAME_PARAMETER_ID, "Run signal name:")
                .description("Run signal/token name.")
                .defaultValue("start")
                .required(true));

        rtcSource.addParameter(Parameter.booleanParameterWithIdAndName(RTC_SIGNAL_VALUE_PARAMETER_ID, "Run signal value:")
                .description("Run signal value.")
//                .defaultValue(true)
                .required(true));

        return rtcSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        return new CompiledRedisSource(copyOf());
    }

    @Override
    public <T extends ExternalSource> CompiledExternalSource compile(T source) throws ValidationException {
        return new CompiledRedisSource((RTCSource) source);
    }

    class CompiledRedisSource implements CompiledExternalSource {

        protected final Logger logger = Logger.getLogger(CompiledRedisSource.class.getName());
        protected final RTCSource source;
        protected volatile boolean running;
        protected Thread thread;
        private final long SLIEEP_TIME = 100L;

        public CompiledRedisSource(RTCSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) {
            
            thread = Thread.currentThread();
            running = true;
            
            int numberEventsCreated = 0;

            Map attributeData = Maps.newHashMap();
            attributeData.put(this.source.getStartSignalName(), this.source.getStartSignalValue());

            while (!thread.isInterrupted() && running && numberEventsCreated < 1) {
                Event e = new Event(attributeData);

                runtime.sendEventFromSource(e, source);
                
                numberEventsCreated++;
                try {
                    Thread.sleep(SLIEEP_TIME);
                } catch (InterruptedException ex) {
                    Exceptions.printStackTrace(ex);
                }
            }            
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }

        @Override
        public void startProcessingEvents(StreamingRuntime runtime) throws ProcessingException {
            
        }
    }
}
