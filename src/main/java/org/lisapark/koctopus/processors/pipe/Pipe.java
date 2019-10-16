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
package org.lisapark.koctopus.processors.pipe;

import com.fasterxml.uuid.Generators;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.lisapark.koctopus.ProgrammerException;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.event.Event;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.memory.Memory;
import org.lisapark.koctopus.core.memory.MemoryProvider;
import org.lisapark.koctopus.core.processor.CompiledProcessor;
import org.lisapark.koctopus.core.processor.AbstractProcessor;
import org.lisapark.koctopus.core.processor.ProcessorInput;
import org.lisapark.koctopus.core.processor.ProcessorOutput;
import org.lisapark.koctopus.core.runtime.ProcessorContext;
import org.lisapark.koctopus.core.transport.TransportReference;
import org.lisapark.koctopus.core.transport.Transport;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com) 
 */
public class Pipe extends AbstractProcessor<Integer> {
    private static final String DEFAULT_NAME = "Connector";
    private static final String DEFAULT_DESCRIPTION = "Transfere data from one processor to another and count number of transactions.";
    private static final String DEFAULT_INPUT_DESCRIPTION = "Field name";
    private static final String DEFAULT_OUTPUT_DESCRIPTION = "Counter name.";

    /**
     * Pipe takes a single input
     */
    private static final int INPUT_ID = 1;
    private static final int OUTPUT_ID = 1;

    protected Pipe(UUID id, String name, String description) {
        super(id, name, description);
    }

    protected Pipe(UUID id, Pipe copyFromPipe) {
        super(id, copyFromPipe);
    }

    protected Pipe(Pipe copyFromPipe) {
        super(copyFromPipe);
    }

    public ProcessorInput getInput() {
        // there is only one input for a Pipe
        return getInputs().get(0);
    }

    @Override
    public Pipe newInstance() {
        return new Pipe(Generators.timeBasedGenerator().generate(), this);
    }

    @Override
    public Pipe newInstance(Gnode gnode) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Pipe copyOf() {
        return new Pipe(this);
    }

    /**
     * {@link Pipe}s need memory to store the prior events that will be used to calculate the average based on. We
     * used a {@link MemoryProvider#createCircularBuffer(int)} to store this data.
     *
     * @param memoryProvider used to create Pipe's memory
     * @return circular buffer
     */
    @Override
    public Memory<Integer> createMemoryForProcessor(MemoryProvider memoryProvider) {
        return memoryProvider.createCircularBuffer(1);
    }

    /**
     * Validates and compile this Pipe.Doing so takes a "snapshot" of the {@link #getInputs()} and {@link #output}
     * and returns a {@link CompiledProcessor}.
     *
     * @return CompiledProcessor
     * @throws org.lisapark.koctopus.core.ValidationException
     */
    @Override
    public CompiledProcessor<Integer> compile() throws ValidationException {
        validate();

        // we copy all the inputs and output taking a "snapshot" of this processor so we are isolated of changes
        Pipe copy = copyOf();
        return new CompiledPipe(copy);
    }

    /**
     * Returns a new {@link Pipe} processor configured with all the appropriate {@link org.lisapark.koctopus.core.parameter.Parameter}s, {@link Input}s
     * and {@link Output}.
     *
     * @return new {@link Pipe}
     */
    public static Pipe newTemplate() {
        UUID processorId = Generators.timeBasedGenerator().generate();
        Pipe pipe = new Pipe(processorId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        // only a single double input
        pipe.addInput(
                ProcessorInput.objectInputWithId(INPUT_ID).name("Input data").description(DEFAULT_INPUT_DESCRIPTION)
        );
        // double output
        try {
            pipe.setOutput(
                    ProcessorOutput.doubleOutputWithId(OUTPUT_ID).name("Counter").description(DEFAULT_OUTPUT_DESCRIPTION).attributeName("counter")
            );
        } catch (ValidationException ex) {
            // this should NOT happen. It means we created the Pipe with an invalid attriubte name
            throw new ProgrammerException(ex);
        }

        return pipe;
    }

    @Override
    public <T extends AbstractProcessor> CompiledProcessor<Integer> compile(T processor) throws ValidationException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Map<String, TransportReference> getReferences() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setReferences(Map<String, TransportReference> sourceref) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * This {@link CompiledProcessor} is the actual logic that implements the Simple Moving Average.
     */
    static class CompiledPipe extends CompiledProcessor<Integer> {

        protected CompiledPipe(Pipe pipe) {
            super(pipe);
        }

        @Override
        public Object processEvent(ProcessorContext<Integer> ctx, Map<Integer, Event> eventsByInputId) {

            Memory<Integer> processorMemory = ctx.getProcessorMemory();

            final Collection<Integer> memoryItems = processorMemory.values();
                        
            List<Integer> list = Lists.newArrayList();
            memoryItems.forEach((memoryItem) -> {
                list.add(memoryItem++);
            });
            processorMemory.add(list.get(0));
            
            return list.get(0);
        }

        @Override
        public Object processEvent(Transport runtime) {
            throw new UnsupportedOperationException("Not supported yet."); 
            //To change body of generated methods, choose Tools | Templates.
        }
    }
}
