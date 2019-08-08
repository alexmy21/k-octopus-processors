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
import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.Maps;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Enumeration;
import org.lisapark.koctopus.core.Output;
import org.lisapark.koctopus.core.Persistable;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.event.Attribute;
import org.lisapark.koctopus.core.event.Event;
import org.lisapark.koctopus.core.event.EventType;
import org.lisapark.koctopus.core.parameter.Parameter;
import org.lisapark.koctopus.core.runtime.ProcessingRuntime;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.lisapark.koctopus.core.source.external.CompiledExternalSource;
import org.lisapark.koctopus.core.source.external.ExternalSource;
import org.lisapark.koctopus.compute.util.Booleans;
import org.lisapark.koctopus.core.ProcessingException;
import org.lisapark.koctopus.core.graph.Gnode;
import org.openide.util.Exceptions;
import org.lisapark.koctopus.core.runtime.StreamingRuntime;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class GdeltZipSource extends ExternalSource {

    private static final String DEFAULT_NAME = "GDELT Zip File source";
    private static final String DEFAULT_DESCRIPTION = "Unzip file and convert to the stream of events.";

    private static final int ZIP_FILE_DIR_PARAMETER_ID = 1;
    private static final int FILE_NAME_PARAMETER_ID = 2;
    private static final int READ_LIMIT_PARAMETER_ID = 3;

    private static void initAttributeList(GdeltZipSource gdeltZipSource) throws ValidationException {

        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Integer.class, "GlobalEventID"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Integer.class, "Day"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Integer.class, "MonthYear"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Integer.class, "Year"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Double.class, "FractionDate"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor1Code"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor1Name"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor1CountryCode"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor1KnownGroupCode"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor1EthnicCode"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor1Religion1Code"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor1Religion2Code"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor1Type1Code"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor1Type2Code"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor1Type3Code"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor2Code"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor2Name"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor2CountryCode"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor2KnownGroupCode"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor2EthnicCode"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor2Religion1Code"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor2Religion2Code"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor2Type1Code"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor2Type2Code"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor2Type3Code"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Boolean.class, "IsRootEvent"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "EventCode"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "EventBaseCode"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "EventRootCode"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Integer.class, "QuadClass"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Double.class, "GoldsteinScale"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Integer.class, "NumMentions"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Integer.class, "NumSources"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Integer.class, "NumArticles"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Double.class, "AvgTone"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Integer.class, "Actor1Geo_Type"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor1Geo_Fullname"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor1Geo_CountryCode"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor1Geo_ADM1Code"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Double.class, "Actor1Geo_Lat"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Double.class, "Actor1Geo_Long"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor1Geo_FeatureID"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Integer.class, "Actor2Geo_Type"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor2Geo_Fullname"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor2Geo_CountryCode"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor2Geo_ADM1Code"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Double.class, "Actor2Geo_Lat"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Double.class, "Actor2Geo_Long"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Actor2Geo_FeatureID"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Integer.class, "ActionGeo_Type"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "ActionGeo_Fullname"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "ActionGeo_CountryCode"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "ActionGeo_ADM1Code"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Double.class, "ActionGeo_Lat"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(Double.class, "ActionGeo_Long"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "ActionGeo_FeatureID"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "DATEADDED"));
        gdeltZipSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "SOURCEURL"));
    }

    public GdeltZipSource(UUID id, String name, String description) {
        super(id, name, description);
    }

    private GdeltZipSource(UUID id, GdeltZipSource copyFromSource) {
        super(id, copyFromSource);
    }

    public GdeltZipSource(GdeltZipSource copyFromSource) {
        super(copyFromSource);
    }

    public Integer getReadLimit() {
        return getParameter(READ_LIMIT_PARAMETER_ID).getValueAsInteger();
    }

    private String getFileName() {
        return getParameter(FILE_NAME_PARAMETER_ID).getValueAsString();
    }

    private String getZipDir() {
        return getParameter(ZIP_FILE_DIR_PARAMETER_ID).getValueAsString();
    }

    @Override
    public GdeltZipSource copyOf() {
        return new GdeltZipSource(this);
    }

    @Override
    public GdeltZipSource newInstance() {
        UUID sourceId = Generators.timeBasedGenerator().generate();
        return new GdeltZipSource(sourceId, this);
    }

    @Override
    public GdeltZipSource newInstance(Gnode gnode) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public static GdeltZipSource newTemplate() {
        UUID sourceId = Generators.timeBasedGenerator().generate();

        GdeltZipSource gdeltZipSource = new GdeltZipSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        gdeltZipSource.setOutput(Output.outputWithId(1).setName("Output"));

        gdeltZipSource.addParameter(
                Parameter.integerParameterWithIdAndName(READ_LIMIT_PARAMETER_ID, "Read limit").
                description("Read line limit.").
                defaultValue(10));

        gdeltZipSource.addParameter(
                Parameter.stringParameterWithIdAndName(FILE_NAME_PARAMETER_ID, "File Name").
                description("Zip File Name.").
                defaultValue("20140306.export.CSV.zip"));

        gdeltZipSource.addParameter(
                Parameter.stringParameterWithIdAndName(ZIP_FILE_DIR_PARAMETER_ID, "Zip Dir").
                description("Zip directory on the server.").
                defaultValue("/home/alexmy/GDELT/"));
        try {
            initAttributeList(gdeltZipSource);
        } catch (ValidationException ex) {
            Exceptions.printStackTrace(ex);
        }

        return gdeltZipSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        return new CompiledTestSource(copyOf());
    }

    @Override
    public <T extends ExternalSource> CompiledExternalSource compile(T source) throws ValidationException {
        return new CompiledTestSource((GdeltZipSource) source);
    }

    static class CompiledTestSource implements CompiledExternalSource {
        private final GdeltZipSource source;
        /**
         * Running is declared volatile because it may be access my different
         * threads
         */
        private volatile boolean running;

        public CompiledTestSource(GdeltZipSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) {
            synchronized (this) {
                checkState(!running, "Source is already processing events. Cannot call processEvents again");
                running = true;
            }
            EventType eventType = source.getOutput().getEventType();
            Integer readLimit = source.getReadLimit();
            String fileName = source.getFileName();
            String zipDir = source.getZipDir();
            Map<String, Object> attributeData = Maps.newHashMap();
            int count = 0;
            String line;
            try {
                ZipFile zipFile = new ZipFile(zipDir.endsWith("/") ? zipDir + fileName : zipDir + "/" + fileName);
                final Enumeration<? extends ZipEntry> entries = zipFile.entries();
                while (running && entries.hasMoreElements()) {
                    final ZipEntry zipEntry = entries.nextElement();
                    if (!zipEntry.isDirectory()) {
                        InputStream input = zipFile.getInputStream(zipEntry);
                        try {
                            BufferedReader br = new BufferedReader(new InputStreamReader(input, "UTF-8"));
                            if (readLimit > 0) {
                                while ((line = br.readLine()) != null && count < readLimit) {
                                    Event newEvent = createEventFromLine(line, eventType);
                                    runtime.sendEventFromSource(newEvent, source);
                                    count++;
                                }
                            } else {
                                while ((line = br.readLine()) != null) {
                                    Event newEvent = createEventFromLine(line, eventType);
                                    runtime.sendEventFromSource(newEvent, source);
                                }
                            }
                        } catch (IllegalArgumentException iae) {
                            System.err.println(iae.getMessage());
                        } catch (IOException e) {                            
                            System.err.println("Unhandled exception:");
                        } finally {
                            if (input != null) {
                                input.close();
                            }
                        }
                    }
                }
            } catch (final IOException ioe) {
                System.err.println("Unhandled exception:");
            }
        }

        @Override
        public void stopProcessingEvents() {
            running = false;
        }

        private synchronized Event createEventFromLine(String line, EventType eventType) {
            String[] fields = line.split("\\t");
            Map<String, Object> attributeValues = Maps.newHashMap();
            int index = 0;
            List<Attribute> attributes = eventType.getAttributes();
            for (Attribute attribute : attributes) {
                Class type = attribute.getType();
                String attributeName = attribute.getName();
                String field = fields[index];
                if (field.isEmpty()) {
                    attributeValues.put(attributeName, null);
                } else {
                    try {
                        if (type == String.class) {
                            String value = field;
                            attributeValues.put(attributeName, value);
                        } else if (type == Integer.class) {
                            int value = Integer.valueOf(field);
                            attributeValues.put(attributeName, value);
                        } else if (type == Short.class) {
                            short value = Short.valueOf(field);
                            attributeValues.put(attributeName, value);
                        } else if (type == Long.class) {
                            long value = Long.valueOf(field);
                            attributeValues.put(attributeName, value);
                        } else if (type == Double.class) {
                            double value = Double.valueOf(field);
                            attributeValues.put(attributeName, value);
                        } else if (type == Float.class) {
                            float value = Float.valueOf(field);
                            attributeValues.put(attributeName, value);
                        } else if (type == Boolean.class) {
                            attributeValues.put(attributeName, Booleans.parseBoolean(field));
                        } else {
                            throw new IllegalArgumentException(String.format("Unknown attribute type %s", type));
                        }
                    } catch (IllegalArgumentException e) {
                        System.out.println(e.getMessage());
                        attributeValues.put(attributeName, null);
                    }
                }
                index++;
                if (index >= fields.length) {
                    break;
                }
            }
            return new Event(attributeValues);
        }

        @Override
        public Object startProcessingEvents(StreamingRuntime runtime) throws ProcessingException {
            return null;
        }
    }
}
