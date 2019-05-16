/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kisline.processors.base;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kisline.processors.base.com.kisline.processors.base.model.BitcoinHistory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import java.io.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"Bitcoin", "NICE", "JSON", "XML", "CSV"})
@CapabilityDescription("Process Bitcoin transactions into XML or JSON format or both")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class BitcoinHistoryProcessor extends AbstractProcessor {

  private static ObjectMapper mapper;
  private AtomicReference<Marshaller> marshaller = new AtomicReference<>();

  private enum Output {
    ALL,
    JSON,
    XML
  };

  private AtomicReference<Output> output = new AtomicReference<>();

  @Override
  protected void init(final ProcessorInitializationContext context) {
    mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    try {
      JAXBContext jaxb = JAXBContext.newInstance(BitcoinHistory.class);
      marshaller.set(jaxb.createMarshaller());
    } catch (Exception e) {
      getLogger().error("Could not create JAXB context", e);
    }
  }

  @Override
  public Set<Relationship> getRelationships() {
    return ConfigUtil.getRelationships();
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return ConfigUtil.getProperties();
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {
    final String output = context.getProperty(ConfigUtil.OUTPUT).getValue();
    this.output.set(Output.valueOf(output));
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session)
      throws ProcessException {
    FlowFile flowFile = session.get();
    if (flowFile == null) {
      return;
    }

    final AtomicInteger jsonCounter = new AtomicInteger();
    final AtomicInteger xmlCounter = new AtomicInteger();
    final AtomicInteger recordsCounter = new AtomicInteger();

    final Set<BitcoinHistory> jsonRecords = ConcurrentHashMap.newKeySet();
    final Set<BitcoinHistory> xmlRecords = ConcurrentHashMap.newKeySet();

    session.read(
        flowFile,
        new InputStreamCallback() {
          @Override
          public void process(InputStream in) throws IOException {
            try (final BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
              final Iterable<CSVRecord> records =
                  CSVFormat.RFC4180.withFirstRecordAsHeader().parse(reader);
              for (final CSVRecord record : records) {
                recordsCounter.incrementAndGet();
                if (isValid(record)) {
                  final BitcoinHistory history = createModel(record);
                  if (isOutputJson()) {
                    jsonRecords.add(history);
                  }
                  if (isOutputXml()) {
                    xmlRecords.add(history);
                  }
                }
              }
            } catch (Exception e) {
              getLogger().error("Error processing input", e);
              session.transfer(flowFile, ConfigUtil.FAILURE);
            }
          }
        });

    if (!isOutputJson()) {
      writeJson(session, flowFile, jsonRecords, jsonCounter);
    }

    if (!isOutputXml()) {
      writeXml(session, flowFile, xmlRecords, xmlCounter);
    }

    session.adjustCounter(ConfigUtil.RECORDS_READ, recordsCounter.get(), true);
    session.adjustCounter(ConfigUtil.JSON_RECORDS, jsonCounter.get(), true);
    session.adjustCounter(ConfigUtil.XML_RECORDS, xmlCounter.get(), true);

    session.remove(flowFile);
  }

  private boolean isValid(final CSVRecord record) {
    return !record.toString().contains("NaN");
  }

  private boolean isOutputJson() {
    return output.get() == Output.ALL || output.get() == Output.JSON;
  }

  private boolean isOutputXml() {
    return output.get() == Output.ALL || output.get() == Output.XML;
  }

  private BitcoinHistory createModel(final CSVRecord record) {
    final BitcoinHistory history = new BitcoinHistory();
    final long unixTimestamp = Long.parseLong(record.get("Timestamp"));
    final Instant instant = Instant.ofEpochSecond(unixTimestamp);
    final ZonedDateTime timestamp = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());

    final double open = Double.parseDouble(record.get("Open"));
    final double high = Double.parseDouble(record.get("High"));
    final double low = Double.parseDouble(record.get("Low"));
    final double close = Double.parseDouble(record.get("Close"));
    final double btcVolume = Double.parseDouble(record.get("Volume_(BTC)"));
    final double usdVolume = Double.parseDouble(record.get("Volume_(Currency)"));
    final double weightedPrice = Double.parseDouble(record.get("Weighted_Price"));

    history.setTimestamp(timestamp);
    history.setOpen(open);
    history.setHigh(high);
    history.setLow(low);
    history.setClose(close);
    history.setBtcVolume(btcVolume);
    history.setUsdVolume(usdVolume);
    history.setWeightedPrice(weightedPrice);
    return history;
  }

  private void writeXml(
      ProcessSession session,
      FlowFile flowFile,
      Set<BitcoinHistory> xmlRecords,
      AtomicInteger count) {
    for (final BitcoinHistory history : xmlRecords) {
      final FlowFile createdFlowFile = session.create(flowFile);
      final FlowFile xmlFlowFile =
          session.write(
              createdFlowFile,
              new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                  try (final BufferedWriter writer =
                      new BufferedWriter(new OutputStreamWriter(out))) {
                    marshaller.get().marshal(history, writer);
                  } catch (Exception e) {
                    getLogger()
                        .error("Could not write record to XML : {}", new Object[] {history}, e);
                  }
                }
              });

      final Map<String, String> attrs = new ConcurrentHashMap<String, String>();
      final String fileName = getFileName(xmlFlowFile) + count.incrementAndGet() + ".xml";
      attrs.put(CoreAttributes.FILENAME.key(), fileName);
      attrs.put(CoreAttributes.MIME_TYPE.key(), ConfigUtil.XML_MIME_TYPE);
      attrs.put(ConfigUtil.JSON_RECORDS, String.valueOf(count));

      final FlowFile updatedFlowFile = session.putAllAttributes(xmlFlowFile, attrs);
      session.transfer(updatedFlowFile, ConfigUtil.XML);
      getLogger().debug("Wrote {} to XML", new Object[] {history});
    }
  }

  private void writeJson(
      ProcessSession session,
      FlowFile flowFile,
      Set<BitcoinHistory> jsonRecords,
      AtomicInteger count) {
    for (final BitcoinHistory history : jsonRecords) {
      final FlowFile createdFlowFile = session.create(flowFile);
      final FlowFile jsonFlowFile =
          session.write(
              createdFlowFile,
              new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                  try (final BufferedWriter writer =
                      new BufferedWriter(new OutputStreamWriter(out))) {
                    mapper.writeValue(writer, history);
                  } catch (Exception e) {
                    getLogger()
                        .error("Could not write record to JSON : {}", new Object[] {history}, e);
                  }
                }
              });

      final Map<String, String> attrs = new ConcurrentHashMap<String, String>();
      final String fileName = getFileName(jsonFlowFile) + count.incrementAndGet() + ".json";
      attrs.put(CoreAttributes.FILENAME.key(), fileName);
      attrs.put(CoreAttributes.MIME_TYPE.key(), ConfigUtil.JSON_MIME_TYPE);
      attrs.put(ConfigUtil.JSON_RECORDS, String.valueOf(count));

      final FlowFile updatedFlowFile = session.putAllAttributes(jsonFlowFile, attrs);
      session.transfer(updatedFlowFile, ConfigUtil.JSON);
      getLogger().debug("Wrote {} to JSON", new Object[] {history});
    }
  }

  private String getFileName(FlowFile flowFile) {
    final String fileName = flowFile.getAttribute(CoreAttributes.FILENAME.key());
    return fileName.substring(0, fileName.lastIndexOf(" "));
  }
}
