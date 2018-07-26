/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.firehose.kafka;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka Firehose base on Kafka 1.0
 */
public class KafkaFirehoseFactory implements FirehoseFactory<ByteBufferInputRowParser>
{
  private static final Logger log = new Logger(KafkaFirehoseFactory.class);
  private static final String DEFAULT_MESSAGE_ENCODING = "UTF-8";
  private final Charset charset;

  @JsonProperty
  private final Properties consumerProps;

  @JsonProperty
  private final String feed;

  @JsonProperty
  @Nullable
  private final String rowDelimiter;

  @JsonProperty
  private final String messageEncoding;

  @JsonProperty
  private final Boolean isAdmonitorMode;

  @JsonProperty
  private final String iplibPath;

  @JsonCreator
  public KafkaFirehoseFactory(
      @JsonProperty("consumerProps") Properties consumerProps,
      @JsonProperty("feed") String feed,
      @JsonProperty("rowDelimiter") @Nullable String rowDelimiter,
      @JsonProperty("messageEncoding") @Nullable String messageEncoding,
      @JsonProperty("isAdmonitorMode") @Nullable Boolean isAdmonitorMode,
      @JsonProperty("iplibPath") @Nullable String iplibPath
  )
  {
    this.consumerProps = consumerProps;
    this.feed = feed;
    this.rowDelimiter = rowDelimiter;
    this.messageEncoding = messageEncoding == null ? DEFAULT_MESSAGE_ENCODING : messageEncoding;
    this.charset = Charset.forName(this.messageEncoding);
    this.isAdmonitorMode = isAdmonitorMode == null ? false : isAdmonitorMode;
    this.iplibPath = iplibPath;
  }

  @Override
  public Firehose connect(final ByteBufferInputRowParser firehoseParser, File temporaryDirectory) throws IOException
  {
    Set<String> newDimExclus = Sets.union(
        firehoseParser.getParseSpec().getDimensionsSpec().getDimensionExclusions(),
        Sets.newHashSet("feed")
    );
    final ByteBufferInputRowParser theParser = firehoseParser.withParseSpec(
        firehoseParser.getParseSpec()
                      .withDimensionsSpec(
                          firehoseParser.getParseSpec()
                                        .getDimensionsSpec()
                                        .withDimensionExclusions(
                                            newDimExclus
                                        )
                      )
    );
    if (rowDelimiter != null) {
      log.info("Parser class [%s]", theParser.getClass().toString());
      Preconditions.checkArgument(
          theParser instanceof StringInputRowParser,
          "rowDelimiter is available only for string input"
      );
    }
    if (isAdmonitorMode) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(iplibPath),
          "isAdmonitorMode is true means the iplibPath must be given"
      );
      synchronized (IpUtil.class) {
        IpUtil.initFromLocalFile(iplibPath);
        log.info("loading iplib: [%s]", iplibPath);
      }
    }

    final KafkaConsumer<ByteBuffer, ByteBuffer> consumer = new KafkaConsumer<>(
        consumerProps,
        new ByteBufferDeserializer(),
        new ByteBufferDeserializer()
    );
    consumer.subscribe(ImmutableList.of(feed));

    return new Firehose()
    {
      private Iterator<ByteBuffer> iter = null;
      private List<ByteBuffer> bufferList = new ArrayList<>();
      private AtomicBoolean isCommited = new AtomicBoolean(false);
      private CharBuffer chars = null;
      private ParseSpec parseSpec = firehoseParser.getParseSpec();
      private static final String BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
      private static final String DEFAULT_VALUE = "N/A";

      /**
       * Transform {@code byteBuffer} to String using the charset.<p>
       * Codes are copied from {@link StringInputRowParser} {@code buildStringKeyMap} funtion.
       *
       * @param byteBuffer  the {@link ByteBuffer} to be transformed
       * @return transformed string. return null if failed with {@link CoderResult}
       */
      @Nullable
      private String getString(ByteBuffer byteBuffer)
      {
        int payloadSize = byteBuffer.remaining();

        if (chars == null || chars.remaining() < payloadSize) {
          chars = CharBuffer.allocate(payloadSize);
        }

        final CoderResult coderResult = charset.newDecoder()
                                               .onMalformedInput(CodingErrorAction.REPLACE)
                                               .onUnmappableCharacter(CodingErrorAction.REPLACE)
                                               .decode(byteBuffer, chars, true);
        if (coderResult.isUnderflow()) {
          chars.flip();
          try {
            return chars.toString();
          }
          finally {
            chars.clear();
          }
        } else {
          // Failed with CoderResult
          return null;
        }
      }

      @Override
      public boolean hasMore()
      {
        if (isCommited.get()) {
          // do commit here
          consumer.commitAsync();
          log.info("offsets commit async.");
          isCommited.set(false);
        }
        try {
          if (iter == null || !iter.hasNext()) {
            bufferList.clear();

            // Always wait for results
            ConsumerRecords<ByteBuffer, ByteBuffer> records = consumer.poll(Long.MAX_VALUE);
            for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
              // For each ConsumerRecord, its value is the message we want
              if (rowDelimiter == null) {
                bufferList.add(record.value());
              } else {
                String recordString = getString(record.value());
                if (recordString != null) {
                  String[] split = StringUtils.split(recordString, rowDelimiter);
                  for (String eachRecordString : split) {
                    bufferList.add(ByteBuffer.wrap(eachRecordString.getBytes(charset)));
                  }
                }
              }
            }
            iter = bufferList.iterator();
          }
        }
        catch (Exception e) {
          /*
             If the process is killed, InterruptException will be thrown. It is good to return false.
             But it may not to commit sucessfully. Need a better solution.
            */
          log.error(e, "Polling message interrupted.");
          consumer.commitSync();
          return false;
        }
        return iter.hasNext();
      }

      private long base62Decode(String base62) throws IllegalArgumentException
      {
        StringBuilder sb = new StringBuilder(base62).reverse();
        long number = 0, base = 1;
        for (char c : sb.toString().toCharArray()) {
          int index = BASE62_ALPHABET.indexOf(c);
          if (-1 == index) {
            throw new IllegalArgumentException("Invalid base 62 string.");
          }
          number += index * base;
          base *= 62;
        }
        return number;
      }

      private InputRow parseAdMonitorRow(ByteBuffer byteBufferInput)
      {
        String str = getString(byteBufferInput);
        Map<String, Object> map = new LinkedHashMap<>();
        String[] split = StringUtils.split(str, '?');
        String logType = null;
        String tmpStr;
        if (split.length > 1) {
          str = split[split.length - 1];
          tmpStr = split[split.length - 2];
          if (tmpStr.endsWith("x.gif")) {
            logType = "imp";
          } else if (tmpStr.endsWith("r.gif")) {
            logType = "clk";
          }
        }
        if (logType == null) {
          throw new ParseException("Unparseable logType found!");
        }

        // 获取ip
        int i = str.lastIndexOf(" ");
        map.put("ip", str.substring(i + 1));
        str = str.substring(0, i);

        split = StringUtils.split(str, '^');
        for (String s : split) {
          String[] tmpSplit = StringUtils.split(s, '=');
          if (tmpSplit.length > 1) {
            map.put(tmpSplit[0], tmpSplit[1]);
          }
        }

        boolean isStable = !map.containsKey("e");
        if (!isStable) {
          String e = (String) map.get("e");
          if (e.contains("__") || e.endsWith("_m") ||
              (map.containsKey("av") && ((String) map.get("av")).startsWith("1"))) {
            isStable = true;
          }
        }

        tmpStr = (String) map.get("k");
        if (StringUtils.isBlank(tmpStr)) {
          throw new ParseException("Unparseable campaign found!");
        }
        map.put("campaign_id", tmpStr);
        tmpStr = (String) map.get("p");
        if (StringUtils.isBlank(tmpStr)) {
          throw new ParseException("Unparseable spid found!");
        }
        map.put("spot_id", tmpStr);
        try {
          map.put("spot_id_10", String.valueOf(base62Decode(tmpStr)));
        }
        catch (Exception e) {
          map.put("spot_id_10", DEFAULT_VALUE);
        }
        map.put("stable_imp", isStable ? ("imp".equals(logType) ? 1 : 0) : 0);
        map.put("stable_clk", isStable ? ("clk".equals(logType) ? 1 : 0) : 0);
        map.put("imp", "imp".equals(logType) ? 1 : 0);
        map.put("clk", "clk".equals(logType) ? 1 : 0);
        map.put("timestamp", map.get("smt"));
        IpUtil.IpSegment ipSegment = null;
        try {
          ipSegment = IpUtil.searchIp((String) map.get("ip"));
        }
        catch (Exception e) {
          log.info("Unparseable ip [%s]", map.get("ip"));
        }
        if (ipSegment == null) {
          map.put("region_id", DEFAULT_VALUE);
          map.put("region_city", DEFAULT_VALUE);
          map.put("region_province", DEFAULT_VALUE);
        } else {
          map.put("region_id", ipSegment.region);
          map.put("region_city", ipSegment.city);
          map.put("region_province", ipSegment.province);
        }

        // IES处理
        map.put("ies_id", map.getOrDefault("ni", DEFAULT_VALUE));

        // 假设此处解析完成
        final List<String> dimensions = parseSpec.getDimensionsSpec().hasCustomDimensions()
                                        ? parseSpec.getDimensionsSpec().getDimensionNames()
                                        : Lists.newArrayList(
                                            Sets.difference(
                                                map.keySet(),
                                                parseSpec.getDimensionsSpec()
                                                         .getDimensionExclusions()
                                            )
                                        );

        final DateTime timestamp;
        try {
          timestamp = parseSpec.getTimestampSpec().extractTimestamp(map);
          if (timestamp == null) {
            final String input = map.toString();
            throw new NullPointerException(
                io.druid.java.util.common.StringUtils.format(
                    "Null timestamp in input: %s",
                    input.length() < 100 ? input : input.substring(0, 100) + "..."
                )
            );
          }
        }
        catch (Exception e) {
          throw new ParseException(e, "Unparseable timestamp found!");
        }

        return new MapBasedInputRow(timestamp.getMillis(), dimensions, map);
      }

      @Override
      public InputRow nextRow()
      {
        if (isAdmonitorMode) {
          return parseAdMonitorRow(iter.next());
        } else {
          return theParser.parse(iter.next());
        }
      }

      @Override
      public Runnable commit()
      {
        return new Runnable()
        {
          @Override
          public void run()
          {
            /*
               KafkaConsumer is not thead safe, so we have to let the thread which polled messages do commit.
             */
            log.info("committing offsets");
            isCommited.set(true);
          }
        };
      }

      @Override
      public void close() throws IOException
      {
        consumer.close();
      }
    };
  }
}
