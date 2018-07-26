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

import com.google.common.collect.Lists;
import io.druid.java.util.common.logger.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 用于处理IP库相关的代码。
 *
 * @author 刘燊
 * @since 2017/12/11
 */
public class IpUtil
{
  private static final Logger log = new Logger(KafkaFirehoseFactory.class);
  /**
   * 默认的IP库HDFS目录
   */
  private static final String IP_LIB_DIR = "/DMP/input/iplib";

  /**
   * IP段数据。
   */
  public static class IpSegment
  {
    /**
     * IP段头
     */
    public Long start;

    /**
     * IP段尾
     */
    public Long end;

    /**
     * region id
     */
    public Integer region;

    /**
     * IP段数据中的省份及以上的信息
     */
    public String province;

    /**
     * IP段数据中的城市信息
     */
    public String city;

    public IpSegment(Long start, Long end, Integer region, String province, String city)
    {
      this.start = start;
      this.end = end;
      this.region = region;
      this.province = province;
      this.city = city;
    }

    @Override
    public String toString()
    {
      StringBuilder sb = new StringBuilder(128);
      sb.append(start).append(",").append(end)
        .append(",").append(region).append(",").append(province).append(",").append(city);
      return sb.toString();
    }
  }

  private static final List<IpSegment> IP_DATA = new ArrayList<>();

  /**
   * 无参初始化函数，用于从{@code IP_LIB_DIR}中读取最新文件作为IP库文件加载。
   *
   * @throws IOException 读取文件错误则抛异常
   */
  public static void initFromIpLibHdfsFile() throws IOException
  {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    Path ipLibFilePath = null;
    long newestTime = -1;
    for (FileStatus fileStatus : fs.listStatus(new Path(IP_LIB_DIR))) {
      if (fileStatus.getModificationTime() > newestTime && fileStatus.isFile() && !fileStatus.getPath()
                                                                                             .getName()
                                                                                             .endsWith("_")) {
        ipLibFilePath = fileStatus.getPath();
      }
    }
    if (ipLibFilePath == null) {
      throw new IOException("No available ip lib file.");
    }
    initFromHdfsFile(conf, ipLibFilePath.toString());
  }

  /**
   * 从HDFS中加载IP库文件
   *
   * @param file HDFS文件路径
   */
  public static void initFromHdfsFile(Configuration conf, String file) throws IOException
  {
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        FileSystem.get(conf).open(new Path(file)),
        StandardCharsets.UTF_8
    ));
    initFromFile(reader);
    reader.close();
  }

  /**
   * 从BufferedReader中加载IP库文件
   *
   * @param reader 数据源reader
   */
  private static void initFromFile(BufferedReader reader) throws IOException
  {
    IP_DATA.clear();
    String tmp;
    String[] items;
    while ((tmp = reader.readLine()) != null) {
      tmp = tmp.trim();
      // 每行格式为{start_ip},{end_ip},{region_id},{province},{city}，其中city部分可能不存在
      if (tmp.contains(",")) {
        items = tmp.split(",");
        IP_DATA.add(new IpSegment(Long.parseLong(items[0]), Long.parseLong(items[1]),
                                  Integer.parseInt(items[2]),
                                  items.length > 3 ? items[3] : "",
                                  items.length == 5 ? items[4] : ""
        ));
      }
    }

    IP_DATA.sort((o1, o2) -> o1.start.compareTo(o2.start));
    log.info("after loading iplib, ip_data size is [%d]", IP_DATA.size());
  }

  /**
   * 从本地文件系统中加载IP库文件
   *
   * @param file 本地文件路径
   */
  public static void initFromLocalFile(String file) throws IOException
  {
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        new FileInputStream(file),
        StandardCharsets.UTF_8
    ));
    initFromFile(reader);
    reader.close();
  }

  /**
   * 尝试查找对应字符串IP在IP库中的值。<p>
   * Note: 如果此时{@link IpUtil}还没有进行过初始化操作，则在{@code getIpData}函数中会默认使用单例模式，
   * 调用{@code initFromIpLibHdfsFile}方法进行初始化。
   *
   * @param ip 需要查找的IP字符串
   *
   * @return 如果找到了，则返回对应的{@link IpSegment}；否则返回null。
   */
  public static IpSegment searchIp(String ip)
  {
    List<IpSegment> ipData = getIpData();

    int start = 0;
    int end = ipData.size() - 1;
    int index;
    long ipValue = ip2long(ip);

    while (end >= start) {
      index = (end + start) / 2;
      if (ipValue < ipData.get(index).start) {
        end = index - 1;
      } else if (ipValue > ipData.get(index).end) {
        start = index + 1;
      } else { // 找到了
        return ipData.get(index);
      }
    }
    return null;
  }

  /**
   * 单例获取{@code IP_DATA}数据，如果默认没有数据，则会尝试使用默认方法加载数据。
   *
   * @return ipSegment列表
   */
  private static List<IpSegment> getIpData()
  {
    if (!IP_DATA.isEmpty()) {
      return IP_DATA;
    }

    synchronized (IP_DATA) {
      if (IP_DATA.isEmpty()) {
        try {
          initFromIpLibHdfsFile();
        }
        catch (IOException e) {
          log.error("error while reading iplib from hdfs");
        }
      }
    }
    return IP_DATA;
  }

  /**
   * ip转long。
   *
   * @param ipStr 待转换ip地址字符串
   *
   * @return 转换结果
   */
  public static long ip2long(String ipStr)
  {
    long result = 0;
    for (String ipSegment : ipStr.split("\\.")) {
      result = (result << 8) + Integer.parseInt(ipSegment);
    }
    return result;
  }

  /**
   * long转ip。
   *
   * @param ip 待转换ip
   *
   * @return 转换结果
   */
  public static String long2ip(long ip)
  {
    List<String> s = new ArrayList<>();
    for (int i = 0; i < 4; ++i) {
      s.add(String.valueOf(ip % 256));
      ip /= 256;
    }
    return StringUtils.join(Lists.reverse(s), ".");
  }
}
