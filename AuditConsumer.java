//package com.mapr.examples;
//import com.mapr.fs;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.ojai.Document;
import org.ojai.types.ODate;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;

import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import com.mapr.fs.MapRFileSystem;
import com.mapr.fs.*;

import com.google.common.io.Resources;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.*;
import java.io.InputStream;
import java.util.regex.*;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.ParseException;

public class AuditConsumer {
  // Set the stream and topic to read from.
  private static final String MAPRFS_URI = "maprfs:///";
  private static KafkaConsumer<String, String> consumer;
  private static final Connection connection = DriverManager.getConnection("ojai:mapr:");
  private static DocumentStore store;
  private static Random random;
  private static String cluster;
  private static String db_path;
  private static int debug;


  public static void main(String[] args) throws IOException,InterruptedException {

    Options options = new Options();
    Option clusterOption = OptionBuilder
      .hasArg(true)
      .withArgName("cluster")
      .isRequired(true)
      .withDescription("cluster name")
      .withLongOpt("cluster")
      .create("c");

    Option debugOption = OptionBuilder
      .hasArg(true)
      .withArgName("debug")
      .isRequired(true)
      .withDescription("debug")
      .withLongOpt("debug")
      .create("d");

    Option dbPathOption = OptionBuilder
      .hasArg(true)
      .withArgName("output_db_path")
      .isRequired(true)
      .withDescription("output db path")
      .withLongOpt("output_db_path")
      .create("o");

    options.addOption(clusterOption);
    options.addOption(debugOption);
    options.addOption(dbPathOption);

    CommandLineParser parser = new BasicParser();
    CommandLine commandLine = null;
    try {
        commandLine = parser.parse(options, args);
    } catch (ParseException e) {
        System.err.println("AuditConsumer -cluster <cluster name> -output_db_path <output db path> -debug <0/1>");
        System.exit(1);
    }

    cluster = commandLine.getOptionValue("c");
    db_path = commandLine.getOptionValue("o");
    debug = Integer.parseInt(commandLine.getOptionValue("d"));

    Runtime.getRuntime().addShutdownHook(new Thread()
    {
      @Override
      public void run()
      {
        System.out.println("Shutdown hook triggered!");
        consumer.close();
        System.out.println("All done.");
      }
    });

    if (!MapRDB.tableExists(db_path)) {
      MapRDB.createTable(db_path); // Create the table if not already present
    }
    store = connection.getStore(db_path);
    random = new Random();
    try (InputStream props = Resources.getResource("consumer.props").openStream()) {
      Properties properties = new Properties();
      properties.load(props);
      if (properties.getProperty("group.id") == null) {
        properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
      }

      consumer = new KafkaConsumer<>(properties);
    }

    Configuration conf = new Configuration();
    String uri = MAPRFS_URI;
    uri = uri + "mapr/";
    conf.set("fs.default.name", uri);
    MapRFileSystem fs = new MapRFileSystem();
    fs.initialize(URI.create(uri), conf, true);
    Pattern pattern = Pattern.compile("/var/mapr/auditstream/auditlogstream:" + cluster +".+" );
    consumer.subscribe(pattern,null);

    int pollTimeout = 1000;
    while (true) {
      // Request unread messages from the topic.
      ConsumerRecords<String, String> consumerRecords = consumer.poll(pollTimeout);
      Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
      if (iterator.hasNext()) {
        while (iterator.hasNext()) {
          ConsumerRecord<String, String> record = iterator.next();
          // Iterate through returned records, extract the value
          // of each message, and print the value to standard output.
          String value = record.value();
          String rvalue = value.replace("\"","");
          processRecord(fs, rvalue, value);
        }
      } else {
    Thread.sleep(1000);
      }
    }
  }

  public static String processRecord(MapRFileSystem fs, String rvalue, String value) 
  {
     StringTokenizer st = new StringTokenizer(rvalue, ",");
     String lfidPath = "";
     String lvolName = "";
     Document document = connection.newDocument();

     while (st.hasMoreTokens())
     {
       String field = st.nextToken();
       StringTokenizer st1 = new StringTokenizer(field, ":");
       while (st1.hasMoreTokens())
       {
         String token = st1.nextToken();

         if (token.endsWith("timestamp")) {
           st1.nextToken(); //date type
           String timestamp_string = st1.nextToken() + ":" + st1.nextToken() + ":" + st1.nextToken().replace("}", "");
           String random_id = String.valueOf(random.nextInt(1000));
           document.set("_id", timestamp_string + "-" + random_id);
         }

         else if (token.endsWith("Fid")) {
           String lfidStr = st1.nextToken();
           String path= null;
           try {
               path = fs.getMountPathFid(lfidStr);
           } catch (IOException e){
           }
           lfidPath = "\"FidPath\":\""+path+"\",";
           if (path != null) {
             document.set(token+"Path", path);
           }
         }

         else if (token.endsWith("volumeId")) {
           String volid = st1.nextToken();
           String name= null;
           try {
             int volumeId = Integer.parseInt(volid);
               name = fs.getVolumeName(volumeId);
             }
           catch (IOException e){
           }
           lvolName = "\"VolumeName\":\""+name+"\",";
           if (name != null) {
             document.set("VolumeName", name);
           }
         } else {
           document.set(token, st1.nextToken().replace("}", ""));
         }
       }
     }
     String result = "";
     StringTokenizer st2 = new StringTokenizer(value,",");
     while (st2.hasMoreTokens()) {
       String tokens = st2.nextToken();
       result = result + tokens + ",";
       if (tokens.contains("Fid")) {
         result = result + lfidPath;
       }
       if (tokens.contains("volumeId")) {
         result = result + lvolName;
       }
     }

     if (debug == 1) {
       System.out.println(result.substring(0, result.length() - 1));
     }
     store.insertOrReplace(document);
     store.flush();
     return result.substring(0, result.length() - 1);
  }
}
