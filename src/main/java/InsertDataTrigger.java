import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

public class InsertDataTrigger implements ITrigger {

    public Collection<Mutation> augment(Partition update) {

        //checking if trigger works and some debug info;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        System.out.println("Hello " + dateFormat.format(new Date()));
        System.out.println("This Insert Data Trigger");
        System.out.println("default charset " + Charset.defaultCharset());      //IMPORTANT check if it's important

        //here we're gonna build the message to kafka based on inserted data
        try {
            UnfilteredRowIterator it = update.unfilteredIterator();
            CFMetaData cfMetaData = update.metadata();

            System.out.println("PartitionKey " + new String(update.partitionKey().getKey().array()));
            System.out.println("update.metadata().clusteringColumns().toString() " + update.metadata().clusteringColumns().toString());

            while (it.hasNext()) {
                JSONObject message = new JSONObject();

                Unfiltered un = it.next();
                Clustering clt = (Clustering) un.clustering();

                message.put("partitionkey", new String(update.partitionKey().getKey().array()));

                System.out.println("clt.toString(cfMetaData) " + clt.toString(cfMetaData));
                System.out.println("clt.getRawValues() " + new String(clt.getRawValues()[0].array()));
                System.out.println("partition.columns().toString() " + update.columns().toString());

                message.put("datetime", new String(clt.getRawValues()[0].array()));

                Iterator<Cell> cells = update.getRow(clt).cells().iterator();

//                Iterator<ColumnDefinition> columnDefinitions = partition.getRow(clt).columns().iterator();
//                while(columnDefinitions.hasNext()){
//                    ColumnDefinition cDef = columnDefinitions.next();
//
//                    System.out.println("cDef.cfName " +  cDef.cfName);
//                    System.out.println("cDef.ksName " +  cDef.ksName);
//                    System.out.println("cDef.kind.toString() " +  cDef.kind.toString());
//                    System.out.println("cDef.name.toString() " +  cDef.name.toString());
//                    System.out.println("cDef.toString() " + cDef.toString());
//                }

                while (cells.hasNext()) {
                    Cell cell = cells.next();
                    System.out.println("cell.column().name.toString() " + cell.column().name.toString());
                    System.out.println("cell.toString()" + cell.toString());
                    Double x = cell.value().getDouble();
                    System.out.println("cell.value().getDouble() " + x);
                    //if(cell.column().name.toString() == "value")
                    System.out.println(x);
                    message.put(cell.column().name.toString(), x);
                    //else
                    //   message.put(cell.column().name.toString(),cell.value().toString());
                }
                System.out.println("un.toString()" + un.toString(cfMetaData));

                if (!message.isEmpty()) {
                    System.out.println(message.toString());

                    //Sending data to kafka
                    Properties props = new Properties();
                    props.put("bootstrap.servers", "localhost:9092");
                    props.put("acks", "all");
                    props.put("retries", 0);
                    props.put("batch.size", 16384);
                    props.put("linger.ms", 1);
                    props.put("buffer.memory", 33554432);
                    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                    Producer<String, String> producer = new KafkaProducer<>(props);
                    producer.send(new ProducerRecord<>("test", message.toString()));//move topic name to some properties
                    producer.close();
                }


            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("End of trigger");
        return Collections.emptyList();
    }
}
