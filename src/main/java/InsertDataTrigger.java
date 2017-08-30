import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;

public class InsertDataTrigger implements ITrigger {

    public Collection<Mutation> augment(Partition update) {
        //checking if trigger works and some debug info;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        System.out.println("Hello " + dateFormat.format(new Date()));
        System.out.println("This Insert Data Trigger");

        try {
            UnfilteredRowIterator it = update.unfilteredIterator();
            CFMetaData cfMetaData = update.metadata();

            System.out.println("PartitionKey " + new String(update.partitionKey().getKey().array()));
            System.out.println("update.metadata().clusteringColumns().toString() " + update.metadata().clusteringColumns().toString());

            while (it.hasNext()) {
                Unfiltered un = it.next();

                switch (un.kind()) {    //checking if it's operation per row, other would be range deletion and in this case we won't send info to kafka

                    case ROW:
                        JSONObject message = new JSONObject();

                        Clustering clt = (Clustering) un.clustering();

                        message.put("channelId", new String(update.partitionKey().getKey().array()));

                        System.out.println("clt.toString(cfMetaData) " + clt.toString(cfMetaData));
                        System.out.println("clt.getRawValues() " + new String(clt.getRawValues()[0].array()));
                        System.out.println("update.columns().toString() " + update.columns().toString());

                        message.put("timestamp", new String(clt.getRawValues()[0].array())); //getting clustering colums of primary key in this case it's only one column timestamp

                        Row row = update.getRow(clt);
                        if(row.deletion().isLive()) {   //checking if it is not row deletion
                            //Iterator<Cell> cells = update.getRow(clt).cells().iterator();
                            Iterator<Cell> cells = row.cells().iterator();

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
                                if (!cell.isTombstone()) {
                                    System.out.println("cell.column().name.toString() " + cell.column().name.toString());
                                    //System.out.println("cell.toString()" + cell.toString());
                                    ByteBuffer cellValue = ByteBufferUtil.clone(cell.value());
                                    Float value = cellValue.getFloat();
                                    System.out.println("copied cellValue " + value);
                                    message.put(cell.column().name.toString(), value);
                                }
                                else
                                    message.clear();    //clear message, if cell was deleted we don't want to inform Kafka about that change at least for now
                            }
                            //System.out.println("un.toString(cfMetaData)" + un.toString(cfMetaData));

                            if (!message.isEmpty()) {
                                System.out.println(message.toString());
                                Properties props = loadProperties();
                                Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
                                producer.send(new ProducerRecord<>(props.getProperty("topic"), message.toString()));
                                producer.close();
                            }
                        }
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("End of trigger");
        return Collections.emptyList();
    }


    private static Properties loadProperties() {
        Properties properties = new Properties();
        InputStream stream = InsertDataTrigger.class.getClassLoader().getResourceAsStream("InsertDataTrigger.properties");
        try {
            properties.load(stream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            FileUtils.closeQuietly(stream);
        }
        return properties;
    }
}
