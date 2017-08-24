import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.triggers.ITrigger;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;

public class InsertDataTrigger implements ITrigger {

    public Collection<Mutation> augment(Partition partition) {

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        System.out.println("Hello " + dateFormat.format(new Date()));
        System.out.println("This Insert Data Trigger");
        System.out.println("default charset " + Charset.defaultCharset());
        System.out.println("partition.toString()" + partition.toString());

        try {
            UnfilteredRowIterator it = partition.unfilteredIterator();
            CFMetaData cfMetaData = partition.metadata();

            System.out.println("PartitionKey " + new String(partition.partitionKey().getKey().array()));

            while (it.hasNext()) {
                Unfiltered un = it.next();

                Clustering clt = (Clustering) un.clustering();

                System.out.println("clt.toString(cfMetaData) " + clt.toString(cfMetaData));
                System.out.println("clt.getRawValues() " + new String(clt.getRawValues()[0].array()));
                System.out.println("partition.columns().toString() " + partition.columns().toString());
                Iterator<Cell> cls = partition.getRow(clt).cells().iterator();
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

                while (cls.hasNext()) {
                    Cell cell = cls.next();
                    System.out.println("cell.column().toSring() " + cell.column().toString());
                    System.out.println("cell.toString()" + cell.toString());
                    System.out.println("cell.value().getDouble() " + cell.value().getDouble());
                }
                System.out.println("un.toString()" + un.toString(cfMetaData));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return Collections.emptyList();

    }
}
