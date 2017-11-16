package cn.edu.thu;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class LoadData {

    private static String ks = "replica";
    private static String nodes = "127.0.0.1";
    private static String outFile = "src/main/resources/out";
    private static int range = 1000;
    private static int lines = 100000;

    public static void main( String[] args ) throws Exception {

        if(args.length == 1 && args[0].equals("help")) {
            System.out.println("5 args: sourceFolder outFolder device_num, data_size, nodes");
            System.out.println("write origin spatio-temporal data to d[device_num]_[data_size].data of cassandra cluster");
            System.out.println("columnFamily data has 4 column, device, time, lon, lat");
            return;
        }

        if(args.length == 2) {
            nodes = args[1];
            ks = args[0];
            outFile = args[0]+"_log";
        }


        List<String> list = Arrays.asList(args);

        writeAndPrint(String.valueOf(list), outFile);

        String cf = "data";
        String createCf = "CREATE TABLE IF NOT EXISTS "+ks + "."+ cf +" (" + "c1 int," + "c2 int,"
                + "c3 int, c4 int," + "PRIMARY KEY(c1,c2,c3,c4)" + ");";

        CassandraCluster cluster = CassandraCluster.getInstance(nodes);
        cluster.dropKeyspace(ks);
        cluster.createKeyspace(ks, "SimpleStrategy", 1);
        Session session = cluster.getSession();
        session.execute(createCf);

        Random random = new Random(10);

        int i = 0;
        while (i < lines) {
            int c1 = 1;
            int c2 = random.nextInt(range);
            int c3 = random.nextInt(range);
            int c4 = random.nextInt(range);
            Statement statement = QueryBuilder.insertInto(ks, cf).value("c1", c1).
                    value("c2", c2).value("c3", c3).value("c4", c4);
            session.execute(statement);
            i++;
            if(i % 10000 == 0) {
                System.out.println("write: " + i / 10000 + "万");
            }
        }

        cluster.close();
    }


    private static void writeAndPrint(String s, String path) {
        System.out.println(s);
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(path, true));
            writer.write(s + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
