package cn.edu.thu;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.util.Random;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class InsertData {
    private static String ks = "replica";
    private static String nodes = "127.0.0.1";
    private static String outFile = "src/main/resources/out";
    private static int range = 1000;
    private static int lines = 500000;

    public static void main( String[] args ) throws Exception {
        String cf = "data";
        String createCf = "CREATE TABLE IF NOT EXISTS " + ks + "." + cf + " (" + "c1 int," + "c2 int,"
                + "c3 int, c4 int, c5 int, " + "PRIMARY KEY(c1,c2,c3)" + ");";

        CassandraCluster cluster = CassandraCluster.getInstance(nodes);
        cluster.dropKeyspace(ks);
        cluster.createKeyspace(ks, "SimpleStrategy", 1); // no USE is ok???
        Session session = cluster.getSession();
        session.execute(createCf);

        Random random = new Random(10);
        System.out.println("write");
        int i = 0;
        while (i < lines) {
            int c1 = 1;
            int c2 = random.nextInt(range);
            int c3 = random.nextInt(range);
            int c4 = random.nextInt(range);
            int c5 = random.nextInt(range);
            Statement statement = QueryBuilder.insertInto(ks, cf).value("c1", c1).
                    value("c2", c2).value("c3", c3).value("c4", c4).value("c5", c5);
            session.execute(statement);
            i++;
            if (i % 10000 == 0) {
                System.out.println("write: " + i / 10000 + "万");
            }
        }
        cluster.close();
    }
}

