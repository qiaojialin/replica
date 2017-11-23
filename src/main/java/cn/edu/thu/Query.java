package cn.edu.thu;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster;
import java.util.Random;

import java.util.concurrent.TimeUnit;

/**
 * Created by RuiLei on 2017/11/23.
 */
public class Query {

    private static String ks = "replica";
    private static String node = "127.0.0.1";
    private static String selectCql = "select * from %s.%s where c1=1 and c2>=%d and c2<=%d;";
    private static int range = 1000;
    private static String cf = "data"; // work with InsertData

    public static void main(String[] args) throws Exception {
        //CassandraCluster cluster = CassandraCluster.getInstance(nodes);
        Cluster cluster = Cluster.builder().addContactPoint(node).build();
        Session session = cluster.connect(ks);

        //Random random = new Random(10);
        //int qnum = random.nextInt(range);
        int qnum = 111;
        TimeWatch watch = TimeWatch.start();
        String selectRangeCql1 = "select COUNT(*) AS total from " + ks + "." + cf+" WHERE c1=1 AND c2>"+qnum+"  ALLOW FILTERING;";
        ResultSet rs1 = session.execute(selectRangeCql1);
        System.out.println("Elapsed Time custom format: " + watch.toMinuteSeconds());
        System.out.println("Elapsed Time in seconds: " + watch.time(TimeUnit.SECONDS));
        System.out.println("Elapsed Time in nano seconds: " + watch.time());
        Row r1 = rs1.one();
        System.out.println(r1.getLong("total"));

        watch = TimeWatch.start();
        String selectRangeCql2 = "select * from " + ks + "." + cf+" WHERE c1=1 AND c2>"+qnum+"  ALLOW FILTERING;";
        ResultSet rs2 = session.execute(selectRangeCql2);
        System.out.println("Elapsed Time custom format: " + watch.toMinuteSeconds());
        System.out.println("Elapsed Time in seconds: " + watch.time(TimeUnit.SECONDS));
        System.out.println("Elapsed Time in nano seconds: " + watch.time());

        /*
        for (Row row : results) {
            System.out.format("%d %d %d %d %d\n", row.getInt("c1"),
                    row.getInt("c2"), row.getInt("c3"),
                    row.getInt("c4"), row.getInt("c5"));
        }
        */
        session.close();
        cluster.close();
    }
}
