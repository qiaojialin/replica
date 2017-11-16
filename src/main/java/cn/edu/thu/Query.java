package cn.edu.thu;

/**
 * Created by qiaojialin on 2017/11/4.
 */
public class Query {

    private static String ks = "replica";
    private static String nodes = "127.0.0.1";
    private static String selectCql = "select * from %s.%s where c1=1 and c2>=%d and c2<=%d;";

    public static void main(String[] args) throws Exception {
        CassandraCluster cluster = CassandraCluster.getInstance(nodes);

    }
}
