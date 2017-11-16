package cn.edu.thu;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CassandraCluster {
    private static Logger logger = LoggerFactory.getLogger(CassandraCluster.class);

    /**
     * !this method MUST be called at least once!
     */
    public static CassandraCluster getInstance(String nodes) throws Exception {
        if (myCluster == null)
            myCluster = new CassandraCluster(nodes);
        return myCluster;
    }

    private static CassandraCluster myCluster;

    private String[] nodes;
    // one cluster for per physical
    private Cluster cluster;
    // one session for per ks
    private Session session;

    private static String dropKsCql= "DROP KEYSPACE IF EXISTS %s;";
    private static String createKsCql = "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'%s', 'replication_factor':%d};";
    private static String createCfCql = "CREATE TABLE IF NOT EXISTS %s.%s (" + "equip text," + "time bigint,"
            + "value blob," + "PRIMARY KEY(equip,time)" + ");";
    static private String createGeoCf = "CREATE TABLE IF NOT EXISTS %s.%s (" + "device text," + "time bigint,"
            + "lon float, lat float," + "PRIMARY KEY(device,time)" + ");";
    private static String deleteCql = "delete value from %s.%s where equip='%s' and time=%d;";
    private static String deleteSTIndexCql = "delete from %s.%s where rowkey=%d and geohash=%d;";

    private static String selectCql = "select * from %s.%s where equip='%s' and time=%d;";
    private static String selectRangeCql = "select * from %s.%s where equip='%s' and time >= %d and time <= %d;";
    private static String selectLatestCql = "select * from %s.%s where equip='%s' order by time desc limit %d;";
    private static String selectCountCql = "select count(*) from %s.%s where equip ='%s' and time > %d and time <= %d;";
    private static String selectEquipCql = "select distinct equip from %s.%s where token(equip) >= %d and token(equip) <= %d;";
    private static String selectMultiCql = "SELECT * FROM %s.%s WHERE equip='%s' AND time IN ";
    private static String selectNearestBeforeCql = "select * from %s.%s where equip='%s' and time <= %d order by time desc limit %d;";
    private static String selectNearestAfterCql = "select * from %s.%s where equip='%s' and time >= %d order by time asc limit %d;";

    private static int confConsistency = 0;

    private CassandraCluster(String nodes) throws Exception {
        this.nodes = nodes.split(",");
        connectCluster(this.nodes);
    }

    public Session getSession() {
        return session;
    }

    public void connectCluster(String[] nodes) throws Exception {
        if (nodes == null || nodes.length == 0)
            return;
        List<InetAddress> addresses = new ArrayList<InetAddress>();
        for (String node : nodes) {
            try {
                addresses.add(InetAddress.getByName(node));
            } catch (UnknownHostException e) {
                logger.error(e.getMessage());
                throw e;
            }
        }
        cluster = Cluster.builder().addContactPoints(addresses)
                //集群需要用户名密码
                //.withCredentials("cassandra", "123456")
                .build();
        logger.info("Try connectCluster cassandra");
        try {
            Metadata metadata = cluster.getMetadata();
            logger.info("Connected to cluster");
            for (Host host : metadata.getAllHosts()) {
                logger.info("Datatacenter: {}; Host: {}; Rack: {}", host.getDatacenter(),
                        host.getAddress().toString(), host.getRack());
            }
            session = cluster.connect();
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public void close() {
        session.close();
        cluster.close();
    }

    public void dropKeyspace(String ks) throws Exception {
        try {
            session.execute(String.format(dropKsCql, ks));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    public void createKeyspace(String ks, String strategy, int replication) throws Exception {
        try {
            session.execute(String.format(createKsCql, ks, strategy, replication));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }


    public void createColumnfamily(String ks, String cf) throws Exception {
        try {
            session.execute(String.format(createCfCql, ks, cf));
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public ResultSet insertData(String ks, String cf, String device, long time, ByteBuffer value) {
        Statement statement = QueryBuilder.insertInto(ks, cf).value("equip", device)
                .value("time", time).value("value", value);
        return session.execute(statement);
    }

    public ResultSet insertDigest(String ks, String cf, String equip, long time, ByteBuffer value) {
        Statement statement = QueryBuilder.insertInto(ks, cf).value("equip", equip)
                .value("time", time).value("value", value);
        return session.execute(statement);
    }


    /**
     *
     * @return
     */
    public ResultSet insertGeoData(String ks, String cf, String device, long time, float lon, float lat) {
        Statement statement = QueryBuilder.insertInto(ks, cf).value("device", device).value("time", time)
                .value("lon", lon).value("lat", lat);
        return session.execute(statement);
    }


    public ResultSet insertSTIndex(String ks, String cf, int rowkey, long geohash, ByteBuffer value) {
        Statement statement = QueryBuilder
                .insertInto(ks, cf)
                .value("rowkey", rowkey)
                .value("geohash", geohash)
                .value("value", value);
        statement = statement.setConsistencyLevel(getConsistency(confConsistency));
        return session.execute(statement);
    }

    public ResultSet select(String ks, String cf, String equip, long time) {
        ResultSet rSet;

        logger.info(String.format(selectCql, ks, cf, equip, time));
        rSet = session.execute(String.format(selectCql, ks, cf, equip, time));

        return rSet;
    }

    public ResultSet selectSTIndex(String ks, String cf, int rowkey, long geohash) {
        ResultSet rSet;
        Statement statement = QueryBuilder
                .select()
                .all()
                .from(ks, cf)
                .where(QueryBuilder.eq("rowkey", rowkey))
                .and(QueryBuilder.eq("geohash", geohash));
        //statement.setConsistencyLevel(getConsistency(confConsistency));
        rSet = session.execute(statement);
        return rSet;
    }

    public ResultSet selectRange(String ks, String cf, String equip, long startTime, long endTime) {
        ResultSet rSet = null;

        logger.info(String.format(selectRangeCql, ks, cf, equip, startTime, endTime));
        rSet = session.execute(String.format(selectRangeCql, ks, cf, equip, startTime, endTime));

        return rSet;
    }

    public ResultSet selectMulti(String ks, String cf, String equip, Long[] timestamps) {
        ResultSet rSet;

        StringBuilder stringBuilder = new StringBuilder(String.format(selectMultiCql, ks, cf, equip));
        stringBuilder.append("(");
        for (int i = 0; i < timestamps.length - 1; i++) {
            stringBuilder.append(timestamps[i]);
            stringBuilder.append(",");
        }
        stringBuilder.append(timestamps[timestamps.length - 1] + ")");

        logger.info(String.format(stringBuilder.toString(), ks, cf, equip));
        rSet = session.execute(String.format(stringBuilder.toString(), ks, cf, equip));

        return rSet;
    }

    public ResultSet selectNearestBefore(String ks, String cf, String equip, long timestamp, int n) {
        ResultSet rSet;

        logger.debug(String.format(selectNearestBeforeCql, ks, cf, equip, timestamp, n));
        rSet = session.execute(String.format(selectNearestBeforeCql, ks, cf, equip, timestamp, n));

        return rSet;
    }

    public ResultSet selectNearestAfter(String ks, String cf, String equip, long timestamp, int n) {
        ResultSet rSet;

        logger.info(String.format(selectNearestAfterCql, ks, cf, equip, timestamp, n));
        rSet = session.execute(String.format(selectNearestAfterCql, ks, cf, equip, timestamp, n));

        return rSet;
    }

    public ResultSet selectLatestN(String ks, String cf, String equip, int n) {
        ResultSet rSet;

        logger.info(String.format(selectLatestCql, ks, cf, equip, n));
        rSet = session.execute(String.format(selectLatestCql, ks, cf, equip, n));

        return rSet;
    }


    public boolean checkKs(String ks) {
        KeyspaceMetadata ksm = cluster.getMetadata().getKeyspace(ks);
        if (ksm == null)
            return false;
        return true;
    }

    public boolean checkCf(String ks, String cf) {
        KeyspaceMetadata ksm = cluster.getMetadata().getKeyspace(ks);
        if (ksm == null)
            return false;

        TableMetadata tm = ksm.getTable(cf);
        if (tm == null)
            return false;

        return true;
    }

    public ResultSet selectCount(String ks, String cf, String equip, long startTime, long endTime) {
        ResultSet rSet = null;

        logger.debug(String.format(selectCountCql, ks, cf, equip, startTime, endTime));
        rSet = session.execute(String.format(selectCountCql, ks, cf, equip, startTime, endTime));

        return rSet;
    }

    public ResultSet deleteData(String ks, String cf, String equip, String time) {

        logger.debug(String.format(deleteCql, ks, cf, equip, time));
        ResultSet rSet = session.execute(String.format(deleteCql, ks, cf, equip, time));

        return rSet;
    }

    public ResultSet deleteSTIndex(String ks, String cf, int rowkey, long geohash) {

        logger.debug(String.format(deleteSTIndexCql, ks, cf, rowkey, geohash));
        ResultSet rSet = session.execute(String.format(deleteSTIndexCql, ks, cf, rowkey, geohash));
        return rSet;
    }


    public ConsistencyLevel getConsistency(int confConsistency){
        if(confConsistency == 0){
            return ConsistencyLevel.ONE;
        }else{
            return ConsistencyLevel.ALL;
        }
    }

    public static void main(String[] args) throws Exception {
        CassandraCluster cluster = CassandraCluster.getInstance("127.0.0.1");
        cluster.dropKeyspace("myks");
        cluster.createKeyspace("myks", "SimpleStrategy", 1);
        cluster.createColumnfamily("myks", "myColumnFamily");
        cluster.close();
    }
}
