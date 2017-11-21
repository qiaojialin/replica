package cn.edu.thu;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MDQuery {
    public static void main(String[] args) {
        List<String> replica1 = new ArrayList<String>();
        List<String> replica2 = new ArrayList<String>();
        List<String> replica3 = new ArrayList<String>();
        for(int i = 1; i<= 10; i++) {
            for(int j = 1; j <= 10; j++) {
                for(int k = 1; k <= 10; k++) {
                    replica1.add(i+","+j+","+k);
                    replica2.add(j+","+i+","+k);
                    replica3.add(k+","+j+","+i);
                }
            }
        }
        System.out.println(replica1);
        System.out.println(replica2);
        System.out.println(replica3);

        System.out.println();
        Collections.sort(replica1, new KeyComparator());
        Collections.sort(replica2, new KeyComparator());
        Collections.sort(replica3, new KeyComparator());

        System.out.println(replica1);
        System.out.println(replica2);
        System.out.println(replica3);

        // 1<=i<=2
        System.out.println(cost(query(replica1, 0, "1", "2")));
        System.out.println(cost(query(replica2, 1, "1", "2")));
        System.out.println(cost(query(replica3, 2, "1", "2")));
        System.out.println();

        // 1<=i<=1
        System.out.println(cost(query(replica1, 0, "1", "1")));
        System.out.println(cost(query(replica2, 1, "1", "1")));
        System.out.println(cost(query(replica3, 2, "1", "1")));
        System.out.println();

        // 3<=j<=5
        System.out.println(cost(query(replica1, 1,"3", "5")));
        System.out.println(cost(query(replica2, 0,"3", "5")));
        System.out.println(cost(query(replica3, 1,"3", "5")));
        System.out.println();

    }


    public static List<Integer> query(List<String> replica, int i, String start, String end) {
        List<Integer> results = new ArrayList<Integer>();
        for(String key: replica) {
            String[] keys = key.split(",");
            if(Integer.valueOf(keys[i]) >= Integer.valueOf(start) && Integer.valueOf(keys[i]) <= Integer.valueOf(end)) {
                results.add(replica.indexOf(key));
            }
        }
        return results;
    }


    public static int cost(List<Integer> results) {
        int cost=0;
        for(int i = 1; i < results.size(); i++) {
            cost += results.get(i) - results.get(i-1) - 1;
        }
        return cost;
    }

}

class KeyComparator implements Comparator<String> {

    public int compare(String o1, String o2) {
        String[] s1 = o1.split(",");
        String[] s2 = o2.split(",");
        for(int i = 0; i < s1.length; i++) {
            if(Integer.valueOf(s1[i]) < Integer.valueOf(s2[i])) {
                return -1;
            } else if (Integer.valueOf(s1[i]) > Integer.valueOf(s2[i])) {
                return 1;
            }
        }
        return 0;
    }
}