package cn.edu.thu;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MDQuery {
    public static void main(String[] args) {
        List<Integer> replica1 = new ArrayList<Integer>();
        List<Integer> replica2 = new ArrayList<Integer>();
        List<Integer> replica3 = new ArrayList<Integer>();
        for(int i = 0; i< 10; i++) {
            for(int j = 0; j < 10; j++) {
                for(int k = 0; k < 10; k++) {
                    replica1.add(Integer.valueOf(i+""+j+""+k));
                    replica2.add(Integer.valueOf(j+""+i+""+k));
                    replica3.add(Integer.valueOf(k+""+j+""+i));
                }
            }
        }
        Collections.sort(replica1);

        // 1<=i<=2
        System.out.println(cost(query(replica1, 100, 299)));
        System.out.println(cost(query(replica2, 100, 299)));
        System.out.println(cost(query(replica3, 100, 299)));
        System.out.println();

        // 1<=i<=1
        System.out.println(cost(query(replica1, 100, 199)));
        System.out.println(cost(query(replica2, 100, 199)));
        System.out.println(cost(query(replica3, 100, 199)));
        System.out.println();

        // i=1,j=2,3<=c<=5
        System.out.println(cost(query(replica1, 123, 125)));
        System.out.println(cost(query(replica2, 123, 125)));
        System.out.println(cost(query(replica3, 123, 125)));
        System.out.println();

    }


    public static List<Integer> query(List<Integer> replica, int start, int end) {
        List<Integer> results = new ArrayList<Integer>();
        for(int key: replica) {
            if(key >= start && key <= end) {
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
