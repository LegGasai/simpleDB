package simpledb;

import org.junit.Test;
import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.*;
import simpledb.optimizer.IntHistogram;
import simpledb.optimizer.JoinOptimizer;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Jiang Yichen
 * @Date: 2024-01-27-11:53
 * @Description: Test for Customized
 */
public class LocalTest {
    public static void main(String[] args) {
        System.out.println((int)Math.floor(10.0/8));
    }


    @Test
    public void test1(){
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("test.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());

        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
    }



    @Test
    public void test2(){
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"field0", "field1", "field2"};

        TupleDesc td = new TupleDesc(types, names);

        // create the tables, associate them with the data files
        // and tell the catalog about the schema  the tables.
        HeapFile table1 = new HeapFile(new File("test1.dat"), td);
        Database.getCatalog().addTable(table1, "t1");

        HeapFile table2 = new HeapFile(new File("test2.dat"), td);
        Database.getCatalog().addTable(table2, "t2");

        // construct the query: we use two SeqScans, which spoonfeed
        // tuples via iterators into join
        TransactionId tid = new TransactionId();

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");
        // create a filter for the where condition
        Filter sf1 = new Filter(
                new Predicate(0,
                        Predicate.Op.GREATER_THAN, new IntField(1)), ss1);

        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);

        // and run it
        try {
            j.open();

            while (j.hasNext()) {
                Tuple tup = j.next();
                System.out.println(tup);
            }
            j.close();
            Database.getBufferPool().transactionComplete(tid);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void test3(){
        IntHistogram h = new IntHistogram(3,-10,0);
        for (int i = 0; i < 11; i++) {
            h.addValue(i-10);
        }
        System.out.println(h.toString());
        System.out.println(h.estimateSelectivity(Predicate.Op.GREATER_THAN,-1));
    }

    @Test
    public void test4(){

    }

    public double getCost(Integer key){
        HashMap<Integer, Double> integerIntegerHashMap = new HashMap<>();
        return integerIntegerHashMap.get(key);
    }

    @Test
    public void test5(){
        ConcurrentHashMap<Integer,Set<Field>> map = new ConcurrentHashMap<>();
        for (int i = 0; i < 6; i++) {
            for (int j = 0; j < 3; j++) {
                if (!map.containsKey(j)){
                    map.put(j,new HashSet<>());
                }
                Set<Field> s = map.get(j);
                s.add(new IntField(i%2));
                map.put(j,s);
            }
        }
        System.out.println(map);
        System.out.println(map.get(0).size());
        System.out.println(map.get(1).size());
        System.out.println(map.get(2).size());
    }

    @Test
    public void test6(){
        List<Integer> lst = Arrays.asList(1, 2, 3,4,5,6,7,8,9,10,11,12);
        Set<Set<Integer>> sets = new JoinOptimizer(null, null).enumerateSubsets(lst, 13);
        for (Set<Integer> set : sets) {
            System.out.println(set);
        }
        System.out.println(sets.size());
    }
}
