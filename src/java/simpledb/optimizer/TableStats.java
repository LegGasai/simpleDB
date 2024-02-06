package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private final int tableId;
    private final int ioCostPerPage;
    private final int pagesNum;
    private final int tuplesNum;
    private final TupleDesc td;
    private final HeapFile dbFile;
    private final ConcurrentHashMap<Integer,IntHistogram> intHistMap;
    private final ConcurrentHashMap<Integer,StringHistogram> strHistMap;
    private final ConcurrentHashMap<Integer,Integer> minMap;
    private final ConcurrentHashMap<Integer,Integer> maxMap;
    private final ConcurrentHashMap<Integer,Set<Field>> distinctMap;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.dbFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.pagesNum = this.dbFile.numPages();
        this.td = dbFile.getTupleDesc();
        this.intHistMap = new ConcurrentHashMap<>();
        this.strHistMap = new ConcurrentHashMap<>();
        this.minMap = new ConcurrentHashMap<>();
        this.maxMap = new ConcurrentHashMap<>();
        this.distinctMap = new ConcurrentHashMap<>();
        this.tuplesNum = this.generateHistogram();

    }

    private int generateHistogram(){
        int totalTuples = 0;
        Transaction tx = new Transaction();
        tx.start();
        DbFileIterator it = dbFile.iterator(tx.getId());
        try {
            it.open();
            while (it.hasNext()){
                Tuple tuple = it.next();
                totalTuples++;
                for (int i = 0; i < this.td.numFields(); i++) {
                    Field field = tuple.getField(i);
                    Type type = field.getType();
                    if (!distinctMap.containsKey(i)){
                        distinctMap.put(i,new HashSet<>());
                    }
                    Set<Field> fieldSet = distinctMap.get(i);
                    fieldSet.add(field);
                    distinctMap.put(i,fieldSet);
                    if (type.equals(Type.INT_TYPE)){
                        // set max/min vale
                        minMap.put(i,Math.min(minMap.getOrDefault(i,Integer.MAX_VALUE),((IntField)field).getValue()));
                        maxMap.put(i,Math.max(maxMap.getOrDefault(i,Integer.MIN_VALUE),((IntField)field).getValue()));
                    }else if (type.equals((Type.STRING_TYPE))){
                        if (!strHistMap.containsKey(i)){
                            strHistMap.put(i,new StringHistogram(NUM_HIST_BINS));
                        }
                    }
                }
            }
            for (int i = 0; i < this.td.numFields(); i++) {
                if (minMap.containsKey(i)){
                    this.intHistMap.put(i,new IntHistogram(NUM_HIST_BINS,minMap.get(i),maxMap.get(i)));
                }
            }
            it.rewind();
            while (it.hasNext()){
                Tuple tuple = it.next();
                for (int i = 0; i < this.td.numFields(); i++) {
                    Field field = tuple.getField(i);
                    Type type = field.getType();
                    if (type.equals(Type.INT_TYPE)){
                        IntHistogram intHistogram = intHistMap.get(i);
                        intHistogram.addValue(((IntField)field).getValue());
                    }else if (type.equals(Type.STRING_TYPE)){
                        StringHistogram strHistogram = strHistMap.get(i);
                        strHistogram.addValue(((StringField)field).getValue());
                    }
                }
            }
        }catch (Exception e){
            System.out.println("generateHistogram error!");
            e.printStackTrace();
        }finally {
            it.close();
            try {
                tx.commit();
            } catch (IOException e) {
                System.out.println("transaction commit error!");
                e.printStackTrace();
            }
        }
        return totalTuples;
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.pagesNum * ioCostPerPage * 2.0;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)(selectivityFactor*tuplesNum);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        Type type = this.td.getFieldType(field);
        if (type.equals(Type.INT_TYPE)){
            return this.intHistMap.get(field).avgSelectivity();
        }else if (type.equals(Type.STRING_TYPE)){
            return this.strHistMap.get(field).avgSelectivity();
        }
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Type type = constant.getType();
        if (type.equals(Type.INT_TYPE)){
            return this.intHistMap.get(field).estimateSelectivity(op,((IntField)constant).getValue());
        }else if (type.equals(Type.STRING_TYPE)){
            return this.strHistMap.get(field).estimateSelectivity(op,((StringField)constant).getValue());
        }
        return 1.0;
    }

    /**
     * Get the number of distinct field value
     * @param field The index of field
     * @return The number of distinct field value
     */
    public int getFieldDistinct(int field){
        if (this.distinctMap.containsKey(field)){
            return this.distinctMap.get(field).size();
        }else{
            return this.tuplesNum;
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.tuplesNum;
    }

}
