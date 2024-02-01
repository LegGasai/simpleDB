package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final Field NO_GROUPING_FIELD = new IntField(NO_GROUPING);

    private final int gbfield;
    private final Type gbfieldType;
    private final int afield;
    private final Op what;
    private final ConcurrentMap<Field, List<Tuple>> groups = new ConcurrentHashMap();
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (tup == null) return;
        // No group
        if (this.gbfield == NO_GROUPING){
            List<Tuple> groupTuples = groups.getOrDefault(NO_GROUPING_FIELD, new ArrayList<Tuple>());
            groupTuples.add(tup);
            groups.put(NO_GROUPING_FIELD,groupTuples);
        }
        // add to special group
        else{
            List<Tuple> groupTuples = groups.getOrDefault(tup.getField(gbfield), new ArrayList<Tuple>());
            groupTuples.add(tup);
            groups.put(tup.getField(gbfield),groupTuples);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc td = gbfield == NO_GROUPING? new TupleDesc(new Type[]{Type.INT_TYPE}):new TupleDesc(new Type[]{gbfieldType,Type.INT_TYPE});
        return new TupleIterator(td,generateAggTuples(td));
    }

    public List<Tuple> generateAggTuples(TupleDesc td){
        List<Tuple> aggTuples = new ArrayList<>();
        for (Map.Entry<Field, List<Tuple>> entry : groups.entrySet()) {
            int aggregateVal = getAggResult(entry.getValue());
            if (gbfield == NO_GROUPING){
                // (aggregateVal)
                Tuple tuple = new Tuple(td);
                tuple.setField(0,new IntField(aggregateVal));
                aggTuples.add(tuple);
            }else{
                // (groupVal, aggregateVal)
                Tuple tuple = new Tuple(td);
                tuple.setField(0,entry.getKey());
                tuple.setField(1,new IntField(aggregateVal));
                aggTuples.add(tuple);
            }
        }
        return aggTuples;
    }

    private int getAggResult(List<Tuple> groupTuples){
        if (what == Op.COUNT){
            return groupTuples.size();
        }else if (what == Op.MAX){
            return groupTuples.stream()
                    .mapToInt(s -> ((IntField) s.getField(afield)).getValue())
                    .max().getAsInt();
        }else if (what == Op.MIN){
            return groupTuples.stream()
                    .mapToInt(s -> ((IntField) s.getField(afield)).getValue())
                    .min().getAsInt();
        }else if (what == Op.SUM){
            return groupTuples.stream()
                    .mapToInt(s -> ((IntField) s.getField(afield)).getValue())
                    .sum();
        }else if (what == Op.AVG){
            return (int)groupTuples.stream()
                    .mapToInt(s -> ((IntField) s.getField(afield)).getValue())
                    .average().getAsDouble();
        }
        throw new UnsupportedOperationException("unknown op type");
    }
}
