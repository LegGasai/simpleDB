package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private final TupleDesc td;
    private final TransactionId tid;
    private Iterator<Tuple> it;
    private int cnt;
    private OpIterator child;
    private List<Tuple> childTups;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        this.childTups = new ArrayList<>();
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        while (child.hasNext()){
            try {
                Database.getBufferPool().deleteTuple(this.tid,child.next());
                this.cnt++;
            }catch (IOException e){
                throw new DbException("delete tuple error");
            }
        }
        Tuple tuple = new Tuple(this.td);
        tuple.setField(0,new IntField(this.cnt));
        childTups.add(tuple);
        it = childTups.iterator();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        it = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        it = childTups.iterator();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (it != null && it.hasNext()){
            return it.next();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{ this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children[0] != this.child){
            this.child = children[0];
        }
    }

}
