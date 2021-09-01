package simpledb.execution;

import simpledb.common.*;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private OpIterator child;
    private TupleDesc td;
    private boolean called = false;

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
        // wildpea
        this.t = t;
        this.child = child;
        this.td = Utility.getTupleDesc(1);
    }

    @Override
    public TupleDesc getTupleDesc() {
        // wildpea
        return null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // wildpea
        child.open();
        called = false;
        super.open();
    }

    @Override
    public void close() {
        // wildpea
        child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        // wildpea
        child.rewind();
        called = false;
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
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // wildpea
        if (called) {
            return null;
        }
        called = true;

        int num = 0;
        try {
            while (child.hasNext()) {
                Database.getBufferPool().deleteTuple(t, child.next());
                ++num;
            }
        } catch (Exception e) {
            throw new DbException("delete err");
        }

        Tuple rst = new Tuple(td);
        rst.setField(0, new IntField(num));
        return rst;
    }

    @Override
    public OpIterator[] getChildren() {
        // wildpea
        return new OpIterator[] { child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // wildpea
        if (children.length > 0) {
            child = children[0];
        } else {
            child = null;
        }
    }

}
