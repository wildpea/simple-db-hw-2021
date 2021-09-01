package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.common.Utility;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.util.Arrays;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private TupleDesc td;
    private boolean called = false;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // wildpea
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        this.td = Utility.getTupleDesc(1);
    }

    @Override
    public TupleDesc getTupleDesc() {
        // wildpea
        return td;
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // wildpea
        if (called) {
            return null;
        }
        called = true;

        //insert
        int num = 0;
        try {
            while (child.hasNext()) {
                Database.getBufferPool().insertTuple(t, tableId, child.next());
                ++num;
            }
        } catch (Exception e) {
            throw new DbException("insert error");
        }
        Tuple rst = new Tuple(td);
        rst.setField(0, new IntField(num));
        return rst;
    }

    @Override
    public OpIterator[] getChildren() {
        // wildpea
        return new OpIterator[]{ child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // wildpea
        if (children.length >= 1) {
            this.child = children[0];
        } else {
            this.child = null;
        }
    }
}
