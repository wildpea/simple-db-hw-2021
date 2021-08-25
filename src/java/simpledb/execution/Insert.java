package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
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
    OpIterator[] children = new OpIterator[1];
    private Tuple rst = null;

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
        children[0] = child;
    }

    @Override
    public TupleDesc getTupleDesc() {
        // wildpea
        return Database.getCatalog().getTupleDesc(tableId);
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // wildpea
        super.open();
        child.open();
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
        TupleDesc oneIntColumns = Utility.getTupleDesc(1);
        rst = new Tuple(oneIntColumns);
        rst.setField(0, new IntField(num));
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
        Tuple rt = rst;
        rst = null;
        return rt;
    }

    @Override
    public OpIterator[] getChildren() {
        // wildpea
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // wildpea
        this.children = children;
        if (children.length >= 1) {
            this.child = children[0];
        } else {
            this.child = null;
        }
    }
}
