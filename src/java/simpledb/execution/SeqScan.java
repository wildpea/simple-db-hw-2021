package simpledb.execution;

import simpledb.common.Database;
import simpledb.storage.DbFile;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    // wildpea
    class ScanTupleDesc extends TupleDesc {
        ScanTupleDesc(TupleDesc std) {
            super(std);
        }
        @Override
        public String getFieldName(int i) throws NoSuchElementException {
            return tableAlias + "." + super.getFieldName(i);
        }
        @Override
        public int fieldNameToIndex(String name) throws NoSuchElementException {
            // wildpea
            return super.fieldNameToIndex(name.substring(tableAlias.length() + 1));
        }
    }

    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFile df;
    private DbFileIterator iter;
    private ScanTupleDesc scanTupleDesc;


    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // wildpea
        this.tid = tid;
        reset(tableid, tableAlias);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // wildpea
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // wildpea
        this.tableid = tableid;
        this.tableAlias = tableAlias == null ? getTableName() : tableAlias;

        df = Database.getCatalog().getDatabaseFile(this.tableid);
        scanTupleDesc = new ScanTupleDesc(this.df.getTupleDesc());
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // wildpea
        this.iter = this.df.iterator(tid);
        iter.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // wildpea
        return scanTupleDesc;
    }

    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        // wildpea
        return iter.hasNext();
    }

    @Override
    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // wildpea
        return iter.next();
    }

    @Override
    public void close() {
        // wildpea
        iter.close();
    }

    @Override
    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // wildpea
        iter.rewind();
    }
}
