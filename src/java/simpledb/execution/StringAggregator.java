package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private TupleDesc td;
    private boolean groupBy;
    private int filedIndex;
    private Map<Field, Tuple> tuples = new HashMap<>();
    private IntField defaultField = new IntField(0);

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // wildpea
        if (!Op.COUNT.equals(what)) {
            throw new IllegalArgumentException("not support operator");
        }

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupBy = gbfield != NO_GROUPING;

        this.filedIndex = gbfield == -1 ? 0 : 1;
        Type[] typeAr = new Type[filedIndex + 1];
        if (groupBy) {
            typeAr[0]  = gbfieldtype;
        }
        typeAr[filedIndex] = Type.INT_TYPE;

        td = new TupleDesc(typeAr);
    }

    private IntField getRst(Field key, IntField f1, StringField f2) {
        if (f1 == null) {
            return new IntField(1);
        }
        return new IntField(f1.getValue() + 1);
    }

    private void initTuple(Field key) {
        Tuple t = new Tuple(td);
        if (groupBy) {
            t.setField(0, key);
        }
        tuples.put(key, t);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        // wildpea
        Field key = !groupBy ? defaultField : tup.getField(gbfield);
        if (!tuples.containsKey(key)) {
            initTuple(key);
        }

        Tuple t = tuples.get(key);
        t.setField(filedIndex, getRst(key, (IntField) t.getField(filedIndex), (StringField) tup.getField(afield)));
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    @Override
    public OpIterator iterator() {
        // wildpea
        return new TupleIterator(td, tuples.values());
    }

}
