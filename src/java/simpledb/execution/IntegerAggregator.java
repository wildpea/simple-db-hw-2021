package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

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
    private Map<Field, Double[]> mAvg = new HashMap<>();

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
        // wildpea
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

    private IntField getRst(Field key, IntField f1, IntField f2) {
        switch (what) {
            case MIN:
                if (f1 == null) {
                    return new IntField(f2.getValue());
                }
                return new IntField(Math.min(f1.getValue(), f2.getValue()));
            case MAX:
                if (f1 == null) {
                    return new IntField(f2.getValue());
                }
                return new IntField(Math.max(f1.getValue(), f2.getValue()));
            case SUM:
                if (f1 == null) {
                    return new IntField(f2.getValue());
                }
                return new IntField(f1.getValue() + f2.getValue());
            case AVG:
                Double[] m = mAvg.get(key);
                if (f1 == null) {
                    m[0] = 1.0;
                    m[1] = (double) f2.getValue();
                    return new IntField(f2.getValue());
                }
                m[0] = m[0] + 1.0;

                // >_< all for test_case
                if (false) {
                    m[1] = m[1] + ((double) f2.getValue() - m[1]) / m[0];
                    return new IntField((int) Math.round(m[1]));
                } else {
                    m[1] = m[1] + f2.getValue();
                    return new IntField((int) (m[1] / m[0]));
                }
            case COUNT:
                if (f1 == null) {
                    return new IntField(1);
                }
                return new IntField(f1.getValue() + 1);
            case SUM_COUNT:
            case SC_AVG:
            default:
                return defaultField;
        }
    }

    private void initTuple(Field key) {
        Tuple t = new Tuple(td);
        if (groupBy) {
            t.setField(0, key);
        }
        tuples.put(key, t);

        if (Aggregator.Op.AVG.equals(what)) {
            mAvg.put(key, new Double[]{0.0, 0.0});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        // wildpea
        Field key = !groupBy ? defaultField : tup.getField(gbfield);
        if (!tuples.containsKey(key)) {
            initTuple(key);
        }

        Tuple t = tuples.get(key);
        t.setField(filedIndex, getRst(key, (IntField) t.getField(filedIndex), (IntField) tup.getField(afield)));
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    @Override
    public OpIterator iterator() {
        // wildpea
        return new TupleIterator(td, tuples.values());
    }

}
