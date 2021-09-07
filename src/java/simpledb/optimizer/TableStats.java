package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

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

    private int tableid;
    private int ioCostPerPage;
    private DbFile dbFile;
    private TupleDesc td;
    private int totalTups;

    private Map<Integer, IntHistogram> intHistogramList = new HashMap<>();
    private Map<Integer, StringHistogram> stringHistogramList = new HashMap<>();


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
        // wildpea
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        this.td = dbFile.getTupleDesc();

        Set<Integer> intFields = new HashSet<>();
        Set<Integer> stringFields = new HashSet<>();
        Map<Integer, Integer[]> intV = new HashMap<>();
        for (int i = 0; i < td.numFields(); ++i) {
            if (td.getFieldType(i).getClass() == Type.INT_TYPE.getClass()) {
                intFields.add(i);
                intV.put(i, new Integer[] { Integer.MAX_VALUE, Integer.MIN_VALUE });
            } else {
                stringFields.add(i);
            }
        }

        totalTups = 0;
        TransactionId tid = new TransactionId();
        DbFileIterator iter = dbFile.iterator(tid);
        try {
            iter.open();
            while (iter.hasNext()) {
                Tuple t = iter.next();
                for (Integer i: intFields) {
                    int tf = ((IntField) t.getField(i)).getValue();
                    intV.get(i)[0] = Math.min(tf, intV.get(i)[0]);
                    intV.get(i)[1] = Math.max(tf, intV.get(i)[1]);
                }
                ++totalTups;
            }

            int buckets = Math.max(Math.min(totalTups / 20, 1000), 1);
            for (Integer i : stringFields) {
                stringHistogramList.put(i, new StringHistogram(buckets));
            }
            for (Integer i : intFields) {
                buckets = Math.max(Math.min(totalTups / 20, intV.get(i)[1] - intV.get(i)[0] + 1), 1);
                intHistogramList.put(i, new IntHistogram(buckets, intV.get(i)[0], intV.get(i)[1]));
            }

            iter.rewind();
            while (iter.hasNext()) {
                Tuple t = iter.next();
                for (Integer i : intFields) {
                    intHistogramList.get(i).addValue(((IntField) t.getField(i)).getValue());
                }
                for (Integer i : stringFields) {
                    stringHistogramList.get(i).addValue(((StringField) t.getField(i)).getValue());
                }
            }
        } catch (Exception e) {
            System.out.println("oops");
        }
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
        // wildpea
        int pageNum = ((HeapFile) dbFile).numPages();
        //double
        return pageNum * ioCostPerPage;
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
        // wildpea
        return (int) (totalTups * selectivityFactor);
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
        // wildpea
        if (intHistogramList.containsKey(field)) {
            return intHistogramList.get(field).avgSelectivity();
        } else {
            return stringHistogramList.get(field).avgSelectivity();
        }
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
        // wildpea
        if (intHistogramList.containsKey(field)) {
            return intHistogramList.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
        } else {
            return stringHistogramList.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // wildpea
        return totalTups;
    }

}
