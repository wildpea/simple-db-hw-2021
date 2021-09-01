package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    List<Integer> buckets;
    private int min;
    private int max;
    private double width;
    private int ntups;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// wildpea
        this.min = min;
        this.max = max;
        this.width = (double) (max - min) / (buckets - 1);
        this.buckets = new ArrayList<>(buckets);
        for (int i = 0; i < buckets; ++i) {
            this.buckets.add(0);
        }
    }

    private int getIndex(int v) {
        int index = (int)Math.floor((v - min) / width);
        if (index < 0) {
            index = 0;
        }
        if (index >= buckets.size()) {
            index = buckets.size() - 1;
        }
        return index;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// wildpea
        int index = getIndex(v);
        buckets.set(index, buckets.get(index) + 1);
        ++ntups;
    }

    //(h / w) / ntups
    private double equal(int v, int h, int index) {
        if (v > max || v < min) {
            return 0;
        }
        return (double)h / width / ntups;
    }

    //b_f = h_b / ntups
    //b_part* of *b = (b_right - const) / w_b
    //(b_f  x  b_part)
    private double greater(int v, int h, int index) {
        if (v > max) {
            return 0;
        }
        double b = (double) h / ntups;
        double bPart = ((double) (index + 1) * width - 1 + min - v) / width;
        double rst = b * bPart;
        while (++index < buckets.size()) {
            rst += ((double) buckets.get(index) / ntups);
        }
        return rst;
    }

    private double less(int v, int h, int index) {
        if (v < min) {
            return 0;
        }
        double b = (double) h / ntups;
        double bPart = (v - ((double)index * width + min)) / width;
        double rst = b * bPart;
        while (--index >= 0) {
            rst += ((double) buckets.get(index) / ntups);
        }
        return rst;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// wildpea
        int index = getIndex(v);
        int h = buckets.get(index);
        switch (op) {
            case EQUALS:
                return equal(v, h, index);
            case GREATER_THAN:
                return greater(v, h, index);
            case LESS_THAN:
                return less(v, h, index);
            case LESS_THAN_OR_EQ:
                return less(v, h, index) + equal(v, h, index);
            case GREATER_THAN_OR_EQ:
                return greater(v, h, index) + equal(v, h, index);
            case NOT_EQUALS:
                return greater(v, h, index) + less(v, h, index);
            case LIKE:
            default:
                break;
        }
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        // wildpea
        return String.format("num:%d,min:%d,max:%d,width:%2f,buckets:%s", buckets.size(), min, max, width
                , buckets.stream().map(String::valueOf).collect(Collectors.joining()));
    }
}
