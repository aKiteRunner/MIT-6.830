package simpledb;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets, min, max, ntups;
    private int[] height;
    private int width, lastWidth;

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
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.height = new int[buckets];
        width = (max - min + 1) / buckets;
        width = Math.max(1, width);
        lastWidth = (max - min + 1) - (buckets - 1) * width;
        ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int idx = Math.min((v - min) / width, buckets - 1);
        ntups++;
        height[idx]++;
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

    	// some code goes here
        if (ntups == 0) return 0;
        double res = 0.0;
        int idx = Math.min((v - min) / width, buckets - 1);
        int w = idx < buckets - 1 ? width : lastWidth;
        switch (op) {
            case EQUALS:
                res = estimateEqual(idx, w, v);
                break;
            case LESS_THAN:
                res = estimateLessThan(idx, w, v);
                break;
            case LESS_THAN_OR_EQ:
                res = estimateEqual(idx, w, v) + estimateLessThan(idx, w, v);
                break;
            case NOT_EQUALS:
                res = 1.0 - estimateEqual(idx, w, v);
                break;
            case GREATER_THAN:
                res = 1.0 - estimateEqual(idx, w, v) - estimateLessThan(idx, w, v);
                break;
            case GREATER_THAN_OR_EQ:
                res = 1.0 - estimateLessThan(idx, w, v);
                break;
            default:
                res = -1.0;
        }
        return res;
    }

    private double estimateEqual(int idx, int w, int v) {
        if (v > max || v < min) return 0;
        return 1.0 * height[idx] / w / ntups;
    }

    private double estimateLessThan(int idx, int w, int v) {
        if (v >= max) return 1.0;
        if (v <= min) return 0;
        double res = 0;
        for (int i = 0; i < idx; i++) {
            res += height[i];
        }
        int left = width * idx + min;
        int cnt = v - left;
        res += 1.0 * height[idx] * cnt / w;
        return res / ntups;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return Arrays.toString(height);
    }
}
