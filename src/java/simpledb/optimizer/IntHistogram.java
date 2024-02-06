package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int bctNum;
    private final int min;
    private final int max;
    private final int interval;
    private final int [] backets;
    private int total;
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
        this.bctNum = buckets;
        this.min = min;
        this.max = max;
        this.interval = (this.max - this.min + 1) / buckets;
        this.backets = new int[this.bctNum];
    }

    private int vToIdx(int v){
        int offset = v-this.min;
        int idx = this.bctNum-1;
        if (this.interval !=0 && offset / this.interval <this.bctNum-1){
            idx = offset / this.interval;
        }
        return idx;
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int idx = vToIdx(v);
        if (v<=this.max && v>=this.min && idx>=0 && idx<this.bctNum){
            this.total += 1;
            this.backets[idx] += 1;
        }
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
        switch (op){
            case EQUALS:{
                return estimateEqual(v);
            }
            case GREATER_THAN:{
                return estimateGreat(v);
            }
            case LESS_THAN:{
                return 1-(estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ,v));
            }
            case LESS_THAN_OR_EQ:{
                return 1-estimateGreat(v);
            }
            case GREATER_THAN_OR_EQ:{
                return estimateGreat(v)+estimateEqual(v);
            }
            case LIKE:{
                return estimateEqual(v);
            }
            case NOT_EQUALS:{
                return 1.0 - estimateEqual(v);
            }
        }
    	// some code goes here
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
    public double avgSelectivity()
    {
        // some code goes here
        double s = 0;
        for (int i = 0; i < this.bctNum; i++) {
            s += (this.backets[i]*1.0)/(this.total*1.0);
        }
        return s/(this.bctNum*1.0);
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < this.bctNum; i++) {
            int w = i== this.bctNum-1?this.max-this.min+1-this.interval*(this.bctNum-1):this.interval;
            int left = this.min + i*interval;
            int right = left + w -1;
            sb.append(String.format("[%d,%d]:%d\n",left,right,this.backets[i]));
        }
        return sb.toString();
    }


    private double estimateGreat(int v){
        if (v<this.min){
            return 1.0;
        }
        if (v>=this.max){
            return 0.0;
        }
        int idx = vToIdx(v);
        // to solve difference the width of last backet
        int w = idx == this.bctNum-1?this.max-this.min+1-this.interval*(this.bctNum-1):this.interval;
        int h = this.backets[idx];
        int mod = (v-this.min)-idx*this.interval;
        double res = 0.0;
        res += ((w-mod-1)/(w*1.0)) * (h*1.0)/(this.total*1.0);
        for (int i = idx+1; i < this.bctNum; i++) {
            res += (this.backets[i]*1.0)/(this.total*1.0);
        }
        return res;
    }

    private double estimateEqual(int v){
        if ((v<this.min) || (v>this.max)){
            return 0.0;
        }
        int idx = vToIdx(v);
        int h = this.backets[idx];
        int w = idx == this.bctNum-1?this.max-this.min+1-this.interval*(this.bctNum-1):this.interval;
        return (1.0/w)*(h*1.0)/(this.total*1.0);
    }
}
