package hadoopjoinexample;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;


/*
 * We need a wrapper for both ProductRecord and SalesOrderRecord
 * The output value of mapper should match with the input value type of reducer 
 * Hadoop asks us to 
 */

public class JoinGenericWritable extends GenericWritable {
    
    private static Class<? extends Writable>[] CLASSES = null;

    static {
        CLASSES = (Class<? extends Writable>[]) new Class[] {
                SalesOrderDataRecord.class,
                ProductRecord.class
        };
    }
   
    public JoinGenericWritable() {}
   
    public JoinGenericWritable(Writable instance) {
        set(instance);
    }

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }
}
