package hadoopjoinexample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


/*
 * The value for the Product table
 */
public class ProductRecord implements Writable {

    public Text productName = new Text();
    public Text productNumber = new Text();
    

    public ProductRecord(){}
               
    public ProductRecord(String productName, String productNumber){
        this.productName.set(productName);
        this.productNumber.set(productNumber);
    }

    public void write(DataOutput out) throws IOException {
        this.productName.write(out);
        this.productNumber.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.productName.readFields(in);
        this.productNumber.readFields(in);
    }
}
