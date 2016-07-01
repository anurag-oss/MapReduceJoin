package hadoopjoinexample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/*
 * This is the join key, which is common,  for the 2 tables
 * The value will be either from the Product table or the SalesOrder Details table
 * To identify the record type, we maintain the record type field . 0 is for product and 1 is for sales order detail 
 * 
 * 
 */

public class ProductIdKey implements WritableComparable<ProductIdKey> {
	
	IntWritable productId = new IntWritable();
	IntWritable recordType = new IntWritable();
	
	
	//some constants for our use
	public static final IntWritable PRODUCT_RECORD = new IntWritable(0);
	public static final IntWritable DATA_RECORD = new IntWritable(1);
	

	/*
	 * this is for our own convenience
	 */
	public ProductIdKey(int productId, IntWritable recordType) {
		super();
		this.productId.set(productId);
		this.recordType = recordType;
	}
	
	

	/*
	 * default constructor is required by hadoop
	 */
	public ProductIdKey() {
		super();
		
	}



	/*
	 * to serialize / deserialize the data, we invoke the serialization of each individual fields and vice versa
	 */
	public void write(DataOutput out) throws IOException {
		this.productId.write(out);
		this.recordType.write(out);
		
	}
	public void readFields(DataInput in) throws IOException {
		this.productId.readFields(in);
		this.recordType.readFields(in);
		
	}

	/*
	 * We want the records to be first sorted based on the key and then on value
	 * This method is called by us in our custom comparator 
	 * hadoopjoinexample.Driver.JoinSortingComparator
	 * We could have avoided the custom comparator, as by default hadoop will call this method while sorting
	 */
	public int compareTo(ProductIdKey o) {
		int compareValue= this.productId.compareTo(o.productId);
		
		if(compareValue==0){
			return this.recordType.compareTo(o.recordType);
		}else{
			return compareValue;
		}
	}



	/*
	 * we are overriding the hash code as we want that all records having same key go to the same reducer
	 * By default the partition logic uses the hash based partition
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((productId == null) ? 0 : productId.hashCode());
		return result;
	}



	/*
	 * When we over-ride the hash code, we should over-ride equals
	 * if hash-code of 2 objects is same, they may or may not be equal
	 * but if they are equal, their hash-code must be same
	 * This method need not be over ridden in our example
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ProductIdKey other = (ProductIdKey) obj;
		if (productId == null) {
			if (other.productId != null)
				return false;
		} else if (!productId.equals(other.productId))
			return false;
		return true;
	}




	
	

	

	
	
	
	

}
