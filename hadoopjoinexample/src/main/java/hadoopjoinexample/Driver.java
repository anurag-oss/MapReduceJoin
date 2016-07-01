package hadoopjoinexample;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/*
 * Redcuer side join is good when both the tables are big
 * 
 */

/*
 * 
 * The code and concepts are inspired from 
 * http://www.codeproject.com/Articles/869383/Implementing-Join-in-Hadoop-Map-Reduce
 * 
 * The above tutorial explain Reducer side Join
 * 
 * You can run the above program locally
 * Make a new run as java application in eclipse
 * The main class is hadoopjoinexample.Driver
 * 
 * The program arguments are 
 * SalesOrderDetail.csv Product.csv  output
 */


public class Driver extends Configured implements Tool{

	
	/*
	 * We want that all the records having the same product key should come as one single call to reducer
	 * so we compare based on the product id .
	 * So a custom grouping comparator
	 */
	public static class JoinGroupingComparator extends WritableComparator {
	    public JoinGroupingComparator() {
	        super (ProductIdKey.class, true);
	    }                             

	    @Override
	    public int compare (WritableComparable a, WritableComparable b){
	        ProductIdKey first = (ProductIdKey) a;
	        ProductIdKey second = (ProductIdKey) b;
	                      
	        return first.productId.compareTo(second.productId);
	    }
	}
	
	
	/*
	 * Sort all the records based on the product id and then based on record type
	 * so a custom sprting comparator
	 * calls hadoopjoinexample.ProductIdKey.compareTo(ProductIdKey)
	 */
	public static class JoinSortingComparator extends WritableComparator {
	    public JoinSortingComparator()
	    {
	        super (ProductIdKey.class, true);
	    }
	                               
	    @Override
	    public int compare (WritableComparable a, WritableComparable b){
	        ProductIdKey first = (ProductIdKey) a;
	        ProductIdKey second = (ProductIdKey) b;
	                                 
	        return first.compareTo(second);
	    }
	}
	
	
	
	/*
	 * 
	 * Mappers
	 */
	public static class SalesOrderDataMapper extends Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {                           
	        String[] recordFields = value.toString().split("\\t");
	        int productId = Integer.parseInt(recordFields[4]);
	        int orderQty = Integer.parseInt(recordFields[3]);
	        double lineTotal = Double.parseDouble(recordFields[8]);
	        
	        //setting the record type correctly
	        ProductIdKey recordKey = new ProductIdKey(productId, ProductIdKey.DATA_RECORD);
	        SalesOrderDataRecord record = new SalesOrderDataRecord(orderQty, lineTotal);
	                                               
	        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
	        context.write(recordKey, genericRecord);
	    }
	}
	               
	public static class ProductMapper extends Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable>{
		
		
		private HashMap<Integer, String> productSubCategories = new HashMap<Integer, String>();
        
	    private void readProductSubcategoriesFile(URI uri) throws IOException{
	        List<String> lines = FileUtils.readLines(new File(uri));
	        for (String line : lines) {
	            String[] recordFields = line.split("\\t");
	            int key = Integer.parseInt(recordFields[0]);
	            String productSubcategoryName = recordFields[2];
	            productSubCategories.put(key, productSubcategoryName);
	        }
	    }
	    
	    
	    public void setup(Context context) throws IOException{
	        URI[] uris = context.getCacheFiles();
	        readProductSubcategoriesFile(uris[0]);
	    }
	       
		
		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] recordFields = value.toString().split("\\t");
	        int productId = Integer.parseInt(recordFields[0]);
	        String productName = recordFields[1];
	        String productNumber = recordFields[2];
	        int productSubcategoryId = recordFields[18].length() > 0 ? Integer.parseInt(recordFields[18]) : 0;
	        String productSubcategoryName = productSubcategoryId > 0 ? productSubCategories.get(productSubcategoryId) : "";
	         
	        //setting the record type correctly
	        ProductIdKey recordKey = new ProductIdKey(productId, ProductIdKey.PRODUCT_RECORD);
	        ProductRecord record = new ProductRecord(productName, productNumber,productSubcategoryName);
	        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
	        context.write(recordKey, genericRecord);
	    }
	}
	
	
	
	/*
	 * 
	 * 
	 * Reducers
	 * 
	 * All the records having the same product are grouped together and come in single call to reduce function
	 */
	public static class JoinRecuder extends Reducer<ProductIdKey, JoinGenericWritable, NullWritable, Text>{
	    public void reduce(ProductIdKey key, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException{
	        StringBuilder output = new StringBuilder();
	        int sumOrderQty = 0;
	        double sumLineTotal = 0.0;
	                                               
	        for (JoinGenericWritable v : values) {
	            Writable record = v.get();
	            if (key.recordType.equals(ProductIdKey.PRODUCT_RECORD)){
	                ProductRecord pRecord = (ProductRecord)record;
	                output.append(Integer.parseInt(key.productId.toString())).append(", ");
	                output.append(pRecord.productName.toString()).append(", ");
	                output.append(pRecord.productNumber.toString()).append(", ");
	                output.append(pRecord.productSubCategoryName.toString()).append(", ");
	            } else {
	                SalesOrderDataRecord record2 = (SalesOrderDataRecord)record;
	                sumOrderQty += Integer.parseInt(record2.orderQty.toString());
	                sumLineTotal += Double.parseDouble(record2.lineTotal.toString());
	            }
	        }
	        
	        if (sumOrderQty > 0) {
	            context.write(NullWritable.get(), new Text(output.toString() + sumOrderQty + ", " + sumLineTotal));
	        }
	    }
	}
	
	/*
	 * Driver
	 * 
	 */
	
	public int run(String[] allArgs) throws Exception {
		
		System.out.println(Arrays.toString(allArgs));
	    String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
	    System.out.println(Arrays.toString(args));
	                               
	    Job job = Job.getInstance(getConf());
	    job.setJarByClass(Driver.class);
	                               
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	                               
	    job.setMapOutputKeyClass(ProductIdKey.class);
	    job.setMapOutputValueClass(JoinGenericWritable.class);
	    
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	                               
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SalesOrderDataMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ProductMapper.class);
	                              
	    job.setReducerClass(JoinRecuder.class);
	                         
	    //Our custom comparators
	    job.setSortComparatorClass(JoinSortingComparator.class);
	    job.setGroupingComparatorClass(JoinGroupingComparator.class);
	                               

	    //adding the cache file
	    job.addCacheFile(new File(args[2]).toURI()); 
	    
	    FileOutputFormat.setOutputPath(job, new Path(args[3]));
	    
	    
	    boolean status = job.waitForCompletion(true);
	    if (status) {
	        return 0;
	    } else {
	        return 1;
	    }             
	}
	
	
	
	
	public static void main(String[] args) throws Exception{                               
	    Configuration conf = new Configuration();
	    int res = ToolRunner.run(new Driver(), args);
	}
}
