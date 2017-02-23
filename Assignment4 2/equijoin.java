import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class equijoin {
		public static class JoinMap extends Mapper<LongWritable, Text, LongWritable, Text> 

		{
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
			{
				String initialvalue = value.toString().replaceAll("\\s","");
				// assigning key value from each of the record 
				Long keyofmap = Long.parseLong(initialvalue.toString().split(",")[1]);
				context.write(new LongWritable(keyofmap), new Text(initialvalue));
			}
		}
		
		public static class JoinReduce extends Reducer<LongWritable, Text, LongWritable, Text> 
		{
		   protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		   {
			   
			   // building input records
			   StringBuilder record = new StringBuilder();
			   for (Text value : values) 
				{
				   record.append(value);
				   record.append(";");
				}
			 // removing extra ; at the end of the record 
			   record.deleteCharAt(record.length()-1);
			   String allrecord = record.toString();
			// splitting the records 
			   String[] singlerecord = allrecord.split(";");
			// iterating through the records for equijoin
			   for(int i=0; i<singlerecord.length-1; i++)
			   {
				   for(int j=i+1; j<singlerecord.length; j++)
				   {	
					   // check if the records are not  from the same table 
					   if(!singlerecord[i].split(",")[0].equals(singlerecord[j].split(",")[0]))
					   {
						   allrecord = singlerecord[i] + "," + singlerecord[j];
						   context.write(null, new Text(allrecord));
					   }
				
				   }
			   }
			}
		}
	
	public static void main(String[] args) throws Exception {
		//  Auto-generated method stub
		 Configuration conf = new Configuration();
		 Job jobequijoin = Job.getInstance(conf, "equiJoin");
		 // defining jar class
		 jobequijoin.setJarByClass(equijoin.class);
		 // defining Mapper class
		 jobequijoin.setMapperClass(JoinMap.class);
		 //defining Reducer class
		 jobequijoin.setReducerClass(JoinReduce.class);
		 //defining key 
		 jobequijoin.setOutputKeyClass(LongWritable.class);
		 //defining output value
		 jobequijoin.setOutputValueClass(Text.class);
		 FileInputFormat.addInputPath(jobequijoin, new Path(args[0]));
		 FileOutputFormat.setOutputPath(jobequijoin, new Path(args[1]));
		 
		 System.exit(jobequijoin.waitForCompletion(true) ? 0 : 1);
	 }
	}
		