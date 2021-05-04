import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class imdb {

    public static void main(String[] args) throws Exception {
        if (args.length !=8) {
            System.err.println("Usage: Imdb <input path> <output path>");
            System.err.println("Arg length is " + args.length);
            for (int i=0; i<args.length; i++) System.err.println("Arg " + i + " is " + args[i]);

            //System.err.println(args+"\n");
            System.exit(-1);
        }


 //------------------------Job 1: Get ratings, combine with movie details
        // and get directors for movies
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "IMDB Director Analysis-x20103689");
        job1.setJarByClass(imdb.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, getRatingsMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, getBasicsMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, getTitleDirectorsMapper.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[3]));

        job1.setJobName("IMDB Director Analysis-x20103689");
        job1.setReducerClass(join_RBD_Reducer.class);
        //job.setNumReduceTasks(1);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.waitForCompletion(true);

 //---------------------Job 2: Get director names from NameBasics.TSV
        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "Join director names with ID");
        job2.setJarByClass(imdb.class);

        MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, getdirectorid.class);
        MultipleInputs.addInputPath(job2, new Path(args[5]), TextInputFormat.class, getdirectorname.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[6]));

        job2.setReducerClass(getdirectornamereducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.waitForCompletion(true);

 //-------------------Job 3: Get top 100 movies for past 70 years from 2020
        // based on user rating and number of votes for each movies
        Configuration conf3 = new Configuration();

        Job job3 = Job.getInstance(conf3, "top 100 rated");
        job3.setJarByClass(imdb.class);

        job3.setMapperClass(top100rated.class);
        job3.setReducerClass(top100ratedreducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(LongWritable.class);

        //job2.setInputFormat(TextInputFormat.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPaths(job3, String.valueOf(new Path(args[6] + "/part-r-00000")));
        FileOutputFormat.setOutputPath(job3, new Path(args[4]));
        job3.waitForCompletion(true);


        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}

/*
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver {

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,
								args).getRemainingArgs();

		Job job1 = Job.getInstance(conf, "top 100 rated");
		job1.setJarByClass(imdb.class);

		job1.setMapperClass(top100rated.class);
		job1.setReducerClass(top100ratedreducer.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(LongWritable.class);

		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[3]));
		job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();


		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "IMDB Director Analysis-x20103689");
        job.setJarByClass(imdb.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, getRatingsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, getBasicsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, getTitleDirectorsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        job.setJobName("IMDB Director Analysis-x20103689");
        job.setReducerClass(join_RBD_Reducer.class);
        //job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);


Reversing job sequence
--------

Configuration conf = new Configuration();
        //String[] otherArgs = new GenericOptionsParser(conf,
          //      args).getRemainingArgs();

        Job job1 = Job.getInstance(conf, "top 100 rated");
        job1.setJarByClass(imdb.class);

        job1.setMapperClass(top100rated.class);
        job1.setReducerClass(top100ratedreducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);

        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[3]));
        job1.waitForCompletion(true);

        //Configuration conf2 = new Configuration();


        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "IMDB Director Analysis-x20103689");
        job2.setJarByClass(imdb.class);

        MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, getRatingsMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, getBasicsMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, getTitleDirectorsMapper.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));

        job2.setJobName("IMDB Director Analysis-x20103689");
        job2.setReducerClass(join_RBD_Reducer.class);
        //job.setNumReduceTasks(1);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
--------


        //----------------Job 3
        Configuration conf3 = new Configuration();

        Job job3 = Job.getInstance(conf3, "top 100 rated");
        job3.setJarByClass(imdb.class);

        job3.setMapperClass(top100rated.class);
        job3.setReducerClass(top100ratedreducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(LongWritable.class);

        //job2.setInputFormat(TextInputFormat.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPaths(job3, String.valueOf(new Path(args[3] + "/part-r-00000")));
        FileOutputFormat.setOutputPath(job3, new Path(args[4]));
        job3.waitForCompletion(true);
*/







