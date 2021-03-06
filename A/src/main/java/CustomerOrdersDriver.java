import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Locale;


public class CustomerOrdersDriver extends Configured implements Tool {
    public static final Log log = LogFactory.getLog(CustomerOrdersDriver.class);
    @Deprecated
    @Override
    public int run(String[] args) throws Exception {

        //System.out.println(args);

        Task task = Task.valueOf(args[3].toUpperCase(Locale.ROOT));

        Job job = Job.getInstance();
        job.setJarByClass(getClass());
        job.setJarByClass(CustomerOrdersDriver.class);
        job.setJobName("ReduceSideJoin Example");

        job.setMapOutputValueClass(GenericCustomerEntity.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapperClass(JoinMapper.class);

        switch(task) {
            case TASKI:
                job.setReducerClass(JoinReducerTaskI.class);
                break;
            case TASKII:
                job.setReducerClass(JoinReducerTaskII.class);
                break;
        }

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new CustomerOrdersDriver(), args);

        System.exit(exitCode);
    }

    public enum Task{
        TASKI, TASKII
    }

}

