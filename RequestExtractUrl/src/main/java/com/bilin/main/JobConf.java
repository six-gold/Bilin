package com.bilin.main;

import com.bilin.job.ImpMapper;
import com.bilin.job.ReqExtractUrlMapper;
import com.bilin.job.ReqExtractUrlReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;

import java.util.Calendar;

public class JobConf extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("logType", args[3]);
        conf.set("property_file_path", args[4]);
        conf.set("RATE", args[5]);
        conf.set("switch", args[6]);
        Calendar c = Calendar.getInstance();

        Job job = Job.getInstance(conf, args[3] + "_extract_url_" + c.get(Calendar.YEAR)
                + (c.get(Calendar.MONTH) + 1) + c.get(Calendar.DAY_OF_MONTH)
                + c.get(Calendar.HOUR_OF_DAY));

        job.setJarByClass(Processor.class);
        job.setReducerClass(ReqExtractUrlReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        FileInputFormat.setInputDirRecursive(job, true);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ReqExtractUrlMapper.class);
        try {
            int tmp = Integer.parseInt(args[1]);
        }catch (NumberFormatException e){
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ImpMapper.class);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        MultipleOutputs.setCountersEnabled(job, true);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
