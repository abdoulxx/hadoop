package com.projetfinal.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Chiffre d'affaires (Quantity * UnitPrice, retours inclus donc net) par pays et par mois,
 * a partir de online_retail.tsv. Sortie : "Pays\tAAAA-MM\tCA".
 */
public class CAParPaysMois {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ca par pays et mois");
        job.setJarByClass(CAParPaysMois.class);
        job.setMapperClass(CAMapper.class);
        job.setCombinerClass(CAReducer.class);
        job.setReducerClass(CAReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
