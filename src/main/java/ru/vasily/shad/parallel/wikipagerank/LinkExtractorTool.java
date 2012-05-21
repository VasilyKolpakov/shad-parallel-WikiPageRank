package ru.vasily.shad.parallel.wikipagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

public class LinkExtractorTool extends Configured implements Tool
{
    private final Path inputPath;
    private final Path outputPath;

    public LinkExtractorTool(Path inputPath, Path outputPath)
    {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public static class LinkExtractorMapper
            extends Mapper<LongWritable, Text, Text, ArticleDataWritable>
    {
        private ArticleDataWritable articleDataWritable = new ArticleDataWritable();
        private Text textWritable = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String article = value.toString();
            String articleName = article.substring(0, article.indexOf("\t"));
            Set<String> links = getLinks(article);
            articleDataWritable.set(1.0, newArrayList(links));
            textWritable.set(articleName);
            context.write(textWritable, articleDataWritable);
        }

        public Set<String> getLinks(String text)
        {
            Pattern linkPattern = Pattern.compile("\\[{2}([^|]*?)(\\||\\]{2})");
            Matcher matcher = linkPattern.matcher(text);
            Set<String> links = newHashSet();
            while (matcher.find())
            {
                links.add(matcher.group(1));
            }
            return links;
        }
    }


    @Override
    public int run(String[] strings) throws Exception
    {
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(LinkExtractorTool.class);
        job.setJobName("linkExtractor");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ArticleDataWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ArticleDataWritable.class);

        job.setMapperClass(LinkExtractorMapper.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
