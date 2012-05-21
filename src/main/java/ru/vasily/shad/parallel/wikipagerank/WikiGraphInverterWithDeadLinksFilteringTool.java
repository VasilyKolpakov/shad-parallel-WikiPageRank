package ru.vasily.shad.parallel.wikipagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;

public class WikiGraphInverterWithDeadLinksFilteringTool extends Configured implements Tool
{
    private final Path inputPath;
    private final Path outputPath;
    private final Class<? extends OutputFormat> outputFormatClass;

    public WikiGraphInverterWithDeadLinksFilteringTool(Path inputPath, Path outputPath, Class<? extends OutputFormat> outputFormatClass)
    {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.outputFormatClass = outputFormatClass;
    }

    public static class WikiGraphInvertMapper
            extends Mapper<Text, ArticleDataWritable, Text, TextOption>
    {
        private final Text textWritable = new Text();
        private final TextOption articleWritable = new TextOption();

        public void map(Text key, ArticleDataWritable value, Context context)
                throws IOException, InterruptedException
        {
            String articleName = key.toString();
            articleWritable.setString(articleName);
            for (String link : value.getLinks())
            {
                textWritable.set(link);
                context.write(textWritable, articleWritable);
            }
            textWritable.set(articleName);
            articleWritable.setNoString();
            context.write(textWritable, articleWritable);
        }
    }


    public static class WikiGraphInvertReducer extends Reducer<Text, TextOption, Text, ArticleDataWritable>
    {
        private final Text textWritable = new Text();
        private final ArticleDataWritable articleDataWritable = new ArticleDataWritable();

        public void reduce(Text key, Iterable<TextOption> values,
                           Context context) throws IOException, InterruptedException
        {
            ArrayList<String> links = newArrayList();
            boolean hasEmptyValue = false;
            for (TextOption value : values)
            {
                if (value.hasValue())
                {
                    links.add(value.get());
                }
                else
                {
                    hasEmptyValue = true;
                }
            }
            if (hasEmptyValue)
            {
                textWritable.set(key.toString());
                articleDataWritable.set(1.0, links);
                context.write(textWritable, articleDataWritable);
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception
    {
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(WikiGraphInverterWithDeadLinksFilteringTool.class);
        job.setJobName("graphInverter");


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextOption.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ArticleDataWritable.class);

        job.setMapperClass(WikiGraphInvertMapper.class);
        job.setReducerClass(WikiGraphInvertReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(outputFormatClass);

        int numberOfReducers = getConf().getInt(ShadParallelConstants.REDUCERS_NUMBER_KEY, 1);
        job.setNumReduceTasks(numberOfReducers);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static class TextOption implements Writable
    {
        private Text text = new Text();
        private boolean hasValue;

        public String get()
        {
            checkState(hasValue);
            return text.toString();
        }

        public boolean hasValue()
        {
            return hasValue;
        }

        public void setString(String string)
        {
            text.set(string);
            hasValue = true;
        }

        public void setNoString()
        {
            hasValue = false;
            text.set("");
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException
        {
            dataOutput.writeBoolean(hasValue);
            if (hasValue)
            {
                text.write(dataOutput);
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException
        {
            hasValue = dataInput.readBoolean();
            if (hasValue)
            {
                text.readFields(dataInput);
            }
            else
            {
                text.set("");
            }
        }
    }
}
