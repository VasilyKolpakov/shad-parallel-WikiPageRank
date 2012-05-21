package ru.vasily.shad.parallel.wikipagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FinalizingMapper extends Mapper<Text, ArticleDataWritable, DoubleWritable, Text>

{
    private final DoubleWritable doubleWritable = new DoubleWritable();

    @Override
    protected void map(Text key, ArticleDataWritable value, Context context) throws IOException, InterruptedException
    {
        doubleWritable .set(value.getPageRank());
        context.write(doubleWritable , key);
    }
}
