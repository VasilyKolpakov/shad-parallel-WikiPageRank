package ru.vasily.shad.parallel.wikipagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.List;

public class PageRankTool extends Configured implements Tool
{
    public static final double DUMP_FACTOR = 0.85;
    private final Path toolInputPath;
    private final Path toolOutputPath;

    private long numberOfPageRankChanges;

    public PageRankTool(Path inputPath, Path outputPath)
    {
        this.toolInputPath = inputPath;
        this.toolOutputPath = outputPath;
    }

    public static class PageRankMapper
            extends Mapper<Text, ArticleDataWritable, Text, AlternativeWritable>
    {
        private final Text textWritable = new Text();
        private final DoubleOrArticleDataWritable doubleOrArticleDataWritable = new DoubleOrArticleDataWritable();

        public void map(Text key, ArticleDataWritable value, Context context)
                throws IOException, InterruptedException
        {
            List<String> articleLinks = value.getLinks();
            int numberOfLinks = articleLinks.size();
            if (numberOfLinks > 0)
            {
                double pageRankDelta = value.getPageRank() / numberOfLinks;
                for (String link : articleLinks)
                {
                    textWritable.set(link);
                    doubleOrArticleDataWritable.getPrimaryWritableForSet().set(pageRankDelta);
                    context.write(textWritable, doubleOrArticleDataWritable);
                }
            }
            String articleName = key.toString();
            textWritable.set(articleName);
            doubleOrArticleDataWritable.getAlternativeWritableForSet()
                                       .set(value.getPageRank(), articleLinks);
            context.write(textWritable, doubleOrArticleDataWritable);
        }
    }

    public static class PageRankReducer
            extends Reducer<Text, DoubleOrArticleDataWritable, Text, ArticleDataWritable>
    {
        private final Text textWritable = new Text();
        private final ArticleDataWritable articleDataWritable = new ArticleDataWritable();

        public void reduce(Text key, Iterable<DoubleOrArticleDataWritable> values, Context context)
                throws IOException, InterruptedException
        {
            String articleName = key.toString();
            PageRankAndLinks pageRankAndLinks = retrievePageRankAndLinks(values);
            if (pageRankAndLinks != null)
            {

                double diff = pageRankAndLinks.newPageRank - pageRankAndLinks.oldPageRank;
                if (diff / pageRankAndLinks.newPageRank > 0.01)
                {
                    context.getCounter(Counter.PageRankChange).increment(1);
                }
                textWritable.set(articleName);
                articleDataWritable.set(pageRankAndLinks.newPageRank, pageRankAndLinks.links);
                context.write(textWritable, articleDataWritable);
            }
            else
            {
                throw new RuntimeException("there is no article data for article " + articleName);
            }
        }

        private PageRankAndLinks retrievePageRankAndLinks(Iterable<DoubleOrArticleDataWritable> values)
        {
            PageRankAndLinks pageRankAndLinks = null;
            boolean articleDataExists = false;
            double pageRank = 1.0 - DUMP_FACTOR;
            double oldPageRank = 0;
            List<String> links = null;
            for (DoubleOrArticleDataWritable value : values)
            {
                if (value.isPrimaryValue())
                {
                    pageRank += DUMP_FACTOR * value.getPrimaryWritable().get();
                }
                else
                {
                    articleDataExists = true;
                    ArticleDataWritable articleData = value.getAlternativeWritable();
                    oldPageRank = articleData.getPageRank();
                    links = articleData.getLinks();
                }
            }
            if (articleDataExists)
            {
                pageRankAndLinks = new PageRankAndLinks(pageRank, oldPageRank, links);
            }
            return pageRankAndLinks;
        }
    }

    public static class PageRankCombiner extends Reducer<Text, DoubleOrArticleDataWritable, Text, DoubleOrArticleDataWritable>
    {
        private final DoubleOrArticleDataWritable doubleOrArticleDataWritable = new DoubleOrArticleDataWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleOrArticleDataWritable> values, Context context) throws IOException, InterruptedException
        {
            double pageRankDelta = 0;
            for (DoubleOrArticleDataWritable value : values)
            {
                if (value.isPrimaryValue())
                {
                    pageRankDelta += value.getPrimaryWritable().get();
                }
                else
                {
                    context.write(key, value);
                }
            }
            doubleOrArticleDataWritable.getPrimaryWritableForSet().set(pageRankDelta);
            context.write(key, doubleOrArticleDataWritable);
        }
    }

    @Override
    public int run(String[] strings) throws Exception
    {
        int numberOfIterations = 1;
        Path inputPath = toolInputPath;
        numberOfPageRankChanges = 1;
        int maxIterations = getConf().getInt(ShadParallelConstants.MAX_ITERATIONS_NUMBER_KEY, 30);
        while (numberOfPageRankChanges != 0 && numberOfIterations < maxIterations)
        {
            String outputFolderName = String.format("%d_page_rank", numberOfIterations);
            Path outputPath = new Path(toolOutputPath, outputFolderName);
            boolean success = runPageRankIteration(inputPath, outputPath);
            if (!success)
            {
                return 1;
            }
            inputPath = outputPath;
            numberOfIterations++;
        }

        Path outputPath = new Path(toolOutputPath, "final_page_rank");
        boolean success = runFinalizing(inputPath, outputPath);
        System.out.println("PageRankTool.run numberOfPageRankChanges = " + numberOfPageRankChanges);
        return success ? 0 : 1;
    }


    private boolean runPageRankIteration(Path inputPath, Path outputPath) throws IOException, InterruptedException, ClassNotFoundException
    {
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(LinkExtractorTool.class);
        job.setJobName("pageRank");
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleOrArticleDataWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ArticleDataWritable.class);

        job.setMapperClass(PageRankMapper.class);
        job.setCombinerClass(PageRankCombiner.class);
        job.setReducerClass(PageRankReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        int numberOfReducers = getConf().getInt(ShadParallelConstants.REDUCERS_NUMBER_KEY, 1);
        job.setNumReduceTasks(numberOfReducers);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean success = job.waitForCompletion(true);

        numberOfPageRankChanges = job.getCounters().findCounter(Counter.PageRankChange).getValue();
        return success;
    }

    private boolean runFinalizing(Path inputPath, Path outputPath) throws IOException, InterruptedException, ClassNotFoundException
    {
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(PageRankTool.class);
        job.setJobName("finalizing_page_rank");
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(FinalizingMapper.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean success = job.waitForCompletion(true);
        return success;
    }

    private static class PageRankAndLinks
    {
        public final double newPageRank;
        public final double oldPageRank;
        public final List<String> links;

        private PageRankAndLinks(double newPageRank, double oldPageRank, List<String> links)
        {
            this.newPageRank = newPageRank;
            this.oldPageRank = oldPageRank;
            this.links = links;
        }
    }

    public static class DoubleOrArticleDataWritable extends AlternativeWritable<DoubleWritable, ArticleDataWritable>
    {
        public DoubleOrArticleDataWritable()
        {
            super(new DoubleWritable(), new ArticleDataWritable());
        }
    }

    public enum Counter
    {
        PageRankChange
    }
}
