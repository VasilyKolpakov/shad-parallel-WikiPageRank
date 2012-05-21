package ru.vasily.shad.parallel.wikipagerank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank
{
    public static void main(String[] args) throws Exception
    {
        Path inputPath = new Path(args[args.length - 2]);
        Path outputPath = new Path(args[args.length - 1]);
        runChainedJobs(inputPath, outputPath, args,
                new LinkExtractorToolFactory(),
                new WikiGraphInvertToolFactory(SequenceFileOutputFormat.class),
                new WikiGraphInvertToolFactory(SequenceFileOutputFormat.class),
                new PageRankToolFactory());
    }

    private static void runTool(Tool tool, String[] args) throws Exception
    {
        int counterErrCode = ToolRunner.run(tool, args);
        if (counterErrCode == 1)
        {
            System.exit(1);
        }
    }


    private static void runChainedJobs(Path inputPath, Path outputPath, String[] args, ToolFactory... toolFactories) throws Exception
    {
        Path toolInputPath = inputPath;
        int toolCount = 0;
        Path lastUsedOutputPath = null;
        for (ToolFactory toolFactory : toolFactories)
        {
            String outputFolderName = folderName(toolCount, toolFactory);
            Path toolOutputPath = new Path(outputPath, outputFolderName);
            Tool tool = toolFactory.createTool(toolInputPath, toolOutputPath);
            runTool(tool, args);
            toolCount++;
            toolInputPath = toolOutputPath;
            lastUsedOutputPath = toolOutputPath;
        }
    }

    private static String folderName(int jobNumber, ToolFactory tool)
    {
        return String.format("%d_%s", jobNumber, tool.getToolName());
    }

}

