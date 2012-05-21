package ru.vasily.shad.parallel.wikipagerank;

import org.apache.hadoop.fs.Path;

public class PageRankToolFactory implements ToolFactory<PageRankTool>
{
    @Override
    public PageRankTool createTool(Path inputPath, Path outputPath)
    {
        return new PageRankTool(inputPath, outputPath);
    }

    @Override
    public String getToolName()
    {
        return "page_rank";
    }
}
