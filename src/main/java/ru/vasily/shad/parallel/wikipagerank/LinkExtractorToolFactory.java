package ru.vasily.shad.parallel.wikipagerank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

public class LinkExtractorToolFactory implements ToolFactory
{
    @Override
    public Tool createTool(Path inputPath, Path outputPath)
    {
        return new LinkExtractorTool(inputPath, outputPath);
    }

    @Override
    public String getToolName()
    {
        return "link_extractor";
    }
}
