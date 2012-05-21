package ru.vasily.shad.parallel.wikipagerank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.util.Tool;

public class WikiGraphInvertToolFactory implements ToolFactory
{
    private final Class<? extends OutputFormat> outputFormatClass;

    public WikiGraphInvertToolFactory(Class<? extends OutputFormat> outputFormatClass)
    {
        this.outputFormatClass = outputFormatClass;
    }

    @Override
    public Tool createTool(Path inputPath, Path outputPath)
    {
        return new WikiGraphInverterWithDeadLinksFilteringTool(inputPath, outputPath, outputFormatClass);
    }

    @Override
    public String getToolName()
    {
        return "wiki_graph_invert";
    }
}
