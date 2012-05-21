package ru.vasily.shad.parallel.wikipagerank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

public interface ToolFactory<T extends Tool>
{
    T createTool(Path inputPath, Path outputPath);

    String getToolName();
}
