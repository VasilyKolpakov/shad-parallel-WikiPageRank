package ru.vasily.shad.parallel.wikipagerank;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ArticleDataWritable implements Writable
{
    private double pageRank;
    private ArrayList<String> links = new ArrayList<String>();

    public void set(double pageRank, List<String> links)
    {
        this.pageRank = pageRank;
        this.links.clear();
        this.links.addAll(links);
    }

    public double getPageRank()
    {
        return pageRank;
    }

    public List<String> getLinks()
    {
        return links;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeDouble(pageRank);
        dataOutput.writeInt(links.size());
        for (String link : links)
        {
            dataOutput.writeUTF(link);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        pageRank = dataInput.readDouble();
        int size = dataInput.readInt();
        links.clear();
        for (int i = 0; i < size; i++)
        {
            links.add(dataInput.readUTF());
        }
    }

    @Override
    public String toString()
    {
        return "ArticleDataWritable{" +
                "pageRank=" + pageRank +
                ", links=" + links +
                '}';
    }
}
