package ru.vasily.shad.parallel.wikipagerank;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;

public abstract class AlternativeWritable<P extends Writable, A extends Writable> implements Writable
{
    private final P primary;
    private final A alternative;
    private boolean isPrimaryValue;

    public AlternativeWritable(P primary, A alternative)
    {
        this.primary = primary;
        this.alternative = alternative;
    }

    public boolean isPrimaryValue()
    {
        return isPrimaryValue;
    }

    public P getPrimaryWritable()
    {
        checkState(isPrimaryValue);
        return primary;
    }

    public A getAlternativeWritable()
    {
        checkState(!isPrimaryValue);
        return alternative;
    }

    public P getPrimaryWritableForSet()
    {
        isPrimaryValue = true;
        return primary;
    }

    public A getAlternativeWritableForSet()
    {
        isPrimaryValue = false;
        return alternative;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeBoolean(isPrimaryValue);
        if (isPrimaryValue)
        {
            primary.write(dataOutput);
        }
        else
        {
            alternative.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        isPrimaryValue = dataInput.readBoolean();
        if (isPrimaryValue)
        {
            primary.readFields(dataInput);
        }
        else
        {
            alternative.readFields(dataInput);
        }
    }
}
