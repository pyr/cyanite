package org.apache.cassandra.index.sasi.analyzer;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.index.sasi.analyzer.filter.*;
import org.apache.cassandra.serializers.*;
import org.apache.cassandra.utils.*;

public class SplittingTokenizer extends AbstractAnalyzer
{
    private static final Logger logger = LoggerFactory.getLogger(NonTokenizingAnalyzer.class);

    private static final Set<AbstractType<?>> VALID_ANALYZABLE_TYPES = new HashSet<AbstractType<?>>()
    {{
        add(UTF8Type.instance);
        add(AsciiType.instance);
    }};

    private AbstractType validator;
    private final static Pattern pattern = Pattern.compile("\\.");
    private NonTokenizingOptions options;
    private FilterPipelineTask filterPipeline;

    private ByteBuffer input;
    private Iterator<String> splits;
    private boolean hasNext = false;

    public void init(Map<String, String> options, AbstractType validator)
    {
        init(NonTokenizingOptions.buildFromMap(options), validator);
    }

    public void init(NonTokenizingOptions tokenizerOptions, AbstractType validator)
    {
        this.validator = validator;
        this.options = tokenizerOptions;
        this.filterPipeline = getFilterPipeline();
    }

    public boolean hasNext()
    {
        // check that we know how to handle the input, otherwise bail
        if (!VALID_ANALYZABLE_TYPES.contains(validator))
        {
            logger.info("Can't analyze the input of type " + validator);
            return false;
        }

        if (splits == null)
        {
            String inputStr = validator.getString(input);
            if (inputStr == null)
                throw new MarshalException(String.format("'null' deserialized value for %s with %s", ByteBufferUtil.bytesToHex(input), validator));
            splits = Arrays.asList(pattern.split(inputStr)).iterator();
        }

        hasNext = splits.hasNext();
        if (hasNext)
        {
            String nextStr = splits.next();
            next = ByteBufferUtil.bytes(nextStr);
        }
        else
        {
            next = null;
        }
        return hasNext;
    }

    public void reset(ByteBuffer input)
    {
        this.next = null;
        this.hasNext = false;
        this.input = input;
        this.splits = null;
    }

    private FilterPipelineTask getFilterPipeline()
    {
        FilterPipelineBuilder builder = new FilterPipelineBuilder(new BasicResultFilters.NoOperation());
        return builder.build();
    }
}

