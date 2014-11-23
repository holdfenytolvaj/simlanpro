package subtitle.pig.input;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import subtitle.hadoop.input.SrtInputFormat;

public class SrtLoader extends LoadFunc {
    private static final Log LOGGER = LogFactory.getLog(SrtLoader.class);

    private RecordReader<Text, Text> reader;

    @Override
    public InputFormat<Text, Text> getInputFormat() {
        return new SrtInputFormat();
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = reader;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            if (!reader.nextKeyValue()) {
                return null;
            }

            Tuple tuple = createTuple(reader.getCurrentKey().toString(), reader.getCurrentValue().toString());
            return tuple;
        } catch (InterruptedException e) {
            LOGGER.error(e);
            return null;
        }
    }

    public Tuple createTuple(String key, String value) {
        return TupleFactory.getInstance().newTuple(Arrays.asList(new DataByteArray(key), new DataByteArray(value)));
    }
}