package wiki.hadoop.input;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.OutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WikiPageRecordReader extends RecordReader<LongWritable, Text> {
	//private static final Log LOGGER = LogFactory.getLog(WordsInWikiPageMapper.class);

	private static AtomicLong numberOfWikiPageRead = new AtomicLong();

	private FileSplit fileSplit;
	private InputStream is;
	private long lengthRead;
	private FileSystem fs;
	private TaskAttemptContext taskAttemptContext;

	private LongWritable currentKey;
	private Text currentValue;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		fileSplit = (FileSplit) inputSplit;
		this.taskAttemptContext = taskAttemptContext;
		fs = fileSplit.getPath().getFileSystem(taskAttemptContext.getConfiguration());

		if (fileSplit.getPath().getName().endsWith(".bz2") || fileSplit.getPath().getName().endsWith(".gz")) {
			CompressionCodec codec = new CompressionCodecFactory(taskAttemptContext.getConfiguration()).getCodec(fileSplit.getPath());
			is = codec.createInputStream(fs.open(fileSplit.getPath()));
		} else if (fileSplit.getPath().getName().endsWith(".xml")) {
			is = fs.open(fileSplit.getPath());
		} else {
			throw new NotImplementedException("Not expected file format for " + fileSplit.getPath().getName() + " only xml, bz2, gz are supported.");
		}

		is.skip(fileSplit.getStart());
	}

	/** 
	 * It is possible that the end of the page is over the specified chunk size (fileSplit.getLength) ,
	 * this is intentional (for easy split of the wikiFile) 
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if (lengthRead >= fileSplit.getLength()) {
			return false;
		}

		OutputBuffer buf = new OutputBuffer();
		long lengthOfThePage;
		if ((lengthOfThePage = WikipediaXmlFileSlicer.getNextPageContent(is, buf)) == -1) {
			currentKey = null;
			currentValue = null;
			return false;
		}
		lengthRead += lengthOfThePage;

		long numOfWikiPageRead = numberOfWikiPageRead.incrementAndGet();

		currentKey = new LongWritable(numOfWikiPageRead);
		currentValue = new Text(buf.getData());

		if (numOfWikiPageRead % 1000 == 0) {
			System.out.println("Read " + numOfWikiPageRead + " wikipedia articles");
		}

		return true;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return currentValue;
	}

	@Override
	public float getProgress() throws IOException {
		return (float) Math.min(1.0, lengthRead / fileSplit.getLength());
	}

	@Override
	public void close() throws IOException {
		taskAttemptContext.setStatus("Closed RecordReader");
		System.out.println("Pages read: " + numberOfWikiPageRead.get());
		is.close();
	}
}
