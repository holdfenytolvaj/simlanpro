package wiki.hadoop.input;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import wiki.hadoop.wordcount.WordsInWikiPageMapper;

public class WikiInputFormat extends FileInputFormat<LongWritable, Text> {
	private static final Log LOGGER = LogFactory.getLog(WordsInWikiPageMapper.class);

	public static final String NUMBER_OF_WIKIPAGE_PER_FILESPLIT = "wiki.numberOfWikiPagesPerFileSplit";
	public static final Integer DONT_SPLIT = Integer.MAX_VALUE;
	public static final Integer SPLIT_PER_CORES = 0;

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		List<InputSplit> splits = new ArrayList<>();
		List<FileStatus> fileStatuses = listStatus(job);
		int numberOfWikiPagesPerFileSplit = job.getConfiguration().getInt(NUMBER_OF_WIKIPAGE_PER_FILESPLIT, DONT_SPLIT);

		for (FileStatus file : fileStatuses) {

			if (numberOfWikiPagesPerFileSplit == DONT_SPLIT) {
				LOGGER.info("Dont split file.");
				splits.add(new FileSplit(file.getPath(), 0, file.getLen(), null));
				continue;
			}
			else if (numberOfWikiPagesPerFileSplit == SPLIT_PER_CORES) {
				LOGGER.info("Split file per cores ");
				int numberOfCores = Runtime.getRuntime().availableProcessors();
				long chunkSize = file.getLen() / numberOfCores;
				for (int i = 0; i < numberOfCores; i++) {
					splits.add(new FileSplit(file.getPath(), i * chunkSize, (i == numberOfCores - 1) ? file.getLen() : (i + 1) * chunkSize, null));
					LOGGER.info("Splitting " + file.getPath() + " to [" + (i * chunkSize) + ", " + ((i == numberOfCores - 1) ? file.getLen() : (i + 1) * chunkSize) + "]");
				}
				continue;
			}
			else {
				LOGGER.info("Split file per number of pages: " + numberOfWikiPagesPerFileSplit);
				splits = splitDumpToExactNumberOfWikiPages(job, file);
			}
		}
		return splits;
	}

	private List<InputSplit> splitDumpToExactNumberOfWikiPages(JobContext job, FileStatus file) throws IOException {
		List<InputSplit> splits = new ArrayList<>();
		CompressionCodecFactory codecFactory = new CompressionCodecFactory(job.getConfiguration());
		FileSystem fs = file.getPath().getFileSystem(job.getConfiguration());
		InputStream inputStream;
		int numberOfWikiPagesPerFileSplit = job.getConfiguration().getInt(NUMBER_OF_WIKIPAGE_PER_FILESPLIT, DONT_SPLIT);

		if (file.getPath().getName().endsWith(".bz2") || file.getPath().getName().endsWith(".gz")) {
			CompressionCodec codec = codecFactory.getCodec(file.getPath());
			inputStream = codec.createInputStream(fs.open(file.getPath()));
		} else if (file.getPath().getName().endsWith(".xml")) {
			inputStream = fs.open(file.getPath());
		} else {
			throw new NotImplementedException("Not expected file format for " + file.getPath().getName() + " only xml, bz2, gz are supported.");
		}

		long pos = 0;
		long skippedBytes;
		BlockLocation[] blockLocations = fs.getFileBlockLocations(file, 0, file.getLen());
		while ((skippedBytes = WikipediaXmlFileSlicer.getEndPositionForSplit(inputStream, numberOfWikiPagesPerFileSplit)) != -1) {
			splits.add(new FileSplit(file.getPath(), pos, pos + skippedBytes, blockLocations[getBlockIndex(blockLocations, pos)].getHosts()));
			pos += skippedBytes;
		}

		return splits;
	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		return new WikiPageRecordReader();
	}
}
