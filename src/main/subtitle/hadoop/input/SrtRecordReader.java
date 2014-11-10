package subtitle.hadoop.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Pattern;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SrtRecordReader extends RecordReader<Text, Text> {
	//private static final Log LOGGER = LogFactory.getLog(SrtRecordReader.class);

	private FileSplit fileSplit;
	private BufferedReader br;

	private FileSystem fs;
	private TaskAttemptContext taskAttemptContext;

	private String filmName;
	int frameCounter;
	private Text currentKey;
	private Text currentValue;

	private static Pattern number = Pattern.compile("\uFEFF?\\d+");

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		fileSplit = (FileSplit) inputSplit;
		this.taskAttemptContext = taskAttemptContext;
		fs = fileSplit.getPath().getFileSystem(taskAttemptContext.getConfiguration());
		InputStream is = fs.open(fileSplit.getPath());

		if (fileSplit.getPath().getName().endsWith(".srt")) {
			br = new BufferedReader(new InputStreamReader(is));
			filmName = fileSplit.getPath().getName().replaceAll("\\.srt$", "");
			frameCounter = 1;
		} else {
			throw new NotImplementedException("Not expected file format for " + fileSplit.getPath().getName() + " only srt are supported.");
		}

		is.skip(fileSplit.getStart());
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder();
		String line;

		while ((line = br.readLine()) != null) {
			line = line.trim();
			if (line.isEmpty()) {
				continue;
			}
			break;
		}

		if (line == null) {
			currentKey = null;
			currentValue = null;
			return false;
		}

		//make it strict, to discover issues asap
		//--- frame number -------------------------------
		if (!number.matcher(line).matches()) {
			throw new NotImplementedException("Not expected line: " + line);
		}
		if (frameCounter == 1) {
			line = line.replaceAll("\uFEFF?", "");//some file starts with BOM
		}
		int numberOfFrame = Integer.parseInt(line);
		if (frameCounter != numberOfFrame) {
			if (frameCounter == 1 && numberOfFrame == 0) {
				//some file starts with 0 counter...
				frameCounter = 0;
			} else {
				throw new NotImplementedException("Not expected frame: " + line);
			}
		}

		//--- timing -------------------------------------
		br.readLine();//we can ignore this

		//--- text ---------------------------------------
		while ((line = br.readLine()) != null) {
			line = line.trim();

			if (line.isEmpty()) {
				break;
			}
			sb.append(line);
		}

		if (sb.length() == 0) {
			currentKey = null;
			currentValue = null;
			return false;
		}

		frameCounter++;

		currentKey = new Text(filmName);
		currentValue = new Text(sb.toString());

		if (frameCounter % 100 == 0) {
			System.out.println(filmName + " position " + frameCounter);
		}

		return true;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return currentValue;
	}

	@Override
	public void close() throws IOException {
		taskAttemptContext.setStatus("Closed RecordReader");
		br.close();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0; //we dont know...
	}
}
