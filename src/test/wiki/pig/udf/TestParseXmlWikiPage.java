package wiki.pig.udf;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class TestParseXmlWikiPage {

	private static final EvalFunc<Tuple> xmlParser = new ParseXmlWikiPage();
	private static Tuple testTuple = TupleFactory.getInstance().newTuple(1);

	@Test
	public void testRegularPage() throws IOException {
		testTuple.set(0, "<page><title>SuperTitle</title><id>42</id><revision><text>Here it comes</text></revision></page>");
		Tuple result = xmlParser.exec(testTuple);

		//title
		assertTrue("SuperTitle".equals(result.get(0)));
		//id
		assertTrue("42".equals(result.get(1)));
		//text
		assertTrue("Here it comes".equals(result.get(2)));
		//isSpecialPage
		assertTrue(!(Boolean) result.get(3));
	}

	@Test
	public void testRedirectPage() throws IOException {
		testTuple.set(0, "<page><title>SuperTitle</title><id>42</id><revision><text>#REDIRECT [[infiniteloop]]</text></revision></page>");
		Tuple result = xmlParser.exec(testTuple);

		//title
		assertTrue("SuperTitle".equals(result.get(0)));
		//id
		assertTrue("42".equals(result.get(1)));
		//text
		assertTrue("#REDIRECT [[infiniteloop]]".equals(result.get(2)));
		//isSpecialPage
		assertTrue((Boolean) result.get(3));
	}

	@Test
	public void testSpecialPage() throws IOException {
		testTuple.set(0, "<page><title>Help:hereAndThere</title><id>42</id><revision><text>Nothing special here</text></revision></page>");
		Tuple result = xmlParser.exec(testTuple);

		//title
		assertTrue("Help:hereAndThere".equals(result.get(0)));
		//id
		assertTrue("42".equals(result.get(1)));
		//text
		assertTrue("Nothing special here".equals(result.get(2)));
		//isSpecialPage
		assertTrue((Boolean) result.get(3));
	}
}
