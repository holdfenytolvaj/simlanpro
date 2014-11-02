
REGISTER ../simlanpro.jar;
DEFINE PARSE_XMLWIKIPAGE wiki.pig.udf.ParseXmlWikiPage();
DEFINE GET_HSK_LEVELS wiki.pig.udf.GetHskLevelsOfText();
DEFINE GET_SENTENCES wiki.pig.udf.GetSentencesFromChineseText();
REGISTER WikiExtractor.py using jython as wikiExtractor;

page = load '/wiki/znwiki/' using org.apache.pig.piggybank.storage.XMLLoader('page') as (pageContent: chararray);
--page = load '/wiki/znwiki_sample/' using org.apache.pig.piggybank.storage.XMLLoader('page') as (pageContent: chararray);

extractedPages = FOREACH page GENERATE FLATTEN(PARSE_XMLWIKIPAGE(pageContent)) ;
normalWikiPages = FILTER extractedPages BY isSpecialPage == false;
parsedPages = FOREACH normalWikiPages GENERATE title, wikiExtractor.clean(text,id) as pageContent;
extractSentencesWithBag = FOREACH parsedPages GENERATE title, GET_SENTENCES(pageContent) as sentences;
extractSentences = FOREACH extractSentencesWithBag GENERATE title, FLATTEN(sentences.$0) as s;
hskSentences = FOREACH extractSentences GENERATE title, FLATTEN(GET_HSK_LEVELS(s)), s;

STORE hskSentences INTO '/wiki/zn_sentence_output_4' USING PigStorage ('\t');
-- dump hskSentences;

