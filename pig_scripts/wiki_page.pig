
REGISTER ../simlanpro.jar;
DEFINE PARSE_XMLWIKIPAGE wiki.pig.udf.ParseXmlWikiPage();
DEFINE GET_HSK_LEVELS wiki.pig.udf.GetHskLevelsOfText();
REGISTER WikiExtractor.py using jython as wikiExtractor;

page = load '/wiki/znwiki/' using org.apache.pig.piggybank.storage.XMLLoader('page') as (pageContent: chararray);
-- page = load '/wiki/znwiki_sample/' using org.apache.pig.piggybank.storage.XMLLoader('page') as (pageContent: chararray);

extractedPages = FOREACH page GENERATE FLATTEN(PARSE_XMLWIKIPAGE(pageContent)) ;
normalWikiPages = FILTER extractedPages BY isSpecialPage == false;
parsedPages = FOREACH normalWikiPages GENERATE title, GET_HSK_LEVELS(wikiExtractor.clean(text, id));

STORE parsedPages INTO '/wiki/zn_page_output_1s' USING PigStorage (',');
-- dump parsedPages;



