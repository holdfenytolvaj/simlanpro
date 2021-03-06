-- pig -x local -f subtitle_en_frequency.pig 
/*
Need the following libs in the path:
- stanford-corenlp-3.5.0-models.jar;
- stanford-corenlp-3.5.0.jar;
- joda-time
- jollyday
*/

REGISTER ../simlanpro.jar;
DEFINE GET_LANG_LEVELS subtitle.pig.udf.GetLanguageLevelOfTextEnByWord();

-- textByFrame = load '~/cirkalo/crawler/subtitles/en/sample/' using subtitle.pig.input.SrtLoader() as (filmName: chararray, content: chararray);
-- textByFrame = load '~/cirkalo/crawler/subtitles/en/all_1/' using subtitle.pig.input.SrtLoader() as (filmName: chararray, content: chararray);
textByFrame = load '/subtitle/en/sample2/' using subtitle.pig.input.SrtLoader() as (filmName: chararray, content: chararray);

levelsByFrame = FOREACH textByFrame GENERATE FLATTEN(GET_LANG_LEVELS(filmName, content));

levelsByFilm = group levelsByFrame by id;
levelsByFilmSum = FOREACH levelsByFilm GENERATE group,
                                                SUM(levelsByFrame.l1) as l1, 
                                                SUM(levelsByFrame.l2) as l2, 
                                                SUM(levelsByFrame.l3) as l3,
                                                SUM(levelsByFrame.l4) as l4,
                                                SUM(levelsByFrame.l5) as l5,
                                                SUM(levelsByFrame.rest) as rest,
                                                SUM(levelsByFrame.wordcount) as wordcount;

result = FOREACH levelsByFilmSum GENERATE group..,
                                (100*l1/wordcount) as l1p,
                                (100*(l1+l2)/wordcount) as l2p,
                                (100*(l1+l2+l3)/wordcount) as l3p,
                                (100*(l1+l2+l3+l4)/wordcount) as l4p,
                                (100*(l1+l2+l3+l4+l5)/wordcount) as l5p,
                                (100*(l1+l2+l3+l4+l5+rest)/wordcount) as l6p;

resultByl6  = order result by l6p;

-- STORE resultByl6 INTO '~/cirkalo/crawler/subtitles/en/out_1/' USING PigStorage ('\t');
STORE resultByl6 INTO '/subtitle/en/sample2_out3' USING PigStorage ('\t');



