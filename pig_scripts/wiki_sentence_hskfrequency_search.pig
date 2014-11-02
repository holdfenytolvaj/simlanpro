
sentences = load '/wiki/zn_sentence_output_3' using PigStorage('\t') as (title, hsk1:int, hsk2:int, hsk3:int, hsk4:int, hsk5:int, hsk6:int, nonhsk:int, wordcount:int, sentence);

-- long = filter sentences by wordcount > 3;
b = FOREACH sentences GENERATE title.., 
(100*hsk1/wordcount) as hsk1p,
(100*(hsk1+hsk2)/wordcount) as hsk2p,
(100*(hsk1+hsk2+hsk3)/wordcount) as hsk3p,
(100*(hsk1+hsk2+hsk3+hsk4)/wordcount) as hsk4p,
(100*(hsk1+hsk2+hsk3+hsk4+hsk5)/wordcount) as hsk5p,
(100*(hsk1+hsk2+hsk3+hsk4+hsk5+hsk6)/wordcount) as hsk6p;
c = filter b by hsk4p > 80;
dump c;

