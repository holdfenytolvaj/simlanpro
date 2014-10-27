=======================
Simple language project
=======================


--- Current state: ---------------------------------------------------------

The basic idea is to find contents (this time) on Wikipedia with limited vocabulary.
E.g. an article that only uses HSK4 words (with e.g. less then 10% HSK5 and above words)
This project so far uses hadoop for word frequency count and pig for classification.

--- The dream: -------------------------------------------------------------

A user can set his/her language level (e.g. 1000 words) and even better a subject
then this program suggest a longer content using only few new words, possible
even give a vocabulary list for the new words. Keep track of the learnt words.
It also works with daily sentences. This can be done with any languages although
beside English and Chinese a stemmer is needed. 

--- What is next: ----------------------------------------------------------

There are many possible next steps:

- add a crawler to search on news sites / socail sites (like weibo)
- extract sentences and classify it 
- create an android app for daily sentences
- refine the code (still many issues)
- do it similar for english, check simple english wikipedia as well


--- Thanks for --------------------------------------------------------------

Some resources that's been used:

Wikipedia http://dumps.wikimedia.org/

hadoop (http://hadoop.apache.org)

pig (http://pig.apache.org)

wikixmlj (https://code.google.com/p/wikixmlj/)

WikiExtractor http://medialab.di.unipi.it/wiki/Wikipedia_Extractor

Chinese word frequency:

http://expsy.ugent.be/subtlex-ch/

HSK words:

http://hskhsk.pythonanywhere.com/hskwords 



  
