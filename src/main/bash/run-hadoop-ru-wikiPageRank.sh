#!/bin/bash
hadoop jar HadoopWikiPageRank-1.0-jar-with-dependencies.jar ru.vasily.shad.parallel.wikipagerank.PageRank \
-D reducers=70 -D maxIterations=50 \
/data/wiki/ru/articles-markup /user/kolpakov/wikiPageRank-ru
