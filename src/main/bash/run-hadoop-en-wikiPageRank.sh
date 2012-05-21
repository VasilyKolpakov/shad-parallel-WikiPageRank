#!/bin/bash
hadoop jar HadoopWikiPageRank-1.0-jar-with-dependencies.jar ru.vasily.shad.parallel.wikipagerank.PageRank \
-D reducers=70 \
/data/wiki/en/articles-markup /user/kolpakov/wikiPageRank/output
