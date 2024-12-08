# Code based on https://blog.devgenius.io/big-data-processing-with-hadoop-and-spark-in-python-on-colab-bff24d85782f
from operator import itemgetter
import sys

current_word = None
current_id = None
current_count = 0
word = None
current_word_docs = []

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    line=line.lower()

    # parse the input we got from mapper.py
    word, docid, count = line.split('	', 2)
    try:
      count = int(count)
    except ValueError:
      #count was not a number, so silently
      #ignore/discard this line
      continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word and current_id == docid:
        current_count += 1
    else:
        if current_word:
            # write result to STDOUT
            # output is the each word frequency in each document
            print ('%s\t%s\t%s' % (current_word, current_count, current_id))
        current_count = 1
        current_word = word
        current_id = docid

# do not forget to output the last word if needed!
if current_word == word:
    print( '%s\t%s\t%s' % (current_word, current_count, current_id))