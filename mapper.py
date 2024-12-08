#Code based on https://blog.devgenius.io/big-data-processing-with-hadoop-and-spark-in-python-on-colab-bff24d85782f
import sys
import io
import re
import nltk
import pandas as pd  

nltk.download('stopwords',quiet=True)
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
punctuations = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''

stop_words = set(stopwords.words('english'))
input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='latin1')
stemmer = PorterStemmer()

docid = 1 #actually line id
for line in input_stream:
  # 1. Read the text of the corpus
  line = line.split(',', 3)[2]
  # 4. Remove excess whitespace
  line = line.strip()
  line = re.sub(r'[^\w\s]', '',line)
  line = line.lower()
  # 3. Remove punctuation
  for x in line:
    if x in punctuations:
      line=line.replace(x, " ")
  
  words=line.split()
  words = [word for word in words if word not in stop_words]
  # 3. Remove number
  words = [word for word in words if not word.isdigit()]
  for word in words:
    # 4. Stemming the words
    stemmed_word = stemmer.stem(word)
    # 2. Remove stop-words
    if stemmed_word not in stop_words:
      # word, docid, 1
      print('%s\t%s\t%s' % (stemmed_word, docid, 1))
      
  docid +=1