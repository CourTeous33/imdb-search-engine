import sys
import io
import re
import nltk
import pandas as pd  

# nltk.download('stopwords',quiet=True)
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
punctuations = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''

stop_words = set(stopwords.words('english'))
input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='latin1')
stemmer = PorterStemmer()

header_skipped = False  # Flag to skip the header
for line in input_stream:
    if not header_skipped:
        header_skipped = True  # Skip the first line (header)
        continue

    # 1. Get doc id
    docid = line.split(',', 3)[0]
    line = ''.join(line.split(',', 3)[2:])

    # 2. Remove excess whitespace
    line = line.strip()
    line = re.sub(r'[^\w\s]', '', line)
    line = line.lower()

    # 3. Remove punctuation
    for x in line:
        if x in punctuations:
            line = line.replace(x, " ")

    words = line.split()

    # 4. Remove numbers
    words = [word for word in words if not word.isdigit()]

    for word in words:
        # 5. Stemming the words
        stemmed_word = stemmer.stem(word)
        
        # Print word, docid, 1 (count)
        print('%s\t%s\t%s' % (stemmed_word, docid, 1))