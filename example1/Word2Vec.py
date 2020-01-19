import sys
import shutil
import tempfile
import urllib.request

from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec

url = sys.argv[1]

with urllib.request.urlopen(url) as response:
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        shutil.copyfileobj(response, tmp_file)

sc = SparkContext(appName='Word2Vec')
inp = sc.textFile(tmp_file.name).map(lambda row: row.split(" "))

word2vec = Word2Vec()
model = word2vec.fit(inp)

synonyms = model.findSynonyms('china', 40)

for word, cosine_distance in synonyms:
    print("{}: {}".format(word, cosine_distance))


# Local Variables:
# mode:python
# mode:outline-minor
# mode:auto-fill
# fill-column: 75
# coding: utf-8
# comment-column:50
# comment-start: "# "
# End:
