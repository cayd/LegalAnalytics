from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
import numpy as np
import pandas as pd

document = []
scr = []
with open('supreme_court_opinions.json') as json_file:
    scr = json.load(json_file)
print("loading complete. splitting data")
    
test = []
for i in range(len(scr)):
    if i % 10 == 0:
        test.append(scr[i]["opinion"])
    else:
        document.append(scr[i]["opinion"])


print(len(document), " training examples and ", len(test), " test examples")
vectorizer = TfidfVectorizer(stop_words='english')
X = vectorizer.fit_transform(document)

print("vectorizer fitted")

true_k = 2
model = KMeans(n_clusters=true_k, init='k-means++', max_iter=100, n_init=1)
model.fit(X)

print("model fitted")

order_centroids = model.cluster_centers_.argsort()[:, ::-1]
terms = vectorizer.get_feature_names()

for i in range(true_k):
    print(“Cluster %d:” % i)
    for ind in order_centroids[i, :10]:
        print(‘ %s’ % terms[ind])

print(“\n”)
print(“Prediction”)
X = vectorizer.transform(test[:1])
predicted = model.predict(X)
print(predicted)
