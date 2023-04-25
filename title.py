#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
from fastapi import FastAPI
from pymongo import MongoClient

app = FastAPI()

# Load job data from MongoDB
client = MongoClient("mongodb+srv://tuansoi19127084:tuansoi19127084@cluster0.n8shx9d.mongodb.net/test?retryWrites=true&w=majority")
db = client.Recommend
collection = db.job
df = pd.DataFrame(list(collection.find()))
df["Mô tả công việc"] = df["title"] + df["Mô tả công việc"] + df["skills"]
df = df[["title", "Mô tả công việc"]]

# Initialize TF-IDF vectorizer
tf = TfidfVectorizer(analyzer="word", ngram_range=(1, 2), min_df=0, stop_words="english")
tfidf_matrix = tf.fit_transform(df["Mô tả công việc"])
cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)
indices = pd.Series(df.index, index=df["title"])

@app.get("/job-recommendations")
async def get_recommendations(title: str):
    # Compute cosine similarity scores
    idx = indices[title]
    sim_scores = list(enumerate(cosine_sim[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    job_indices = [i[0] for i in sim_scores[1:11]]
    recommended_jobs = df.iloc[job_indices]["title"].tolist()

    return {"recommended_jobs": recommended_jobs}

