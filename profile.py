import pandas as pd
import numpy as np
from typing import List
from fastapi import FastAPI
from pymongo import MongoClient
from bson.objectid import ObjectId
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

app = FastAPI()

# Connect to MongoDB Atlas
client = MongoClient("mongodb+srv://tuansoi19127084:tuansoi19127084@cluster0.n8shx9d.mongodb.net/?retryWrites=true&w=majority")
db = client.Recommend
job_collection = db.job
user_collection = db.user
user_history_collection = db.user_history

# Load data into pandas dataframes
job_TOPCV = pd.DataFrame(list(job_collection.find()))
users_CV = pd.DataFrame(list(user_collection.find()))
users_CV_history = pd.DataFrame(list(user_history_collection.find()))

users_CV['Position'] = users_CV['Position'].fillna('')
users_CV['Skill'] = users_CV['Skill'].fillna('')
users_CV['YearsExperience'] = users_CV['YearsExperience'].fillna('').astype(str)
users_CV['Position'] = users_CV['Position'] + users_CV['Skill'] + users_CV['YearsExperience']
tf = TfidfVectorizer(analyzer='word',ngram_range=(1, 2),min_df=0, stop_words='english')
tfidf_matrix = tf.fit_transform(users_CV['Position'])
cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)
users_CV = users_CV.reset_index()
userid = users_CV['id']
indices_3 = pd.Series(users_CV.index, index=users_CV['id'])

def get_recommendations_userwise_2(userid):
    idx = indices_3[userid]
    sim_scores = list(enumerate(cosine_sim[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    user_indices = [i[0] for i in sim_scores]
    return user_indices[0:5]

def get_job_id_3(userid_list):
    sorted_history = users_CV_history.sort_values(by=["times"], ascending=False)
    jobs_userwise = sorted_history['user_id'].isin(userid_list)
    df1 = sorted_history[jobs_userwise][['job_id', 'user_id', 'times']]
    joblist = df1['job_id'].tolist()
    Job_list = job_TOPCV['job_id'].isin(joblist)
    df_temp = job_TOPCV[Job_list][['job_id', 'title', 'Mô tả công việc']]
    
    #merge
    merged_df = pd.merge(df_temp, df1, on="job_id")
    sorted_df = merged_df.sort_values(by=["times"], ascending=False)
    return sorted_df

@app.get('/recommend_jobs/{user_id}')
async def recommend_jobs(user_id: int) -> List[dict]:
    user_indices = get_recommendations_userwise_2(user_id)
    jobs_df = get_job_id_3(user_indices)
    recommended_jobs = []
    for index, row in jobs_df.iterrows():
        recommended_jobs.append({
            "job_id": row["job_id"],
            "title": row["title"],
            "Mô tả công việc": row["Mô tả công việc"],
            "times": row["times"]
        })
    return recommended_jobs