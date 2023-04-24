#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
from fastapi import FastAPI
from pymongo import MongoClient
from bson import ObjectId

app = FastAPI()

# replace these values with your own Mongo Cloud connection string and database/collection names
mongo_uri = "mongodb+srv://tuansoi19127084:tuansoi19127084@cluster0.n8shx9d.mongodb.net/recommend?retryWrites=true&w=majority"
mongo_db_name = "Recommend"
mongo_collection_name = "job"

@app.on_event("startup")
async def startup_event():
    global mongo_client
    mongo_client = MongoClient(mongo_uri)

@app.get("/search-jobs")
async def search_jobs(keyword: str):
    # read data from MongoDB collection
    mongo_db = mongo_client[mongo_db_name]
    jobs_collection = mongo_db[mongo_collection_name]
    cursor = jobs_collection.find()
    jobs = list(cursor)

    # convert ObjectId objects to strings
    for job in jobs:
        job["_id"] = str(job["_id"])

    # convert to pandas dataframe
    jobs_df = pd.DataFrame(jobs)

    # filter by keyword
    matches = []
    for index, row in jobs_df.iterrows():
        if keyword.lower() in ''.join(map(str, row)).lower():
            matches.append(True)
        else:
            matches.append(False)
    jobs = jobs_df[matches].reset_index(drop=True)

    # return results
    if len(jobs) == 0:
        return {"message": "Không tìm thấy công việc phù hợp."}
    else:
        return jobs.to_dict(orient="records")

