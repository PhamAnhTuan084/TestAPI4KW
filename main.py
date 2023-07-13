import os
import urllib.parse
from fastapi import HTTPException, status, Query
from requests.exceptions import ConnectionError, Timeout
from starlette.requests import Request
import urllib.parse
from paginate_sqlalchemy import SqlalchemyOrmPage
import math
import pandas as pd
import mysql.connector
from elasticsearch import Elasticsearch
from fastapi import FastAPI
from fastapi import Query
from pymongo import MongoClient
import random
import numpy as np
import warnings
import re
import json
import requests

app = FastAPI()

# Define Elasticsearch connection
es = Elasticsearch("http://localhost:9201")

index_name = "jobs_index"

# Đường dẫn đến tệp trên Google Drive
file_stpwd = "https://drive.google.com/uc?id=1AQrnIFnqzPQbXXbYRADj5yh1I3_E_YMt"

# Tên biến toàn cục để lưu trữ nội dung của tệp
stopwords_vn = None

def load_stopwords():
    global stopwords_vn

    if stopwords_vn is None:
        # Tải tệp từ URL nếu chưa được tải
        response = requests.get(file_stpwd)
        stopwords_vn = response.text.splitlines()

    return stopwords_vn

# Gọi hàm load_stopwords() để đảm bảo tệp đã được tải trước khi sử dụng
stop_words = load_stopwords()

file_url = "https://drive.google.com/uc?id=1kAK11AE9FIsLge78Ih9vzYCrGCxqOkOf"

df = pd.read_csv(file_url)


# Delete all data in Elasticsearch
def delete_all_data():
    response = es.indices.delete(index="_all")
    return response

delete_all_data()

# Create or update Elasticsearch index
index_mapping = {
    "settings": {
        "analysis": {
            "filter": {
                "vietnamese_stop": {
                    "type": "stop",
                    "stopwords": stop_words
                }
            },
            "analyzer": {
                "vietnamese_analyzer": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "vietnamese_stop"
                    ]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "job_id": {"type": "integer"},
            "Số lượng tuyển": {"type": "integer"},
            "Hình thức làm việc": {"type": "text"},
            "Cấp bậc": {"type": "text"},
            "Giới tính": {"type": "text"},
            "min_yoe": {"type": "integer"},
            "max_yoe": {"type": "integer"},
            "Mô tả công việc": {"type": "text"},
            "Yêu cầu ứng viên": {"type": "text"},
            "min_salary": {"type": "integer"},
            "max_salary": {"type": "integer"},         
        }
    }
}

if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=index_mapping)
else:
    es.indices.put_mapping(index=index_name, body=index_mapping['mappings'])

# Index job data into Elasticsearch
for _, row in df.iterrows():
    job_data = row.to_dict()
    # Convert skills to a list
    job_data['skills'] = [skill.strip() for skill in job_data['skills'].split(",")]
    es.index(index=index_name, body=job_data)


@app.get("/jobs")
def search_jobs(
    keyword: str = Query(None, description="Keyword to search for"),
    addresses: str = Query(None, description="addresses filter"),
    skill: str = Query(None, description="Skill filter"),
    categories: str = Query(None, description="Categories filter"),
    page: int = Query(1, description="Page number"),
    limit: int = Query(10, description="Number of results per page"),
):
    try:
        # Search for jobs in Elasticsearch
        body = {
            "size": limit,
            "from": (page - 1) * limit,
            "query": {
                "bool": {
                    "must": [
                        {"multi_match": {"query": keyword, "fields": ["*"]}}
                    ],
                    "filter": []
                }
            }
        }

        if addresses:
            body["query"]["bool"]["filter"].append({"match": {"addresses": addresses}})

        if skill:
            body["query"]["bool"]["filter"].append({"match": {"skills": skill}})

        if categories:
            decoded_categories = urllib.parse.unquote(categories)
            body["query"]["bool"]["filter"].append({"match": {"categories": decoded_categories}})

        result = es.search(index="jobs_index", body=body)
        hits = result["hits"]["hits"]

        max_score = result['hits']['max_score']
        min_score = max_score * 0.7
        print(min_score)

        jobs = []
        for hit in hits:
            job_info = hit["_source"]
            job_info["score"] = hit["_score"]
            jobs.append(job_info)

        filtered_jobs = [job for job in jobs if job["score"] > min_score]

        total = len(filtered_jobs)

        # Calculate pagination information
        total_pages = math.ceil(total / limit)
        base_url = f"http://localhost:8001/jobs?keyword={keyword}&addresses={addresses}&skill={skill}&categories={categories}"
        first_page_url = f"{base_url}&page=1"
        last_page = total_pages
        last_page_url = f"{base_url}&page={last_page}"
        next_page = page + 1 if page < total_pages else None
        prev_page = page - 1 if page > 1 else None

        links = [
            {
                "url": None,
                "label": "&laquo; Previous",
                "active": False
            },
            {
                "url": first_page_url,
                "label": "1",
                "active": page == 1
            }
        ]

        for i in range(2, total_pages + 1):
            links.append({
                "url": f"{base_url}&page={i}",
                "label": str(i),
                "active": page == i
            })

        links.append({
            "url": f"{base_url}&page={next_page}" if next_page else None,
            "label": "Next &raquo;",
            "active": False
        })

        if len(filtered_jobs) == 0:
            return {
                "error": False,
                "message": "Không tìm thấy công việc",
                "data": None,
                "status_code": 404
            }
        
        pagination_info = {
            "first_page_url": first_page_url,
            "from": (page - 1) * limit + 1,
            "last_page": last_page,
            "last_page_url": last_page_url,
            "links": links,
            "next_page_url": f"{base_url}&page={next_page}" if next_page else None,
            "path": f"http://localhost:8001/jobs?keyword={keyword}&addresses={addresses}&skill={skill}&categories={categories}",
            "per_page": limit,
            "prev_page_url": f"{base_url}&page={prev_page}" if prev_page else None,
            "to": min(page * limit, total),
            "total": total
        }

        return {
            "error": False,
            "message": "Xử lí thành công",
            "data": {
                "jobs": {
                    "current_page": page,
                    "data": filtered_jobs,
                    "pagination_info": pagination_info
                }
            },
            "status_code": 200
        }

    except (ConnectionError, TimeoutError, Timeout) as e:
        # Xử lý lỗi mạng
        return {
            "error": True,
            "message": "Lỗi mạng",
            "data": [],
            "status_code": 503
        }
    except (ValueError, TypeError) as e:
        # Xử lý lỗi đầu vào gây crash hoặc lỗi xử lý không mong muốn
        return {
            "error": True,
            "message": "Lỗi đầu vào gây crash",
            "data": [],
            "status_code": 400
        }
    except Exception as e:
        return {
            "error": True,
            "message": "Lổi đầu vào không hợp lệ/ Lỗi website đang gặp sự cố",
            "data": [],
            "status_code": 500
        }