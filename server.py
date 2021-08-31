import flask
import json
from flask import jsonify
from flask_cors import CORS, cross_origin
import signal
import sys
import pandas
import numpy as np
import time
import requests
import re
from flask import request

app = flask.Flask(__name__)
app.config["DEBUG"] = True
# cors = CORS(app, resources={r"/todos/*": {"origins": "*"}})
app.config['CORS_HEADERS'] = 'Content-Type'

# df = pandas.read_csv("archive/rotten_tomatoes_movies.csv")
df = pandas.read_csv("archive/enriched.csv")

@app.route('/movies/genres', methods=['GET'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def get_genres():
    genres = set()
    genre_tags = df['genres'].tolist()
    for tag in genre_tags:
        if not isinstance(tag, str):
            continue
        for genre in re.findall(r"[\w'& ]+", tag):
            
            genres.add(genre)
    genres = list(genres)
    print(len(genres))
    return json.dumps(genres)

@app.route('/reviews/movie/<movie_id>', methods=['GET'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def get_movie_id(movie_id):
    rows = df.loc[df['rotten_tomatoes_link'] == f"m/{movie_id}"]
    print(rows)
    return json.dumps(rows.to_dict(orient="records"))

@app.route('/reviews/genres/<genre>', methods=['GET', 'POST'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def get_genre_reviews(genre):
    print(request.data)
    if "date" in request.json:
        rows = df[(df['genres'].str.contains(genre, na=False)) & (df['original_release_date'] >= request.json.get("date"))]
        respone_df = pandas.DataFrame(rows).replace({np.nan:None})
    else:
        rows = df[df['genres'].str.contains(genre, na=False)]
        respone_df = pandas.DataFrame(rows).replace({np.nan:None})
    return jsonify(respone_df.to_dict(orient="records"))

    
    

def get_movie_image(movie_title, release_year):
    params = {
        "apikey": "17000b52",
        "t": movie_title,
        "y": release_year.split("-")[0]
    }


    r = requests.get("http://www.omdbapi.com", params=params)
    print(r)

if __name__ == "__main__":
    
    app.run()
