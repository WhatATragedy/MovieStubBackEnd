import flask
import json
from flask import jsonify
from flask_cors import CORS, cross_origin
import signal
import sys
import pandas
import numpy as np

import time

app = flask.Flask(__name__)
app.config["DEBUG"] = True
cors = CORS(app, resources={r"/todos/*": {"origins": "*"}})
app.config['CORS_HEADERS'] = 'Content-Type'

df = pandas.read_csv("archive/rotten_tomatoes_movies.csv")

@app.route('/reviews/movie/<movie_id>', methods=['GET'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def get_movie_id(movie_id):
    rows = df.loc[df['rotten_tomatoes_link'] == f"m/{movie_id}"]
    print(rows)
    return json.dumps(rows.to_dict(orient="records"))

@app.route('/reviews/genres/<genre>', methods=['GET'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def get_genre_reviews(genre):
    rows = df[df['genres'].str.contains(genre, na=False)]
    respone_df = pandas.DataFrame(rows).replace({np.nan:None})
    return jsonify(respone_df.to_dict(orient="records"))

if __name__ == "__main__":
    
    app.run()
