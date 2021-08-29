import requests
import pandas
import swifter

def get_movie_image(movie_row):
    # https://www.omdbapi.com/?t=Dead+End&y=2003&apikey=17000b52
    try:
        release_date = movie_row['original_release_date'].split("-")[0]
    except Exception as e:
        movie_row['url'] = None
        movie_row['metascore'] = None
        movie_row['imdb'] = None
        return movie_row
        
    params = {
        "apikey": "17000b52",
        "t": movie_row['movie_title'],
        "y": release_date
    }
    print(params)
    r = requests.get("http://www.omdbapi.com", params=params)
    data = r.json()
    if data.get("Error"):
        movie_row['url'] = None
        movie_row['metascore'] = None
        movie_row['imdb'] = None
    else:
        movie_row['url'] = data.get("Poster")
        movie_row['metascore'] = data.get("Metascore")
        movie_row['imdb'] = data.get("imdbRating")
    return movie_row


# df = pandas.read_csv("../archive/rotten_tomatoes_movies.csv").head(100000)
df = pandas.read_csv("../archive/rotten_tomatoes_movies.csv").head(10000)

enriched = pandas.read_csv("../archive/enriched.csv")
print("Loaded in Pandas")

common = df.merge(enriched,on=['rotten_tomatoes_link'])
tobe_enriched = df[(~df.rotten_tomatoes_link.isin(common.rotten_tomatoes_link))]

print(f"About to enrich {df.shape[0]} records")


# test = get_movie_image("Dead End", "2003-01-19")
# df = tobe_enriched.swifter.progress_bar(True).apply(get_movie_image, axis=1)
df = tobe_enriched.apply(get_movie_image, axis=1)

print("Done Enriching, Saving...")

new_enriched = pandas.concat([df, enriched], ignore_index=True)

new_enriched.to_csv("../archive/enriched.csv")

print("Done Saving..")


