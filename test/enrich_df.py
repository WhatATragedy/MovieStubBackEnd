import requests
import pandas

def get_movie_image(movie_row):
    # https://www.omdbapi.com/?t=Dead+End&y=2003&apikey=17000b52
    # print(movie_row)
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
    r = requests.get("https://www.omdbapi.com", params=params)
    data = r.json()
    if data.get("Error"):
        raise ValueError(f"Might have reached daily limit! {data}")

        movie_row['url'] = None
        movie_row['metascore'] = None
        movie_row['imdb'] = None
    else:
        movie_row['url'] = data.get("Poster")
        movie_row['metascore'] = data.get("Metascore")
        movie_row['imdb'] = data.get("imdbRating")
    print(f'Just Enriched {movie_row["movie_title"]} with IMDB: {data.get("imdbRating")} and url {data.get("Poster")}')
    return movie_row


# df = pandas.read_csv("../archive/rotten_tomatoes_movies.csv").head(100000)
base_df = pandas.read_csv("../archive/rotten_tomatoes_movies.csv")
print(base_df)

enriched = pandas.read_csv("../archive/enriched.csv")
# enriched = pandas.DataFrame(columns=['rotten_tomatoes_link'])
print(enriched)
print("Loaded in Pandas")

common = base_df.merge(enriched,on=['rotten_tomatoes_link'])
print(f"There are {common.shape[0]} common items...")
tobe_enriched = base_df[(~base_df.rotten_tomatoes_link.isin(common.rotten_tomatoes_link))]
tobe_enriched = tobe_enriched.sample(frac=1).reset_index(drop=True).head(10000)

print(f"There are {tobe_enriched.shape[0]} items to be enriched...")
print(tobe_enriched.head(10))
df = tobe_enriched.apply(get_movie_image, axis=1)

print("Done Enriching, Saving...")

new_enriched = pandas.concat([df, enriched], ignore_index=True)
print("Here are the newly enriched dataframes")
print(new_enriched.head(10))
print(new_enriched.columns)

new_enriched.to_csv("../archive/enriched.csv", index=False)

print("Done Saving..")


