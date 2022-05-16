import pandas as pd

try:
    from opensearchpy import OpenSearch, helpers
    import pandas
    import os
    import sys

    print("loaded the packages successfully")
except:
    print("Some packages are missing")


def create_os_client():
    host = []
    auth = ("icopensearch", "")
    ca_certs_path = "~/Downloads/ca-certificates/cluster-ca-certificate.pem"

    client = OpenSearch(
        hosts=host,
        http_compress=True,
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        ca_certs=ca_certs_path,
    )
    return client


class OpenSearchCluster:
    def __init__(self):
        self.os_client = create_os_client()

    def upload(self, records):
        try:
            res = helpers.bulk(self.os_client, records)
        except Exception as e:
            print("{}".format(e))


def generate_json(list_of_json_data):
    for index, data in enumerate(list_of_json_data):
        yield {
            "_index": "netflix_movie_list",
            "_id": index,
            "_source": {
                "cast": data.get("cast", None),
                "country": data.get("country", None),
                "date_added": data.get("date_added", None),
                "description": data.get("description", None),
                "director": data.get("director", None),
                "listed_in": data.get("listed_in", None),
                "rating": data.get("rating", None),
                "release_year": data.get("release_year", None),
                "title": data.get("title", None),
                "type": data.get("type", None),
            },
        }


def main():
    df = pd.read_csv("./netflix_titles.csv")
    df = df.dropna()
    print(df.shape)

    json_data = df.to_dict(orient="records")

    generated_json_data = generate_json(json_data)

    os_client = OpenSearchCluster()
    os_client.upload(generated_json_data)
    print("load completed")


if __name__ == "__main__":
    main()
