import datetime
import json
import codecs
import os
import pandas as pd
import pickle
import requests

# import criteo_marketing as cm
# from criteo_marketing import Configuration
import yaml


def get_metrics(
    advertiser_id: str,
    start_date_input: datetime.datetime,
    end_date_input: datetime.datetime,
    metrics: list,
) -> pd.DataFrame:

    start_date = start_date_input.isoformat()
    end_date = end_date_input.isoformat()

    # ACCESS KEYS READING
    path_config = os.path.join(os.sep.join(__file__.split(os.sep)[:-1]), "config.yaml")
    with open(path_config) as f:
        config = yaml.safe_load(f)

    # reading token
    token_flag = False
    if os.path.isfile("token.pickle"):
        with open("token.pickle", "rb") as p:
            print("Загрузка token из файла pickle")
            token = pickle.load(p)
            token_flag = datetime.datetime.now() - token[
                "token_get_time"
            ] < datetime.timedelta(seconds=540)
            if not token_flag:
                print("Токен устарел")

    if not token_flag:
        # GETTING token
        print("Запрос на получение token")
        url_token = "https://api.criteo.com/oauth2/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data_urlencode = {
            "client_id": config["CLIENT_ID"],
            "client_secret": config["CLIENT_SECRET"],
            "grant_type": "client_credentials",
        }

        token = {}
        token["token_dict"] = requests.post(
            url=url_token, headers=headers, data=data_urlencode
        ).json()
        token["token_get_time"] = datetime.datetime.now()
        with open("token.pickle", "wb") as p:
            pickle.dump(token, p)

            # GETTING target data
    url_statistic = "https://api.criteo.com/2021-04/statistics/report"
    headers = {
        "Authorization": "Bearer " + token["token_dict"]["access_token"],
        "Accept": "text/plain",
        "Content-Type": "application/*+json",
    }

    post_body = {
        "advertiserIds": advertiser_id,
        "startDate": start_date,
        "endDate": end_date,
        "format": "json",
        "dimensions": ["AdsetId", "Day"],
        "metrics": metrics,
        "timezone": "PST",
        "currency": "USD",
    }

    print(
        f"Запрос на получение данных для advertiser_id:{advertiser_id}, метрики: "
        + " ".join(metrics)
    )
    r = requests.post(
        url_statistic,
        headers=headers,
        json=post_body,
    )
    print("Данные получены")
    decoded_data = codecs.decode(r.text.encode(), "utf-8-sig")
    json_data = json.loads(decoded_data)
    df_data = pd.DataFrame(json_data["Rows"])
    print(df_data.info())

    return df_data


if __name__ == "__main__":

    # Данные на вход запроса статистики Criteo
    advertiser_id = "18183"
    end_date_input = datetime.datetime.now() - datetime.timedelta(days=30)
    start_date_input = (
        end_date_input - datetime.timedelta(days=30) - datetime.timedelta(days=1)
    )
    # ALL METHRICS: https://developers.criteo.com/marketing-solutions/docs/metrics#full-list-of-metrics
    metrics = [
        "Displays",
        "Clicks",
        "AdvertiserCost",
        "Reach",
        "SalesPc1d",
        "RevenueGeneratedPc1d",
    ]

    get_metrics(advertiser_id, start_date_input, end_date_input, metrics)
