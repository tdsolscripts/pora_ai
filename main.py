import os
import pandas as pd
from toloka.async_client.client import AsyncTolokaClient

import toloka.client as toloka
import requests
from datetime import datetime, timedelta
import time
from tqdm import tqdm
import json

import zipfile
import glob

from multiprocessing import Pool


OAUTH_TOKEN = 'AQAAAABZitNVAACtpeQGajz4zEowgRjy5TbY9_g'
HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}
toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')

URL_WORKER = 'https://toloka.dev/api/new/requester/workers/'
URL_API = "https://toloka.dev/api/v1/"

validator_df = pd.concat([pd.read_excel('Pora.Ai валидация.xlsx', sheet_name='Матвей Валидация Очки 1'),
                          pd.read_excel('Pora.Ai валидация.xlsx', sheet_name='Катя Валидация Макияж'),
                          pd.read_excel('Pora.Ai валидация.xlsx', sheet_name='Кристина Валидация Очки 2'),
                          pd.read_excel('Pora.Ai валидация.xlsx', sheet_name='Лиза Валидация Руки + Голов Убо'),
                          ])

validator_df.dropna(subset='assignment-id')

main_dirs_names = set()

def process_assignment(assignment_id):
    try:
        main_dir_name = validator_df.loc[validator_df['assignment-id']==assignment_id, 'project'].values[0]
        second_dir_name = validator_df.loc[validator_df['assignment-id']==assignment_id, 'category'].values[0]
        os.makedirs(os.path.join('photos', main_dir_name, second_dir_name), exist_ok=True)

        attachment_id = toloka_client.get_assignment(assignment_id).solutions[0].output_values['img_1']
        attachment_extension = toloka_client.get_attachment(attachment_id).name.split('.')[-1]

        full_path = os.path.join('photos', main_dir_name, second_dir_name, f'{assignment_id}.{attachment_extension}')

        if not os.path.exists(full_path):
            with open(full_path, 'wb') as file:
                toloka_client.download_attachment(attachment_id, out=file)

    except Exception as e:
        print(assignment_id, e)


if __name__ == '__main__':
    main_dirs_names = set()
    for assignment_id in validator_df['assignment-id'].dropna().unique():
        main_dir_name = validator_df.loc[validator_df['assignment-id']==assignment_id, 'project'].values[0]
        main_dirs_names.add(main_dir_name)

    with Pool() as pool:
        for _ in tqdm(pool.imap_unordered(process_assignment, validator_df['assignment-id'].dropna().unique()), total=len(validator_df['assignment-id'].dropna().unique())):
            pass
