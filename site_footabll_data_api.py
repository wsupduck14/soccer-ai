import pandas as pd
from time import sleep, perf_counter
import requests
import json


def main() -> tuple[pd.DataFrame, pd.DataFrame]:

    league_data = pd.DataFrame()
    full_league_data = pd.DataFrame()

    season_starts = range(2018, 2024)
    # season_starts = [2012]
    leagues = ['PL', 'ELC', 'FL1', 'BL1', 'SA', 'DED', 'PP:', 'PD', 'CL', 'EC',]
    # leagues = ['PL']
    failed_requests = pd.DataFrame()

    headers = {'X-Auth-Token': '3ad6692b267440feb8286d30fb7e38ce'}

    for league in leagues:
        for year in season_starts:
            start = perf_counter()

            uri = f'https://api.football-data.org/v4/competitions/{league}/matches/?season={year}'

            try:
                season_match_data, response = pull_football_data_org_matches_api(uri, headers)
            except:
                print(response.json())  # type: ignore
                fail = pd.DataFrame({'league': [league], 'year': [year]})
                print(f'failed {league}, {year}')
                failed_requests = pd.concat([failed_requests, fail], ignore_index=True)
                season_match_data = pd.DataFrame()

            if response.status_code == 200:  # type: ignore
                print(f'success {league}, {year}')
            else:
                print(response.json())  # type: ignore
                fail = pd.DataFrame({'league': [league], 'year': [year]})
                print(f'failed {league}, {year}')
                failed_requests = pd.concat([failed_requests, fail], ignore_index=True)
                season_match_data = pd.DataFrame()

            league_data = pd.concat([league_data, season_match_data]).reset_index(drop=True)
            end = perf_counter()

            # allowed 10 requests/min, allow some wiggle room just in case. Could do this smarter with the data from the response but eh
            sleep(7-(end-start))
        full_league_data = pd.concat([full_league_data, league_data]).reset_index(drop=True)
    return full_league_data, failed_requests


def unpack_refs(input_df: pd.DataFrame, ) -> pd.DataFrame:
    input_df = input_df.explode('referees').reset_index(drop=True)
    ref_df = pd.json_normalize(input_df['referees'])  # type: ignore
    non_specific_cols = ref_df.columns

    for col in non_specific_cols:

        ref_df['referee.'+col] = ref_df[col]
    # only care about main match official for nwo
    ref_df = ref_df[ref_df['referee.type'] == 'REFEREE'].drop(columns=non_specific_cols).drop(columns='referee.type')
    output_df = pd.concat([input_df.drop(columns='referees'), ref_df], axis=1)
    # will deal with missing and duplicated data manually
    return output_df.groupby('id').agg(list).reset_index()


def pull_football_data_org_matches_api(uri: str, headers: dict) -> tuple[pd.DataFrame, requests.Response]:
    response = requests.get(uri, headers=headers)
    if response.status_code == 200:
        season_match_data = pd.json_normalize(response.json()['matches'])
        drop_cols = season_match_data.columns[season_match_data.columns.str.contains(
            '|'.join(['crest', 'flag', 'emblem', 'area.name', 'odds', 'lastUpdated', 'season.endDate', 'season.winner', 'season.currentMatchday']))]
        season_match_data = season_match_data.drop(columns=drop_cols)
        if season_match_data[season_match_data['status'] != 'FINISHED'].shape[0] > 0:
            print(season_match_data['status'].value_counts())
            season_match_data = season_match_data[season_match_data['status'] == 'FINISHED']
        ref_df = unpack_refs(season_match_data[['id', 'referees']])
        season_match_data = pd.merge(left=season_match_data.drop(columns='referees'), right=ref_df, on='id', validate='1:1')
    else:
        season_match_data = pd.DataFrame()

    return season_match_data, response


if __name__ == '__main__':
    full_match_data, failed_requests = main()
    full_match_data.sort_values(['competition.code', 'utcDate']).reset_index(drop=True).to_parquet('try_the_api.parquet')

    failed_requests = failed_requests.drop_duplicates().sort_values(['league', 'year']).reset_index(drop=True)
    print(failed_requests)
    failed_requests.to_parquet('failed_requests.parquet')
