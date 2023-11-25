import pandas as pd
import re
import csv
import os
import time
import pyarrow
from datetime import date

data_file_directory = "C:\\Users\\Alec\\Documents\\Python\\soccer-ai\\data_files\\betting\\"
available_leagues_filepath = "C:\\Users\\Alec\\Documents\\Python\\soccer-ai\\data_files\\betting\\available_leagues.csv"


def _download_betting_info(all_leagues: list[dict], league_dl_selection: str, refresh_year: int):
    download_leagues = []
    failed_download_list = []
    success_league_list = []
    all_league_names = []

    for league in all_leagues:
        all_league_names.append(league['name'])

    for file in os.listdir(data_file_directory):
        if '.parquet' in file:
            success_league_list.append(file.split('.')[0])

    not_downloaded = list(set(all_league_names) - set(success_league_list))

    if league_dl_selection == 'y':
        for league in all_leagues:
            if league['name'] in not_downloaded:
                download_leagues.append(league)

    elif league_dl_selection == 'n':
        download_leagues = all_leagues

    else:
        try:
            download_leagues = [all_leagues[int(league_dl_selection)]]
        except Exception as failure:
            print('Invalid league selection')
            raise failure

    for league in download_leagues:
        try:
            download_league_bet_stats(league, refresh_year)
        except Exception as failure:
            print(failure)
            print(f'failed download for {league["name"]}')
            failed_download_list.append(league['name'])
    if len(failed_download_list) > 0:
        print(f'failed downloads for {failed_download_list}')


def get_all_available_leagues(available_leagues_filepath) -> list[dict]:
    with open(file=available_leagues_filepath, mode='r', encoding='utf=8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader)
        return [{"name": row[1], "url": row[2], "start_year": row[3]} for row in reader]


def download_league_bet_stats(league: dict, refresh_year: int = 0):
    league_name = league['name']
    raw_link = league['url']
    start_year = int(league['start_year'])
    if refresh_year != 0:
        start_year = refresh_year

    league_filepath = f'{data_file_directory}{league_name}.parquet'
    league_matches_bet_stats = pd.DataFrame()

    for year in range(start_year, int(date.today().year + 1)):
        year_key = f'{str(year)[2:]}{str(year + 1)[2:]}'
        download_link = raw_link.format(year_key)
        print(f'start download for {league_name}, {year}')
        try:
            tmp_df = pd.read_csv(download_link, encoding="ISO-8859-1")
            tmp_df['season'] = year + 1
            league_matches_bet_stats = pd.concat([league_matches_bet_stats, tmp_df])
        except:
            time.sleep(3)
            try:
                tmp_df = pd.read_csv(download_link, encoding="ISO-8859-1")
                tmp_df['season'] = year
                league_matches_bet_stats = pd.concat([league_matches_bet_stats, tmp_df])
            except:
                time.sleep(3)
                try:
                    tmp_df = pd.read_csv(download_link, encoding="ISO-8859-1")
                    tmp_df['season'] = year
                    league_matches_bet_stats = pd.concat([league_matches_bet_stats, tmp_df])
                except:
                    print(f'Failed download after 3 attempts {league_name}, {year}')
        if refresh_year != 0:
            break

    cols = ['Div', 'Date', 'Time', 'season', 'HomeTeam', 'AwayTeam', 'Referee', 'Attendance', 'FTHG', 'FTAG',
            'HTHG', 'HTAG', 'FTR', 'HTR', 'HS', 'AS', 'HST', 'AST', 'HHW', 'AHW', 'HC', 'AC', 'HF',
            'AF', 'HO', 'AO', 'HY', 'AY', 'HR', 'AR', 'HFKC', 'AFKC', 'MaxH', 'MaxD', 'MaxA',
            'AvgH', 'AvgD', 'AvgA', 'Max>2.5', 'Max<2.5', 'Avg>2.5', 'Avg<2.5', 'B365>2.5', 'B365<2.5']
    try:
        league_matches_bet_stats = league_matches_bet_stats[cols]
    except KeyError as error:
        error_str = str(error)
        missing_keys = set(re.findall("'(.*?)'", error_str))
        league_matches_bet_stats = league_matches_bet_stats[list(set(cols)-missing_keys)]

    league_matches_bet_stats = league_matches_bet_stats.rename(columns={
        'Div': 'div', 'Date': 'date', 'Time': 'time', 'Season': 'season', 'HomeTeam': 'home_team', 'AwayTeam': 'away_team',
        'Referee': 'referee', 'Attendance': 'attendance', 'FTHG': 'full_home_goals', 'FTAG': 'full_away_goals',
        'HTHG': 'half_home_goals', 'HTAG': 'half_away_goals', 'FTR': 'full_result', 'HTR': 'half_result', 'HS': 'home_shots',
        'AS': 'away_shots', 'HST': 'home_shots_target', 'AST': 'away_shots_target', 'HHW': 'home_shots_hit_woodwork',
        'AHW': 'away_shots_hit_woodwork', 'HC': 'home_corners', 'AC': 'away_corners', 'HF': 'home_fouls', 'AF': 'away_fouls',
        'HO': 'home_offsides', 'AO': 'away_offsides', 'HY': 'home_yellow', 'AY': 'away_yellow', 'HR': 'home_red', 'AR': 'away_red',
        'HFKC': 'home_free_kicks_conceded', 'AFKC': 'away_free_kicks_conceded', 'MaxH': 'market_max_home_win_odds',
        'MaxD': 'market_max_draw_win_odds', 'MaxA': 'market_max_away_win_odds', 'AvgH': 'market_avg_home_win_odds',
        'AvgD': 'market_avg_draw_win_odds', 'AvgA': 'market_avg_away_win_odds', 'Max>2.5': 'market_max_over_2.5_goals',
        'Max<2.5': 'market_max_under_2.5_goals', 'Avg>2.5': 'market_avg_over_2.5_goals', 'Avg<2.5': 'market_avg_under_2.5_goals',
        'B365>2.5': 'bet365_over_2.5_goals', 'B365<2.5': 'bet365_under_2.5_goals',
    },
    )
    league_matches_bet_stats['date'] = pd.to_datetime(league_matches_bet_stats['date'], format="mixed", dayfirst=True)

    if refresh_year != 0:
        existing_stats = pd.read_parquet(league_filepath)
        existing_stats = existing_stats[existing_stats['season'] != refresh_year]
        league_matches_bet_stats = pd.concat([league_matches_bet_stats, existing_stats])

    league_matches_bet_stats = league_matches_bet_stats.drop_duplicates()
    league_matches_bet_stats = league_matches_bet_stats.sort_values('date', ascending=False).reset_index(drop=True)

    try:
        league_matches_bet_stats.to_parquet(league_filepath, index=False)
    except pyarrow.lib.ArrowInvalid or pyarrow.lib.ArrowTypeError as fail:
        fail_columns = re.findall('column (.*?) with', str(fail))
        print(f'coercing columns for {league_name}, columns {fail_columns}')
        for col in fail_columns:
            league_matches_bet_stats[col] = pd.to_numeric(league_matches_bet_stats[col].str.findall('(\d+)'), errors='coerce')
        league_matches_bet_stats.to_parquet(league_filepath, index=False)

    print(f'finished downloading {league_name}')


if __name__ == '__main__':
    os.makedirs(data_file_directory, exist_ok=True)
    all_leagues = get_all_available_leagues(available_leagues_filepath)
    refresh_year = 0
    dl_league_pref = input('Download Missing Leagues? y/n :  ').lower()
    if dl_league_pref == 'n':
        for i, league in enumerate(all_leagues):
            print(f'Option {i} - {all_leagues[i]["name"]}')
        dl_league_pref = input('Download one specific league? Enter number from above or n again :  ')
        refresh_year = int(input('Refresh certain year? YYYY (e.g. 2023 for 2023/2024, 0 for No) :  '))

    _download_betting_info(all_leagues, dl_league_pref, refresh_year)
    complete_info = pd.DataFrame()
    if os.path.exists(data_file_directory+'all_downloaded_leagues.parquet'):
        os.remove(data_file_directory+'all_downloaded_leagues.parquet')
    for file in os.listdir(data_file_directory):
        if '.parquet' in file:
            complete_info = pd.concat([complete_info, pd.read_parquet(data_file_directory+file)])
    complete_info = complete_info.sort_values('date', ascending=False).reset_index(drop=True)
    complete_info.to_parquet(data_file_directory + 'all_downloaded_leagues.parquet')

    print('sucessfully created all_downloaded_leagues.parquet')
