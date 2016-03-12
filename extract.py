import json
import pandas as pd
import datetime
import uuid
import numpy as np
import time

def _solution_length(solution):
    try:
        return len(solution)
    except:
        if solution is None:
            return 0
        elif isinstance(solution, (int, float, np.int64)):
            return -1
        else:
            raise Exception('Unknown input type: {}'.format(solution))

def time_limit_filter(df, millisecond_limit):
    start = int(df['completedDate'].min())
    middle = start + millisecond_limit
    end = int(df['completedDate'].max())

    before_cutoff = df[(df['completedDate'] >= start) & (df['completedDate'] <= middle)]
    after_cutoff = df[(df['completedDate'] > middle) & (df['completedDate'] <= end)]

    return before_cutoff, after_cutoff

def feature_extractor(df):
    df = df.copy(deep=True)
    feature_dict = {}

    if 'solution' not in df.columns:
        df['solution'] = ''
    df['solution'].fillna('', inplace=True)
    if 'name' not in df.columns:
        df['name'] = ''
    df['name'].fillna('', inplace=True)


    feature_dict['num_exercises'] = len(df)
    feature_dict['num_distinct_exercises'] = len(df['name'].unique())
    feature_dict['num_active_days'] = len(df['completedDate'].map(lambda x: datetime.datetime.fromtimestamp(x/1000).date()).unique())
    feature_dict['num_distinct_modules'] = len(df['name'].map(lambda x: x[:x.find(':')]).unique())
    # feature_dict['num_codepen_uses'] = len(df['solution'].map(lambda x: 0 if x is None or type(x) == float else x.startswith('https://codepen.io')).unique())

    solution_lengths = df['solution'].map(_solution_length)
    feature_dict['min_solution_length'] = solution_lengths.min()
    feature_dict['max_solution_length'] = solution_lengths.max()
    feature_dict['avg_solution_length'] = solution_lengths.mean()
    # feature_dict['med_solution_length'] = np.median(solution_lengths)
    feature_dict['std_solution_length'] = solution_lengths.std()
    feature_dict['num_empty_solutions'] = (solution_lengths == 0).sum()
    feature_dict['num_unsolved'] = (solution_lengths == -1).sum()

    time_splits = np.diff(sorted(df['completedDate'].tolist()))
    if len(time_splits) == 0:
        time_splits = np.array([-1])
    feature_dict['min_sec_between_solutions'] = time_splits.min()
    feature_dict['max_sec_between_solutions'] = time_splits.max()
    feature_dict['avg_sec_between_solutions'] = time_splits.mean()
    # feature_dict['med_sec_between_solutions'] = np.median(time_splits)
    feature_dict['std_sec_between_solutions'] = time_splits.std()

    return feature_dict

def readline(line_string):
    line_string = line_string.strip()
    if line_string[-1] == ',':
        line_string = line_string[:-1]

    exercises = json.loads(line_string)
    return pd.DataFrame(exercises)

if __name__ == '__main__':
    user_data = []
    with open('output.json') as stream:
        start_time = time.time()
        for line in stream:
            line = line.strip()
            if line == '[' or line == ']':
                continue
            df = readline(line)
            if not set(df.columns).issubset(set(['name', 'completedDate', 'solution'])):
                raise Exception(df.columns)
            before, after = time_limit_filter(df, 1000*60*60*24*14)
	    before = feature_extractor(before)
            before['after_days'] = len(after)
            user_data.append(before)
            if len(user_data) % 1000 == 0:
                print len(user_data)/100000.0, time.time() - start_time

    user_data = pd.DataFrame(user_data)
    user_data.to_csv('user_data.csv')
