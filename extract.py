import json
import pandas as pd
import datetime
import uuid
import numpy as np

def _solution_length(solution):
    if solution is None:
        return 0
    if type(solution) == float:
        return -1
    else:
        return

def time_limit_filter(df, millisecond_limit):
    start = df['completedDate'].min()
    middle = start + millisecond_limit
    end = df['completedDate'].max()

    before_cutoff = df[(df['completedDate'] >= start) & (df['completedDate'] <= middle)]
    after_cutoff = df[(df['completedDate'] > middle) & (df['completedDate'] <= end)]

    return before_cutoff, after_cutoff

def feature_extractor(df):
    df = df.copy(deep=True)
    feature_dict = {}

    feature_dict['num_exercises'] = len(df)
    feature_dict['num_distinct_exercises'] = len(df['name'].unique())
    feature_dict['num_active_days'] = len(df['completedDate'].map(lambda x: datetime.datetime.fromtimestamp(x/1000).date()).unique())
    feature_dict['num_distinct_modules'] = len(df['name'].map(lambda x: x[:x.find(':')]).unique())
    feature_dict['num_codepen_uses'] = len(df['solution'].map(lambda x: 0 if x is None or type(x) == float else x.startswith('https://codepen.io')).unique())

    solution_lengths = df['solution'].map(_solution_length)
    feature_dict['min_solution_length'] = solution_lengths.min()
    feature_dict['max_solution_length'] = solution_lengths.max()
    feature_dict['avg_solution_length'] = solution_lengths.mean()
    feature_dict['med_solution_length'] = np.median(solution_lengths)
    feature_dict['std_solution_length'] = solution_lengths.std()
    feature_dict['num_empty_solutions'] = (solution_lengths == 0).sum()
    feature_dict['num_unsolved'] = (solution_lengths == -1).sum()

    time_splits = np.diff(df['completedDate'])
    feature_dict['min_sec_between_solutions'] = time_splits.min()
    feature_dict['max_sec_between_solutions'] = time_splits.max()
    feature_dict['avg_sec_between_solutions'] = time_splits.mean()
    feature_dict['med_sec_between_solutions'] = np.median(time_splits)
    feature_dict['std_sec_between_solutions'] = time_splits.std()

    return feature_dict

def readline(line_string):
    line_string = line_string.strip()
    if line_string[-1] == ',':
        line_string = line_string[:-1]

    exercises = json.loads(line_string)
    return pd.DataFrame(exercises)

if __name__ == '__main__':
    with open('sample_line.txt') as stream:
        for line in stream:
            df = readline(line)
            before, after = time_limit_filter(df, 1000*60*60*24*7)
