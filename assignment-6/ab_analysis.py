import pandas as pd
import os
import sys
from scipy.stats import chi2_contingency
from scipy.stats import mannwhitneyu


def count_of_users_who_did_and_did_not_search(sample_users: pd.DataFrame):
    """
    @param sample_users: a DataFrame where each row contains a user id, boolean value indicating
    if the user is a teacher, the number of times a user logged in, and the number of times the
    user searched
    @return: a collection which contains the number of users who used the search function and
    the number of users who never searched
    """
    has_never_searched = sample_users['search_count'] < 1
    searched_more_than_once = sample_users[~has_never_searched]
    never_searched = sample_users[has_never_searched]
    return searched_more_than_once.index.size, never_searched.index.size


def separate_users_with_odd_and_even_uid(data: pd.DataFrame):
    """
    @param data: a DataFrame where each row contains a user id, boolean value indicating
    if the user is a teacher, the number of times a user logged in, and the number of times the
    user searched
    @return: two DataFrames, where the first has all the users with an even user id, and the second
    has all the users containing odd user ids
    """
    data_cpy = data.copy()
    has_even_id = data_cpy['uid'] % 2 == 0
    users_with_even_ids = data_cpy[has_even_id]
    users_with_odd_ids = data_cpy[~has_even_id]
    return users_with_even_ids, users_with_odd_ids


def prepare_search_usage_contingency_table(searches: pd.DataFrame):
    """
    @param searches: a DataFrame where each row contains a user id, boolean value indicating
    if the user is a teacher, the number of times a user logged in, and the number of times the
    user searched
    @return: a collection containing two rows, the first representing the users with even ids and
    the second representing the users with odd ids. Each row contains the number of users who
    searched more than once and the number of users who never searched
    """
    users_with_even_ids, users_with_odd_ids = separate_users_with_odd_and_even_uid(searches)

    even_id_searches, odd_id_searches = map(count_of_users_who_did_and_did_not_search,
                                            [users_with_even_ids, users_with_odd_ids])
    contingency_table = list(zip(even_id_searches, odd_id_searches))
    return contingency_table


def join_search_counts(even_id_users: pd.DataFrame, odd_id_users: pd.DataFrame):
    """
    @param even_id_users: a DataFrame containing only the users with an even user id
    @param odd_id_users: a DataFrame containing only the users with an odd user id
    @return: a collection with two rows, the first representing the number of
    times users with an even id used the search feature and the second representing the number of
    times users with an odd id used the search feature
    """
    even_id_search_counts = even_id_users['search_count'].values
    odd_id_search_counts = odd_id_users['search_count'].values
    return [even_id_search_counts, odd_id_search_counts]


def prepare_search_freq_contingency_table(data: pd.DataFrame):
    """
    @param data: a DataFrame where each row contains a user id, boolean value indicating
    if the user is a teacher, the number of times a user logged in, and the number of times the
    user searched
    @return: a collection containing two rows, the first containing the number of times users with
    even ids used the search feature and the second containing the number of times users with
    odd ids used the search feature
    """

    users_with_even_ids, users_with_odd_ids = separate_users_with_odd_and_even_uid(data)

    return join_search_counts(users_with_even_ids, users_with_odd_ids)


if not os.getenv('TESTING'):
    file_name = sys.argv[1]
    user_data = pd.read_json(file_name, lines=True, orient="records")

    increased_usage_table = prepare_search_usage_contingency_table(user_data)
    even_id_searches, odd_id_searches = prepare_search_freq_contingency_table(user_data)

    # print(increased_frequency_table)
    usage_pvalue = chi2_contingency(increased_usage_table).pvalue
    frequency_pvalue = mannwhitneyu(even_id_searches, odd_id_searches).pvalue

    print(usage_pvalue)
    print(frequency_pvalue)
