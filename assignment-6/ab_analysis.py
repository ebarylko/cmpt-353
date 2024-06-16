import pandas as pd


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


def prepare_contingency_table(searches: pd.DataFrame) -> pd.DataFrame:
    """
    @param searches: a DataFrame where each row contains a user id, boolean value indicating
    if the user is a teacher, the number of times a user logged in, and the number of times the
    user searched
    @return: a DataFrame containing two rows, the first representing the users with even ids and
    the second representing the users with odd ids. Each row contains the number of users who
    searched more than once and the number of users who never searched
    """
    has_even_id = searches['uid'] % 2 == 0
    users_with_even_ids = searches[has_even_id]
    users_with_odd_ids = searches[~has_even_id]

    even_id_searches, odd_id_searches = map(count_of_users_who_did_and_did_not_search,
                                            [users_with_even_ids, users_with_odd_ids])
    searched_at_least_once, never_searched = list(zip(even_id_searches, odd_id_searches))
    return pd.DataFrame({"searched_more_than_once": searched_at_least_once,
                         "never_searched": never_searched},
                        index=["even_id", "odd_id"])

