import pandas as pd
import os
import sys
from scipy.stats import chi2_contingency, mannwhitneyu


def count_of_users_who_did_and_did_not_search(users: pd.DataFrame):
    """
    @param users: a DataFrame where each row contains a user id, a boolean value indicating
    if the user is a teacher, the number of times the user logged in, and the number of times the
    user searched
    @return: a collection which contains the number of users who used the search function and
    the number of users who never searched
    """
    has_never_searched = users['search_count'] < 1
    users_who_searched = users[~has_never_searched]
    users_who_never_searched = users[has_never_searched]
    return users_who_searched.index.size, users_who_never_searched.index.size


def separate_users_with_odd_and_even_uid(data: pd.DataFrame):
    """
    @param data: a DataFrame where each row contains a user id, a boolean value indicating
    if the user is a teacher, the number of times a user logged in, and the number of times the
    user searched
    @return: two DataFrames, where the first has all the users with an even user id and the second
    has all the users with an odd user id
    """
    data_cpy = data.copy()
    has_even_id = data_cpy['uid'] % 2 == 0
    users_with_even_ids = data_cpy[has_even_id]
    users_with_odd_ids = data_cpy[~has_even_id]
    return users_with_even_ids, users_with_odd_ids


def prepare_search_usage_contingency_table(searches: pd.DataFrame):
    """
    @param searches: a DataFrame where each row contains a user id, a boolean value indicating
    if the user is a teacher, the number of times a user logged in, and the number of times the
    user used the search feature
    @return: a collection containing two rows, the first representing the users with even ids and
    the second representing the users with odd ids. Each row contains the number of users who
    searched more than once and the number of users who never searched
    """
    users_with_even_ids, users_with_odd_ids = separate_users_with_odd_and_even_uid(searches)

    even_id_searches, odd_id_searches = map(count_of_users_who_did_and_did_not_search,
                                            [users_with_even_ids, users_with_odd_ids])
    contingency_table = list(zip(even_id_searches, odd_id_searches))
    return contingency_table


def prepare_search_freq_contingency_table(data: pd.DataFrame):
    """
    @param data: a DataFrame where each row contains a user id, boolean value indicating
    if the user is a teacher, the number of times a user logged in, and the number of times the
    user searched
    @return: a collection containing two rows, the first containing the number of times users with
    even ids used the search feature and the second containing the number of times users with
    odd ids used the search feature
    """
    def get_search_counts(df: pd.DataFrame):
        """
        @param df: a DataFrame containing users and the number of times they
        used the search feature
        @return: the number of times each user used the search feature
        """
        return df['search_count'].to_numpy(copy=True)

    users_with_even_ids, users_with_odd_ids = separate_users_with_odd_and_even_uid(data)
    return map(get_search_counts, [users_with_even_ids, users_with_odd_ids])


def get_teachers(users: pd.DataFrame) -> pd.DataFrame:
    """
    @param users: a DataFrame where each row contains a user id, a boolean value indicating
    if the user is a teacher, the number of times a user logged in, and the number of times the
    user searched
    @return: a DataFrame only containing the users who are teachers
    """
    return users.query('is_instructor == True')


def usage_and_freq_pvalue(data: pd.DataFrame):
    """
    @param data: a DataFrame where each row contains a user id, a boolean value indicating
    if the user is a teacher, the number of times a user logged in, and the number of times the
    user used the search feature
    @return: the p-value of the chi squared test testing if more users engaged with the search feature
    and the p-value of the Mann-Whitney U-test checking if the search feature was used more frequently
    """
    increased_usage_table = prepare_search_usage_contingency_table(data)
    even_id_searches, odd_id_searches = prepare_search_freq_contingency_table(data)

    usage_pvalue = chi2_contingency(increased_usage_table).pvalue
    frequency_pvalue = mannwhitneyu(even_id_searches, odd_id_searches).pvalue
    return usage_pvalue, frequency_pvalue


OUTPUT_TEMPLATE = (
    '"Did more/less users use the search feature?" p-value:  {0:.3g}\n'
    '"Did users search more/less?" p-value:  {1:.3g} \n'
    '"Did more/less instructors use the search feature?" p-value:  {2:.3g}\n'
    '"Did instructors search more/less?" p-value:  {3:.3g}'
)


def print_pvalues(original_data_pvalues, modified_data_pvalues):
    normal_chi_squared_pvalue, normal_mann_whitney_u_pvalue = original_data_pvalues
    modified_chi_squared_pvalue, modified_mann_whitney_u_pvalue = modified_data_pvalues

    print(OUTPUT_TEMPLATE.format(
        normal_chi_squared_pvalue,
        normal_mann_whitney_u_pvalue,
        modified_chi_squared_pvalue,
        modified_mann_whitney_u_pvalue
    ))


if not os.getenv('TESTING'):
    file_name = sys.argv[1]
    user_data = pd.read_json(file_name, lines=True, orient="records")
    normal_data_pvalues = usage_and_freq_pvalue(user_data)

    only_teachers = get_teachers(user_data)
    teacher_data_pvalues = usage_and_freq_pvalue(only_teachers)

    print_pvalues(normal_data_pvalues, teacher_data_pvalues)

