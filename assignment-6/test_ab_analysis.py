import ab_analysis as an
import pandas as pd
import numpy.testing as npt
import pandas.testing as pdt

sample_users = pd.DataFrame({"uid": [4, 2],
                             "is_instructor": [True, False],
                             "login_count": [1, 0],
                             "search_count": [2, 0]})

expected_search_values = (1, 1)


def test_count_of_users_who_did_and_did_not_search():
    assert an.count_of_users_who_did_and_did_not_search(sample_users) == expected_search_values


sample_searches = pd.DataFrame({"uid": [1, 2, 3],
                                "is_instructor": [True, False, True],
                                "login_count": [1, 0, 9],
                                "search_count": [2, 0, 4]})

expected_contingency_table = [(0, 2), (1, 0)]


def test_prepare_search_usage_contingency_table():
    assert an.prepare_search_usage_contingency_table(sample_searches) == expected_contingency_table


sample_data = pd.DataFrame({"uid": [1, 2, 3],
                            "is_instructor": [True, False, True],
                            "login_count": [1, 0, 9],
                            "search_count": [2, 0, 4]})


even_id_searches, odd_id_searches = an.prepare_search_freq_contingency_table(sample_data)


def test_prepare_search_freq_contingency_table():
    npt.assert_array_equal(even_id_searches, [0])
    npt.assert_array_equal(odd_id_searches, [2, 4])


mixed_users = pd.DataFrame({"uid": [1, 2, 3],
                            "is_instructor": [True, False, False],
                            "login_count": [1, 0, 9],
                            "search_count": [2, 0, 4]})

expected_teachers = pd.DataFrame({"uid": [1],
                                  "is_instructor": [True],
                                  "login_count": [1],
                                  "search_count": [2]})


def test_get_teachers():
    pdt.assert_frame_equal(an.get_teachers(mixed_users), expected_teachers)