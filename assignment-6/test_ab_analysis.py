import ab_analysis as an
import pandas as pd
import pandas.testing as pdt

sample_users = pd.DataFrame({"uid": [4, 2],
                             "is_instructor": [True, False],
                             "login_count": [1, 0],
                             "search_count": [2, 0]})

expected_search_values = (1, 1)


def test_count_of_users_who_did_and_did_not_search():
    assert an.count_of_users_who_did_and_did_not_search(sample_users) == expected_search_values
#
#
# sample_searches = pd.DataFrame({"uid": [1, 2, 3],
#                                 "is_instructor": [True, False, True],
#                                 "login_count": [1, 0, 9],
#                                 "search_count": [2, 0, 4]})
#
# expected_table = pd.DataFrame({"searched_more_than_once": [0, 2],
#                                "never_searched": [1, 0]},
#                               index=["even_id", "odd_id"])
#
#
# def test_prepare_contingency_table():
#     pdt.assert_frame_equal(an.prepare_contingency_table(sample_searches), expected_table)