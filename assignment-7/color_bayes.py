import pandas as pd
import os
import sys
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB


def normalized_rgb_values_and_colors(data: pd.DataFrame):
    """
    @param data: a DataFrame where each row contains the R, G, and B value of a color, a guess for the name of the color
    corresponding to the RGB values, and an indication of how accurate the guess is
    @return: two numpy arrays, one containing the normalized RGB values and the other containing the guesses for the
    colors
    """
    cpy = data.copy()
    colors = cpy['Label']
    rgb_values = cpy[['R', 'G', 'B']] / 255
    return rgb_values.to_numpy(), colors.to_numpy()


if not os.getenv('TESTING'):
    file_name = sys.argv[1]
    color_data = pd.read_csv(file_name)

    normalized_rgb_values, colors = normalized_rgb_values_and_colors(color_data)
    training_rgb_values, validation_rgb_values, training_colors, validation_colors = train_test_split(normalized_rgb_values,
                                                                                                      colors)
    model = GaussianNB()
    model.fit(training_rgb_values, training_colors)
    print(model.score(validation_rgb_values, validation_colors))