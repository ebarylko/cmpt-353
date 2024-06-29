import pandas as pd
import os
import sys
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import FunctionTransformer
from sklearn.naive_bayes import GaussianNB
from sklearn.pipeline import make_pipeline
from skimage.color import rgb2lab


def normalized_rgb_values_and_colors(data: pd.DataFrame):
    """
    @param data: a DataFrame where each row contains the R, G, and B value of a color,
     a guess for the name of the color corresponding to the RGB values, and an indication of how
     accurate the guess is
    @return: two numpy arrays, one containing the normalized RGB values and the other containing the
    guesses for the colors
    """
    cpy = data.copy()
    colors = cpy['Label']
    rgb_values = cpy[['R', 'G', 'B']] / 255
    return rgb_values.to_numpy(), colors.to_numpy()


def generate_lab_model():
    """
    @return: a naive Bayes model trained on color values in LAB space
    """
    return make_pipeline(FunctionTransformer(rgb2lab),
                         GaussianNB())


if not os.getenv('TESTING'):
    file_name = sys.argv[1]
    color_data = pd.read_csv(file_name)

    normalized_rgb_values, colors = normalized_rgb_values_and_colors(color_data)
    (training_rgb_values, validation_rgb_values,
     training_colors, validation_colors) = train_test_split(normalized_rgb_values,
                                                            colors)
    rgb_model = GaussianNB()
    rgb_model.fit(training_rgb_values, training_colors)

    lab_color_model = generate_lab_model()
    lab_color_model.fit(training_rgb_values, training_colors)

    rgb_validation_score = rgb_model.score(validation_rgb_values, validation_colors)
    lab_validation_score = lab_color_model.score(validation_rgb_values, validation_colors)

    print("RGB model validation score:", rgb_validation_score)
    print("LAB model validation score:", lab_validation_score)

