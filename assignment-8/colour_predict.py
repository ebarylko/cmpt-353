import pandas as pd
import os
import sys
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import FunctionTransformer
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
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


def generate_bayes_lab_model():
    """
    @return: a naive Bayes model trained on color values in LAB space
    """
    return make_pipeline(FunctionTransformer(rgb2lab),
                         GaussianNB())


def gen_neighbours_lab_model():
    """
    @return: a k nearest neighbours model trained on color values in LAB space
    """
    return make_pipeline(FunctionTransformer(rgb2lab),
                         KNeighborsClassifier())


def gen_decision_tree_lab_model():
    """
    @return: a k nearest neighbours model trained on color values in LAB space
    """
    return make_pipeline(FunctionTransformer(rgb2lab),
                         DecisionTreeClassifier())


OUTPUT_TEMPLATE = (
    'Bayesian classifier:     {bayes_rgb:.3f}  {bayes_convert:.3f}\n'
    'kNN classifier:          {knn_rgb:.3f}  {knn_convert:.3f}\n'
    'Rand forest classifier:  {rf_rgb:.3f}  {rf_convert:.3f}\n'
)


if not os.getenv('TESTING'):

    file_name = sys.argv[1]
    color_data = pd.read_csv(file_name)

    normalized_rgb_values, colors = normalized_rgb_values_and_colors(color_data)
    (training_rgb_values, validation_rgb_values,
     training_colors, validation_colors) = train_test_split(normalized_rgb_values,
                                                            colors)
    naive_rgb_model = GaussianNB()
    naive_rgb_model.fit(training_rgb_values, training_colors)

    naive_lab_color_model = generate_bayes_lab_model()
    naive_lab_color_model.fit(training_rgb_values, training_colors)

    kneighbours_rgb_model = KNeighborsClassifier()
    kneighbours_rgb_model.fit(training_rgb_values, training_colors)

    kneighbours_lab_model = gen_neighbours_lab_model()
    kneighbours_lab_model.fit(training_rgb_values, training_colors)

    forest_rgb_model = DecisionTreeClassifier()
    forest_rgb_model.fit(training_rgb_values, training_colors)

    forest_lab_model = gen_decision_tree_lab_model()
    forest_lab_model.fit(training_rgb_values, training_colors)

    print(OUTPUT_TEMPLATE.format(
        bayes_rgb=naive_rgb_model.score(validation_rgb_values, validation_colors),
        bayes_convert=naive_lab_color_model.score(validation_rgb_values, validation_colors),
        knn_rgb=kneighbours_rgb_model.score(validation_rgb_values, validation_colors),
        knn_convert=kneighbours_lab_model.score(validation_rgb_values, validation_colors),
        rf_rgb=forest_rgb_model.score(validation_rgb_values, validation_colors),
        rf_convert=forest_lab_model.score(validation_rgb_values, validation_colors),
    ))
