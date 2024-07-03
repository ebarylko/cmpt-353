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
from functools import partial
from itertools import chain


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


def create_both_models(model):
    """
    @param model: a machine learning model
    @return: two instances of the model, one to be used on RGB values and the other to be used on
    LAB colors
    """
    return model(), make_pipeline(FunctionTransformer(rgb2lab),
                                  model())

def train_and_evaluate_model(training_data, validation_data, model):
    """
    @param training_data: the data used to train the model
    @param validation_data: the data used to validate the model
    @param model: the model to be trained
    @return: trains the model using training_data and evaluates it using the validation_data
    """
    model.fit(*training_data)
    return model.score(*validation_data)


# def get_validation_data_score(validation_data, model):
#     """
#     @param validation_data: the data used to validate the model
#     @param model: the model to be used
#     @return: the score of the model's predictions on the validation data
#     """
#     print(model)
#     # print(model.score(*validation_data))
#     return model.score(*validation_data)
#
if not os.getenv('TESTING'):

    file_name = sys.argv[1]
    color_data = pd.read_csv(file_name)

    normalized_rgb_values, colors = normalized_rgb_values_and_colors(color_data)
    (training_rgb_values, validation_rgb_values,
     training_colors, validation_colors) = train_test_split(normalized_rgb_values,
                                                            colors)

    rgb_and_lab_models = chain.from_iterable(map(create_both_models,
                                                 [GaussianNB, KNeighborsClassifier, DecisionTreeClassifier]
                                                 ))

    fit_and_score = partial(train_and_evaluate_model,
                       (training_rgb_values, training_colors),
                       (validation_rgb_values, validation_colors))

    naive_rgb_score, naive_lab_score, neighbours_rgb_score, \
        neighbours_lab_score, tree_rgb_score, tree_lab_score = map(fit_and_score, rgb_and_lab_models)

    print(OUTPUT_TEMPLATE.format(
        bayes_rgb=naive_rgb_score,
        bayes_convert=naive_lab_score,
        knn_rgb=neighbours_rgb_score,
        knn_convert=neighbours_lab_score,
        rf_rgb=tree_rgb_score,
        # rf_convert=forest_lab_model.score(training_rgb_values, training_colors)
        rf_convert=tree_lab_score,
    ))
