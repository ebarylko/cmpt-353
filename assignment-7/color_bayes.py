import pandas as pd
import os
import sys
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import FunctionTransformer
from sklearn.naive_bayes import GaussianNB
from sklearn.pipeline import make_pipeline
from skimage.color import rgb2lab
import matplotlib.pyplot as plt
import numpy as np
from skimage.color import lab2rgb


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


COLOUR_RGB = {
    'red': (255, 0, 0),
    'orange': (255, 112, 0),
    'yellow': (255, 255, 0),
    'green': (0, 231, 0),
    'blue': (0, 0, 255),
    'purple': (185, 0, 185),
    'brown': (117, 60, 0),
    'pink': (255, 184, 184),
    'black': (0, 0, 0),
    'grey': (150, 150, 150),
    'white': (255, 255, 255),
}

name_to_rgb = np.vectorize(COLOUR_RGB.get, otypes=[np.uint8, np.uint8, np.uint8])


def plot_predictions(model, lum=67, resolution=300):
    """
    Create a slice of LAB colour space with given luminance; predict with the model; plot the results.
    """
    wid = resolution
    hei = resolution
    n_ticks = 5

    # create a hei*wid grid of LAB colour values, with L=lum
    ag = np.linspace(-100, 100, wid)
    bg = np.linspace(-100, 100, hei)
    aa, bb = np.meshgrid(ag, bg)
    ll = lum * np.ones((hei, wid))
    lab_grid = np.stack([ll, aa, bb], axis=2)

    # convert to RGB for consistency with original input
    X_grid = lab2rgb(lab_grid)

    # predict and convert predictions to colours so we can see what's happening
    y_grid = model.predict(X_grid.reshape((-1, 3)))
    pixels = np.stack(name_to_rgb(y_grid), axis=1) / 255
    pixels = pixels.reshape((hei, wid, 3))

    # plot input and predictions
    plt.figure(figsize=(10, 5))
    plt.suptitle('Predictions at L=%g' % (lum,))
    plt.subplot(1, 2, 1)
    plt.title('Inputs')
    plt.xticks(np.linspace(0, wid, n_ticks), np.linspace(-100, 100, n_ticks))
    plt.yticks(np.linspace(0, hei, n_ticks), np.linspace(-100, 100, n_ticks))
    plt.xlabel('A')
    plt.ylabel('B')
    plt.imshow(X_grid.reshape((hei, wid, -1)))

    plt.subplot(1, 2, 2)
    plt.title('Predicted Labels')
    plt.xticks(np.linspace(0, wid, n_ticks), np.linspace(-100, 100, n_ticks))
    plt.yticks(np.linspace(0, hei, n_ticks), np.linspace(-100, 100, n_ticks))
    plt.xlabel('A')
    plt.imshow(pixels)


def generate_lab_model():
    """
    @return: a naive Bayes model trained on color values in LAB space
    """
    return make_pipeline(FunctionTransformer(rgb2lab),
                         GaussianNB())


def print_model_validation_scores(rgb_score, lab_score):
    """
    @param rgb_score: the score of the RGB model when applied on the validation dataset
    @param lab_score: the score of the LAB model when applied on the validation dataset
    @return: prints out the scores of the RGB AND LAB models on their respective validation datasets
    """
    print("RGB model validation score:", rgb_score)
    print("LAB model validation score:", lab_score)


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

    print_model_validation_scores(rgb_validation_score, lab_validation_score)

