import pandas as pd


def prepare_data_for_model(data: pd.DataFrame):
    """
    @param data: a DataFrame where each row contains the R, G, and B value of a color, a guess for the name of the color
    corresponding to the RGB values, and an indication of how accurate the guess is
    @return: a DataFrame containing the normalized RGB values and a Series containing the guesses for the colors
    """
    colors = data['Label']
    rgb_values = data[['R', 'G', 'B']] / 255
    return rgb_values, colors
