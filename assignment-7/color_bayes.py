import pandas as pd


def prepare_data_for_model(data: pd.DataFrame):
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


