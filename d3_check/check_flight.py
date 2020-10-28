import pandas as pd
import numpy as np


def check_flight(path):
    predictions = pd.read_csv(f'{path}/final_list.csv')

    origin, dest = str(input(f'Please select an Origin and Destination airport codes: ')).split()
    itinerary = origin.upper() + '_' + dest.upper()
    airports = predictions['itinerary'].tolist()
    if itinerary not in airports:
        print(f'Please, choose other airports')

    df = predictions.loc[(predictions['itinerary'] == itinerary)]

    if df.groupby(predictions['itinerary']).apply(lambda x: np.average(x['price'], weights=x['passengers'])).empty:
        return f"Sorry we couldn't find flights between {origin} and {dest}"
    else:
        avg_price = df.groupby(predictions['itinerary']).apply(
            lambda x: np.average(x['price'], weights=x['passengers']))
        title = f'Average plane ticket for a flight from {origin.upper()} to {dest.upper()} is {round(avg_price.item(), 2)} dollars'
        return print(title)




