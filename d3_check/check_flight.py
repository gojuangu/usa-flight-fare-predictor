import pandas as pd
import numpy as np


def check_flight(target_path_1, origin, dest, quarter, fare_class):





    predictions = pd.read_csv(f'{target_path_1}/final_list.csv')
    itinerary = origin + '_' + dest

    df = predictions.loc[(predictions['itinerary'] == itinerary) &
                         (predictions['Quarter'] == quarter) &
                         (predictions['FareClass'] == fare_class)]

    avg_price = df.groupby(predictions['itinerary']).apply(lambda x: np.average(x['price'], weights=x['passengers']))

    title = f'Average plane ticket for a flight from {origin} to {dest} is {round(avg_price.item(),2)} dollars'
    return title
