import pandas as pd
import numpy as np


def check_flight(path):
    while True:
        try:
            quarter = int((input('Enter the quarter you would like to flight: ')))
        except ValueError:
            print('Sorry you did not introduce a number')
            continue
        if quarter not in (1, 2, 3, 4):
            print(f'Please, choose a quarter (1 to 4)')
            continue
        else:
            break
    while True:
        try:
            origin = str((input('Enter your origin airport code: ')))
            dest = str((input('Enter your destination airport code: ')))
        except ValueError:
            print('Sorry you did not introduce a number')
            continue
        if dest is str:
            print(f'Please select a correct Destination and/or origin')
            continue
        else:
            break


    predictions = pd.read_csv(f'{path}/final_list.csv')

    itinerary = origin + '_' + dest

    df = predictions.loc[(predictions['itinerary'] == itinerary) &
                         (predictions['Quarter'] == quarter)]

    if df.groupby(predictions['itinerary']).apply(lambda x: np.average(x['price'], weights=x['passengers'])).empty:
        return f"Sorry we couldn't find flights between {origin} and {dest} in the {quarter} of the year"
    else:
        avg_price =df.groupby(predictions['itinerary']).apply(lambda x: np.average(x['price'], weights=x['passengers']))
        title = f'Average plane ticket for a flight from {origin} to {dest} is {round(avg_price.item(),2)} dollars'
        return title



def help():
    help_type = input('What')