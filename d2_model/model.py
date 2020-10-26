import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import RandomizedSearchCV

def data_partition(path):
    planes = pd.read_csv(f'{path}/definitive_2.csv')
    planes, planes_predict = train_test_split(planes, test_size=0.2)

    planes.to_csv(f'{path}/planes_train.csv')
    planes_predict.to_csv(f'{path}/planes_predict.csv')


#machine learning preprocessing

def model(path):
    print('Modeling...')
    NUM_FEATS = ['Quarter', 'Distance', 'DistanceGroup', 'CouponGeoType', 'passengers']
    CAT_FEATS = ['itinerary', 'RPCarrier', 'FareClass']
    FEATS = NUM_FEATS + CAT_FEATS
    TARGET = 'avg_fare'

    numeric_transformer = \
        Pipeline(steps=[('imputer', SimpleImputer(strategy='mean')),
                        ('robustscaler', StandardScaler()),
                        ])

    categorical_transformer = \
        Pipeline(steps=[('imputer', SimpleImputer(strategy='constant')),
                        ('onehot', OneHotEncoder(handle_unknown='ignore'))])

    preprocessor = \
        ColumnTransformer(transformers=[('num', numeric_transformer, NUM_FEATS),
                                        ('cat', categorical_transformer, CAT_FEATS)
                                        ])

    planes = pd.read_csv(f'{path}/planes_train.csv')
    planes_train, planes_test = train_test_split(planes, test_size=0.2)
    planes_predict = pd.read_csv(f'{path}/planes_predict.csv')

    model = Pipeline(steps=[('preprocessor', preprocessor),
                            ('regressor', RandomForestRegressor(n_jobs=-1,
                                                                max_depth=500,
                                                                min_samples_split=23
                                                                ))])

    model.fit(planes_train[FEATS], planes_train[TARGET])

    # model optimizier
    param_grid = {
        'preprocessor__num__imputer__strategy': ['mean', 'median'],
        'regressor__n_estimators': [16, 32, 64, 128, 256, 512],
        'regressor__max_depth': [2, 4, 8, 16],
    }

    grid_search = RandomizedSearchCV(model,
                                     param_grid,
                                     cv=5,
                                     verbose=10,
                                     scoring='neg_root_mean_squared_error',
                                     n_jobs=-1,
                                     n_iter=32)

    grid_search.fit(planes[FEATS], planes[TARGET])

    y_pred = grid_search.predict(planes_predict[FEATS])
    final_list = pd.DataFrame({'itinerary': planes_predict['itinerary'],
                               'Quarter': planes_predict['Quarter'],
                               'RPCarrier': planes_predict['RPCarrier'],
                               'FareClass': planes_predict['FareClass'],
                               'passengers': planes_predict['passengers'],
                               'price': y_pred
                               })
    final_list.to_csv(f'{path}/final_list.csv', index=False)