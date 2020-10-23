import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder
from sklearn.preprocessing import PolynomialFeatures
from sklearn.preprocessing import KBinsDiscretizer
from sklearn.preprocessing import RobustScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import MaxAbsScaler
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score

def data_partition():
    planes = pd.read_csv('/home/juan/Downloads/definitive_1.csv')
    planes, planes_predict = train_test_split(planes, test_size=0.2)

    planes.to_csv('/home/juan/Downloads/planes_train.csv')
    planes_predict.to_csv('/home/juan/Downloads/planes_predict.csv')

    return dataframe

#machine learning preprocessing

def preprocessing(dataframe):
    NUM_FEATS = ['Quarter', 'Distance', 'DistanceGroup', 'CouponGeoType', 'sum(Passengers)']
    CAT_FEATS = ['itinerary', 'RPCarrier', 'FareClass']
    FEATS = NUM_FEATS + CAT_FEATS
    TARGET = 'avg(Fare)'

    numeric_transformer = \
        Pipeline(steps=[('imputer', SimpleImputer(strategy='mean')),
                        ('robustscaler', StandardScaler()),
                        # ('poly', PolynomialFeatures()),
                        # ('disc', KBinsDiscretizer())
                        ])

    categorical_transformer = \
        Pipeline(steps=[('imputer', SimpleImputer(strategy='constant')),
                        ('onehot', OneHotEncoder(handle_unknown='ignore'))])

    preprocessor = \
        ColumnTransformer(transformers=[('num', numeric_transformer, NUM_FEATS),
                                        ('cat', categorical_transformer, CAT_FEATS)
                                        ])

def model():
    planes_train, planes_test = train_test_split(planes, test_size=0.2)

    model = Pipeline(steps=[('preprocessor', preprocessor),
                            ('regressor', RandomForestRegressor(n_jobs=-1,
                                                                max_depth=500,
                                                                min_samples_split=23
                                                                ))])

    model.fit(planes_train[FEATS], planes_train[TARGET])

def model_checks():
    y_test = model.predict(planes_test[FEATS])
    y_train = model.predict(planes_train[FEATS])

    print(f"test error: {mean_squared_error(y_pred=y_test, y_true=planes_test[TARGET], squared=False)}")
    print(f"train error: {mean_squared_error(y_pred=y_train, y_true=planes_train[TARGET], squared=False)}")

    # Check the model with CrossValidation
    scores = cross_val_score(model,
                             planes[FEATS],
                             planes[TARGET],
                             scoring='neg_root_mean_squared_error',
                             cv=5, n_jobs=-1)
    print(np.mean(-scores))