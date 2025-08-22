from sklearn.metrics import accuracy_score
from xgboost import XGBClassifier
import pandas as pd
from datetime import datetime
import numpy as np
import pickle
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
#import torch
from sklearn import metrics
from xgboost import XGBClassifier
import json

# def evaluate_model(model, X_train, X_test, y_train, y_test, labels=['H', 'D', 'A'], y_pred_override=None):
#     # Predict train and test
#     y_pred_train = model.predict(X_train)
#     y_pred_test = model.predict(X_test) if y_pred_override is None else y_pred_override

#     # Handle torch tensors
#     if isinstance(y_pred_train, torch.Tensor):
#         y_pred_test = np.reshape(y_pred_test.numpy(), len(y_test)) if isinstance(y_pred_test, torch.Tensor) else y_pred_test
#         y_pred_train = np.reshape(y_pred_train.numpy(), len(y_train))

#     # Accuracy
#     train_accuracy = (sum(y_pred_train == y_train) / len(y_train)) * 100
#     test_accuracy = (sum(y_pred_test == y_test) / len(y_test)) * 100

#     print(f"Test accuracy: {metrics.accuracy_score(y_pred=y_pred_test, y_true=y_test)}")

#     # Confusion matrix
#     cm = confusion_matrix(y_test, y_pred_test)
#     disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=labels)
#     disp.plot()

def time_series_split(df, features, target, train_size = 0.8) -> pd.DataFrame:
    split_idx = int(len(df) * train_size)
    train = df.iloc[:split_idx]
    test = df.iloc[split_idx:]
    X_train = train[features].to_numpy()
    y_train = train[target].to_numpy()
    X_test = test[features].to_numpy()
    y_test = test[target].to_numpy()

    return X_train, X_test, y_train, y_test

def train_simple_model(df, file_name='model_win_loose.pkl'):
    print('Training the simple model:')
    with open('../../src/training/features_simple_model.json', 'r') as f:
        features = json.load(f)

    X_train, X_test, y_train, y_test = time_series_split(df, features, 'home_won')


    print(F"X_train_shape: {X_train.shape},  y_train_shape: {y_train.shape}")
    print(F"X_test_shape: {X_test.shape},  y_test_shape: {y_test.shape}")

    xgb_clf = XGBClassifier(
        objective='multi:softmax',
        num_class=2,
        max_depth=4,           
        n_estimators=100,       
        learning_rate=0.1,
        reg_alpha = 10,
        reg_lambda = 5
    )

    xgb_clf.fit(X_train, y_train)

    y_pred_test = xgb_clf.predict(X_test)
    print(metrics.classification_report(y_true=y_test, y_pred=y_pred_test))

    #evaluate_model(xgb_clf, X_train, X_test, y_train, y_test, labels=['NW', 'W'])

    with open(f"../../models/{file_name}", 'wb') as f:
        pickle.dump(xgb_clf, f)
    return accuracy_score(y_test, y_pred_test)

def train_complex_model(df, file_name='model_win_draw_loose.pkl'):
    print('Training the complex model:')
    with open('../../src/training/features_complex_model.json', 'r') as f:
        features = json.load(f)

    X_train, X_test, y_train, y_test = time_series_split(df, features, 'target')

    print(F"X_train_shape: {X_train.shape},  y_train_shape: {y_train.shape}")
    print(F"X_test_shape: {X_test.shape},  y_test_shape: {y_test.shape}")

    xgb_clf = XGBClassifier(
        objective='multi:softmax',
        num_class=3,
        max_depth=2,           
        n_estimators=100,       
        learning_rate=0.1,
        reg_alpha = 10,
        reg_lambda = 5
    )

    xgb_clf.fit(X_train, y_train)

    y_pred_test_proba = xgb_clf.predict_proba(X_test)
    y_pred_test = xgb_clf.predict(X_test)
    #y_pred_test = [1 if abs(p[0] - p[2]) < 0.025 else np.argmax(p) for p in y_pred_test_proba]

    #evaluate_model(xgb_clf, X_train, X_test, y_train, y_test, y_pred_override=y_pred_test)
    print(metrics.classification_report(y_true=y_test, y_pred=y_pred_test))
    with open(f"../../models/{file_name}", 'wb') as f:
        pickle.dump(xgb_clf, f)
    return accuracy_score(y_test, y_pred_test)

def train():
    df = pd.read_csv('../../data/processed/buli_matches_rolling.csv', index_col=0)

    #only use already played matches for training
    df = df[df['result_home'].notna()]
    df['target'] = df['result_home'].map({"W": 0, "D":1, "L":2})
    df['date'] = pd.to_datetime(df['date'])

    return train_simple_model(df, 'simple_model_retrained.pkl'), train_complex_model(df, 'complex_model_retrained.pkl')
    
def main():
    df = pd.read_csv('./data/processed/buli_matches_rolling.csv', index_col=0)

    #only use already played matches for training
    df = df[df['result_home'].notna()]
    df['target'] = df['result_home'].map({"W": 0, "D":1, "L":2})
    df['date'] = pd.to_datetime(df['date'])

    train_simple_model(df)
    train_complex_model(df)


if __name__ == '__main__':
    main()