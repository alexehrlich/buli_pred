from sklearn.metrics import accuracy_score
from xgboost import XGBClassifier
import pandas as pd
from datetime import datetime
import numpy as np
import pickle
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
import torch
from sklearn import metrics
from xgboost import XGBClassifier

def evaluate_model(model, X_train, X_test, y_train, y_test, labels=['H', 'D', 'A'], y_pred_override=None):
    # Predict train and test
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test) if y_pred_override is None else y_pred_override

    # Handle torch tensors
    if isinstance(y_pred_train, torch.Tensor):
        y_pred_test = np.reshape(y_pred_test.numpy(), len(y_test)) if isinstance(y_pred_test, torch.Tensor) else y_pred_test
        y_pred_train = np.reshape(y_pred_train.numpy(), len(y_train))

    # Accuracy
    train_accuracy = (sum(y_pred_train == y_train) / len(y_train)) * 100
    test_accuracy = (sum(y_pred_test == y_test) / len(y_test)) * 100

    print(f"Test accuracy: {metrics.accuracy_score(y_pred=y_pred_test, y_true=y_test)}")

    # Confusion matrix
    cm = confusion_matrix(y_test, y_pred_test)
    disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=labels)
    disp.plot()

def train_simple_model(df):
    features = [
    "xg_home_rolling_3",
    "xg_away_rolling_3",
    "elo_home_rolling_3",
    "elo_away_rolling_3",
    "elo_rolling_diff",           # Combined team strength
    "xg_rolling_diff",            # Attack quality difference
    "goal_ratio_diff",            # Recent scoring form
    "ga_per_xga_diff",            # Defense efficiency diff
    "gf_per_xg_diff",             # Offense efficiency diff
    "elo_diff_x_xg_diff",         # Interaction term, might capture outliers --> draws?
    "elo_diff_sq",

    # "days_since_home",            # Rest days (can impact performance)
    # "days_since_away",

    "goal_ratio_home_rolling_1",
    "goal_ratio_away_rolling_1",

    # "match_hour",
    # "day_code",
    # "week",

    # "poss_home_rolling_3",
    # "sot_home_rolling_3",
    # "poss_away_rolling_3",
    # "sot_away_rolling_3",
    # "xg_away_rolling_3",
    # "xg_home_rolling_3"
    ]

    cutoff_date = pd.to_datetime('2024-05-01')
    X_train = df[df['date'] < cutoff_date][features].to_numpy()
    y_train = df[df['date'] < cutoff_date]['home_won'].to_numpy()
    X_test = df[df['date'] >= cutoff_date][features].to_numpy()
    y_test = df[df['date'] >= cutoff_date]['home_won'].to_numpy()


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

    with open('./models/model_win_loose.pkl', 'wb') as f:
        pickle.dump(xgb_clf, f)

def train_complex_model(df):
    features = [
    "xg_home_rolling_3",
    "xg_away_rolling_3",
    "elo_home_rolling_3",
    "elo_away_rolling_3",
    "elo_rolling_diff",           # Combined team strength
    "xg_rolling_diff",            # Attack quality difference
    "goal_ratio_diff",            # Recent scoring form
    "ga_per_xga_diff",            # Defense efficiency diff
    "gf_per_xg_diff",             # Offense efficiency diff
    "elo_diff_x_xg_diff",         # Interaction term, might capture outliers --> draws?
    "elo_diff_sq",

    "days_since_home",            # Rest days (can impact performance)
    "days_since_away",

    "goal_ratio_home_rolling_1",
    "goal_ratio_away_rolling_1",

    "match_hour",
    "day_code",
    "week",

    "poss_home_rolling_3",
    "sot_home_rolling_3",
    "poss_away_rolling_3",
    "sot_away_rolling_3",
    "xg_away_rolling_3",
    "xg_home_rolling_3"
    ]

    cutoff_date = pd.to_datetime('2024-05-01')
    X_train = df[df['date'] < cutoff_date][features].to_numpy()
    y_train = df[df['date'] < cutoff_date]['target'].to_numpy()
    X_test = df[df['date'] >= cutoff_date][features].to_numpy()
    y_test = df[df['date'] >= cutoff_date]['target'].to_numpy()

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
    with open('./models/model_win_draw_loose.pkl', 'wb') as f:
        pickle.dump(xgb_clf, f)

def main():
    df = pd.read_csv('./data/processed/buli_matches_rolling.csv', index_col=0)
    df = df[df['result_home'].notna()]
    df['target'] = df['result_home'].map({"W": 0, "D":1, "L":2})
    df['date'] = pd.to_datetime(df['date'])

    train_simple_model(df)
    train_complex_model(df)


if __name__ == '__main__':
    main()