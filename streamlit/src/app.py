import streamlit as st
import requests
import json
import altair as alt
import pandas as pd

def is_highest_proba(values, event):
    highest_proba = -1.0
    highest_event = None
    
    for e, proba in values.items():
        if float(proba) > highest_proba:
            highest_event = e
            highest_proba = float(proba)
    return highest_event == event
     
def get_prediction_label(text, results, key):
    probability = float(results[key])
    if is_highest_proba(results, key):
        color = "#4CAF50"
    else:
        return text + f" {float(results[key]):.1f}%"
    
    label_html = f"""
    <div style="
        color: white;
        font-weight: bold;
    ">
        {text}
        <span style="
            background-color: {color};
            color: white;
            padding: 4px 6px;
            border-radius: 8px;
            display: inline-block;
            margin-left: 6px;
        ">
            {probability:.1f}%
        </span>
    </div>
    """
    return label_html

def load_new_matches():
   st.info("Loading new matches, this takes a while ...")
   requests.get('http://fastapi:8000/update')

def retrain_model():
    requests.get('http://fastapi:8000/retrain_model')

st.title("Bundesliga Match Predictor")
st.set_page_config(layout="wide")

date_selected = st.sidebar.date_input("Match date")
st.sidebar.button("Load new matches", on_click=load_new_matches)
st.sidebar.button("Retrain Model", on_click=retrain_model)

r = requests.get('http://fastapi:8000/predict', params={"date": str(date_selected)})
if r.status_code == 200:
    matches = json.loads(r.text)
else:
    st.info("Something went wrong :-(")

if len(matches) == 0:
    st.info(f"No matches found for {date_selected}")

for match in matches:
    container = st.container(border=True)
    matches_df = pd.DataFrame.from_dict(match['elos'])
    df_long = matches_df.reset_index().melt(id_vars="index", var_name="Team", value_name="Value")

    with container:
        if match['result_home'] == None:
            container.header(f"{match['team_home']}" + "   vs.   " + f"{match['team_away']}")
        else:
            container.header(f"{match['team_home']}  {int(match['goals_home'])}\t:\t{int(match['goals_away'])} {match['team_away']}")
        with st.expander("View prediction"):
            c1, c2 = st.columns(2)
            c1.subheader("Simple model prediction", divider='blue')
            c1.markdown(get_prediction_label(f"{match['team_home']} wins: ", match['simple_model_results'], 'home_team_win'), unsafe_allow_html=True)
            c1.markdown(get_prediction_label(f"{match['team_home']} looses or draw: ", match['simple_model_results'], 'home_team_draw_or_loss'), unsafe_allow_html=True)
            
            c2.subheader("Complex model prediction", divider='blue')
            c2.markdown(get_prediction_label(f"{match['team_home']} wins: ", match['complex_model_results'], 'home_team_win'), unsafe_allow_html=True)
            c2.markdown(get_prediction_label(f"Draw: ", match['complex_model_results'], 'draw'), unsafe_allow_html=True)
            c2.markdown(get_prediction_label(f"{match['team_away']} wins: ", match['complex_model_results'], 'away_team_win'), unsafe_allow_html=True)
            
            st.divider()

            chart = (
                    alt.Chart(df_long)
                    .mark_line(point=False)
                    .encode(
                        x=alt.X("index:N", title="Date"),
                        y=alt.Y("Value:Q", title="Score", scale=alt.Scale(zero=False)),
                        color=alt.Color("Team:N", title="Team"),
                    )
                    .properties(title="Elo Ratings", width=600, height=400))
            st.altair_chart(chart)