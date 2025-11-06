import streamlit as st
import requests
import json
import altair as alt
import pandas as pd

def load_new_matches():
   st.info("Loading new matches, this takes a while ...")
   requests.get('http://fastapi:8000/update')

# with open('/Users/alexanderehrlich/Coding/42/Specialization/AI/buli/streamlit/src/test.json', 'r') as f:
#     matches = json.load(f)

st.title("Bundesliga Match Predictor")
st.set_page_config(layout="wide")

date_selected = st.sidebar.date_input("Match date")
st.sidebar.button("Load new matches", on_click=load_new_matches)

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
        container.subheader(f"{match['team_home']}   vs.   {match['team_away']}")
       else:
        container.subheader(f"{match['team_home']}  {int(match['goals_home'])}\t:\t{int(match['goals_away'])} {match['team_away']}")
       with st.expander("View details"):
        c1, c2 = st.columns(2)

        c1.write(f"{match['team_home']} Wins: {float(match['simple_model_results']['home_team_win']):.2f}%")
        c1.write(f"{match['team_home']} Draw or Loss: {float(match['simple_model_results']['home_team_draw_or_loss']):.2f}%")
        c2.write(f"{match['team_home']} Wins: {float(match['complex_model_results']['home_team_win']):.2f}%")
        c2.write(f"Draw: {float(match['complex_model_results']['draw']):.2f}%")
        c2.write(f"{match['team_away']} Wins: {float(match['complex_model_results']['away_team_win']):.2f}%")

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