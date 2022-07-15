import streamlit as st

st.title('TIA Dashboard')

options = st.multiselect(
     'What are your favorite colors',
     ['Green', 'Yellow', 'Red', 'Blue'],
     ['Yellow', 'Red'])

st.write('You selected:', options)

st.write("""
    - [Exploratory Data Analysis](https://github.com/dendihandian/bank-marketing-analysis/blob/main/bank-marketing.ipynb)
    - [Dataset](https://www.kaggle.com/datasets/dhirajnirne/bank-marketing)
""")