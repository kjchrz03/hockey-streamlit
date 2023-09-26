import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(page_title="Check This Data", page_icon="🏒", initial_sidebar_state="expanded")


##########################################
##  Title, Tabs, and Sidebar            ##
##########################################

st.title("Check This Data")
st.markdown('''##### <span style="color:gray">Explore NHL Advanced Stats, Simply</span>
            ''', unsafe_allow_html=True)
                
tab_player, tab_team, tab_explore, tab_faq = st.tabs(["Player Lookup", "Team Lookup", "Explore", "FAQ"])



st.sidebar.markdown(" ## About Check This Data")
st.sidebar.markdown("Dip your toes into advanced hockey analytics with some of my favorite metrics"  )              
st.sidebar.info("Read more about how the model works and see the code on my [Github](https://github.com/kjchrz03/hockey-streamlit).", icon="ℹ️")