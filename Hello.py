# Copyright (c) Streamlit Inc. (2018-2022) Snowflake Inc. (2022)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import streamlit as st
#from streamlit.logger import get_logger
import numpy as np

#LOGGER = get_logger(__name__)

st.set_page_config(page_title="Check This Data", page_icon="🏒", initial_sidebar_state="expanded")

def run():
    st.set_page_config(
        page_title="Hello",
        page_icon="👋",
    )


##########################################
##  Title, Tabs, and Sidebar            ##
##########################################

st.title("Check This Data")
st.markdown('''##### <span style="color:gray">Explore Player Data</span>
            ''', unsafe_allow_html=True)
                
tab_player, tab_team, tab_explore, tab_faq = st.tabs(["Player Report Cards", "Shots Heatmap", "Explore", "FAQ"])

col1, col2, col3 = st.sidebar.columns([1,8,1])
with col1:
    st.write("")
with col2:
    st.image('figures/heroguy.png',  use_column_width=True)
with col3:
    st.write("")

st.sidebar.markdown(" ## About Check This Data")
st.sidebar.markdown("I love hockey and I love data, so I built this app to visualize some of my favorite advanced metrics to help users visualize them."  )              
st.sidebar.info("Read more about my process on Github [Github](https://github.com/kjchrz03/hockey-streamlit).", icon="ℹ️")


if __name__ == "__main__":
    run()
