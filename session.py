import streamlit as st
import pandas as pd

class Session(object) :
    def __init__(self) -> None:

        if not "holding_data" in st.session_state :
            st.session_state.holding_data = pd.DataFrame()

        if not "tran_data" in st.session_state :
            st.session_state.tran_data = pd.DataFrame()

        if not "is_uploaded" in st.session_state :
            st.session_state.is_uploaded = False
        if not "is_processed" in st.session_state :
            st.session_state.is_processed = False
        if not "asof_date" in st.session_state :
            st.session_state.asof_date = ""

        if "user_name" not in st.session_state :
            st.session_state.user_name = ""

        if "user_id_rnd" not in st.session_state :
            st.session_state.user_id_rnd = ""

        if "sign_up" not in st.session_state :
            st.session_state.sign_up = False
        if "is_loggedin" not in st.session_state :
            st.session_state.is_loggedin = False
