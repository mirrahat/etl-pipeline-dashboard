# Azure Web App Configuration for Streamlit
# Developed by: Mir Hasibul Hasan Rahat

import streamlit as st
import os

# Azure-specific Streamlit configuration
def configure_for_azure():
    """Configure Streamlit for Azure Web App deployment"""
    
    # Set Azure-specific configurations
    st.set_page_config(
        page_title="ETL Dashboard - Mir Hasibul Hasan Rahat",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Azure environment settings
    if os.getenv('WEBSITE_HOSTNAME'):  # Running on Azure
        st.markdown("""
        <style>
        .main .block-container {
            max-width: 1200px;
            padding-top: 1rem;
        }
        </style>
        """, unsafe_allow_html=True)