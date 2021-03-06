from src.application.config import *
from src.application.connect_db import connect_db_session, connectivity, check_connectivity
import pandas as pd
from PIL import Image

class Application():
    def __init__(self):
        # the text that displays in the tab at the very top of the page
        st.set_page_config(page_title='Chicago Budget Ordinance 2021 Analysis')
        self.db_path = os.sep.join([os.environ['PWD'], 'db'])

    def run_app(self):
        # primary app call will run everything contained in frame()
        self.frame()

    def frame(self):
        # place main components of page here, add more as necessary
        self.title()
        self.body()
        self.footer()

    def title(self):
        # execute st calls for title section
        st.title('Experimenting With SQLAlchmey and ORM')

    def body(self):
        # execute st calls for body section
        # a header for this section
        sub_title = 'Testing and Exploring the SQL Alchemy Schema with the Chicago 2021 Budget Ordinance'
        st.markdown(f"<h3 style='text-align: center; color: black;font-family:courier;'>{sub_title}</h3>", unsafe_allow_html=True)
        # display some overview graphs
        x = connect_db_session(self.db_path)
        # st.write(x)
        # connection = connectivity(x)
        # check_connectivity(session)

    def footer(self):
        # make st calls for footer section here
        version_status = 'Version Alpha'
        st.markdown(
            f'<i style="font-size:11px">{version_status}</i>',
            unsafe_allow_html=True)
        owner_url = 'https://github.com/justinhchae/city_budget'
        st.markdown(
            f'<i style="font-size:11px">&copy All Rights Reserved [The Project Group]({owner_url})</i>',
            unsafe_allow_html=True)
        st.markdown(
            '<p style="font-size:11px">The information provided by this app (the “Site”) is for general informational purposes only. All information on the Site is provided in good faith, however we make no representation or warranty of any kind, express or implied, regarding the accuracy, adequacy, validity, reliability, availability or completeness of any information on the Site.</p>',
            unsafe_allow_html=True
        )
