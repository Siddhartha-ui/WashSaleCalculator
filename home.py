import streamlit as st
from streamlit_option_menu import option_menu
from upload import Upload
from login import Login
import pandas as pd
from calcwashsale import CalcwashSale , WashsaleReport
from session import Session


if __name__ == "__main__":

    
    st.set_page_config(page_title="Stock Tool", layout="wide", page_icon="chart_with_upwards_trend")
    FileName = "stock.h5"

    Session = Session()


def SignUp():

            login = Login()

            with st.container() :

                buff, col, col2, buff2 = st.columns([1,2,2,1])
                col.subheader("Create New Account")
                new_user = col.text_input("Username", key= "new_User").strip()
                new_password = col.text_input("Password",type='password', key= "new_password").strip()
                confirm_new_pasword = col.text_input("Confirm New Password",type='password', key= "new_confirm_pass").strip()
                save_btn = col.button("Save")

            if len(new_user) > 0 and len(new_password) > 0 and len(confirm_new_pasword) > 0 and save_btn  :

                    msg = login.PasswordRule(new_password, 8)
                    if len(msg) > 0 :
                       col.error(msg)
                       st.stop()  
                    
                    if login.ValidateConfirmPass(new_password,confirm_new_pasword) :

                        if login.CheckDuplicateuser(new_user) :
                            login.create_usertable()
                            login.add_userdata(new_user,login.make_hashes(new_password))
                            col.success("You have successfully created a valid Account")
                            col.info("Go to Login Menu to login")
                            
                            

                        else :
                            col.error ("User Exists. Create a new user")
                    else :
                        col.error ("New Password and Confirm New Password does not match")
            else :
                   pass


def uploadtest() :

    df = pd.DataFrame(columns=['Id','Symbol','Transaction Type' , 'Q', 'Price', 'Trade Date' , 'Fund' , 'Com' , 'Proceeds Currency', 'NetProceeds'])
    Tran_Type = ['Buy', 'Sell']
    config = {
    'Id' : st.column_config.TextColumn('LotId (required)', required=True),
    'Symbol' : st.column_config.TextColumn('Symbol', required= True),
    'Transaction Type' : st.column_config.SelectboxColumn('Transaction Type(Buy/Sell)', options= Tran_Type, required= True ),
    'Q' : st.column_config.NumberColumn('Quantity', required= True),
    'Price' : st.column_config.NumberColumn('Price', required= True),
    'Trade Date' : st.column_config.DateColumn('Trade Date', required= True),
    'Fund' : st.column_config.TextColumn('Fund', required= True),
    'Com' : st.column_config.NumberColumn('Commision', required= True),
    'Proceeds Currency' : st.column_config.TextColumn('Proceeds Currency'),
    'NetProceeds' : st.column_config.NumberColumn('NetProceeds', required= True)

    }

    result = st.data_editor(df, column_config = config, num_rows='dynamic')
    
    if st.button('Get results'):
        st.write(result) 


def lopinoperation() :

   login = Login()

   result = False

   with st.container() :

        buff, col, col2, buff2 = st.columns([1,2,2,1])

    
        col.subheader("Login")

        username = col.text_input("User Name", key= "user").strip()
        password = col.text_input("Password",type='password', key= "pass").strip()

        col1, col2 = col.columns([2,1])
        login_btn = col1.button("Sign-In")

   if login_btn :

        if len(str(username).strip()) == 0 :
           col.error ("Please enter user name")
           st.stop()
        elif  len(str(password).strip()) == 0 :
           col.error ("Please enter password")
           st.stop()
        else :
            if len(str(username).strip()) > 0 and len(str(password).strip()) > 0 :
                result =login.SignIn(username,password)
        if result:
                col.success("Logged In as {}".format(username))
                st.session_state.is_loggedin = True
                st.session_state.user_name = str(username)
                st.session_state.user_id_rnd = login.getuserid(username)
        else :
                col.error("Login credential does not exist.Please sign-Up")

with st.sidebar:
        selected = option_menu("", ["Sign-In", "Sign-Up", "Upload data","---", "Calculation", 'Report',  'Logout'],
        icons=['person', 'door-open', 'cloud-upload', None, "list-task", 'bar-chart', 'door-closed'],
        menu_icon="cast", default_index=0
        # styles={
        #     "container": {"padding": "0!important", "background-color": "#fafafa"},
        #     "icon": {"color": "blue", "font-size": "20px"},
        #     "nav-link": {"font-size": "14px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
        #     "nav-link-selected": {"background-color": "grey"},
        # }

        )


if selected == "Sign-In" :
       
       lopinoperation()


elif selected == "Sign-Up" :
    SignUp()

elif selected == "Logout" :
   if st.session_state.is_loggedin :
        st.subheader(f"User  {st.session_state.user_name}  is successfully logged out")
        st.session_state.clear()
   else :
        st.error ("Please Sign-in")
        st.stop()

elif selected == "Upload data" :

        if not st.session_state.is_loggedin :
            st.error ("Please Sign-in ")
            st.stop()
        else :
            upload = Upload()
            upload.ShowhUploader()
            st.session_state.is_uploaded = True

elif selected == "Calculation" :
        if not st.session_state.is_loggedin :
            st.error ("Please Sign-in ")
            st.stop()
        else  :
            
            calcwashsale = CalcwashSale()
            calcwashsale.calcWashSale()


elif selected == "Report" :
        if not st.session_state.is_loggedin :
            st.error ("Please Sign-in ")
            st.stop()
        else :
            report = WashsaleReport()
            report.runreport()

            

