# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 15:18:11 2021

@author: ivan.severovic
"""

import mysql.connector
import requests
import pickle
import base64
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup

def get_transactions():
    params = {"api-key": 'any'}
    response = requests.get("https://pinkman.online/api/", params=params)
    # print (type(response.json()))
    # for key,value in response.json().items():
    #     print(key,value)
    transactions = response.json()['data']
    with open('transactions.pkl', 'wb') as f:
        pickle.dump(transactions, f)
        
def transactions_to_csv():
    with open('transactions.pkl', 'rb') as f:
        transactions = pickle.load(f)
    columns = ['ID','Type','User_ID','Date','Amount','Currency','Processed','Details']
    values = []
    for transaction in transactions:
        row = []
        for k,v in transaction.items():
            row.append(v)
        values.append(row)
    df_transactions = pd.DataFrame(values,columns=columns)
    df_transactions.to_csv( "Transactions.csv", index=False, encoding='utf-8-sig')
    return df_transactions

def get_users():
    mydb = mysql.connector.connect(
      host="localhost",
      user="Ivan",
      password="ivan123",
      database="condor"
    )
    
    mycursor = mydb.cursor()
    sql = "SELECT * FROM users \
          INNER JOIN country ON users.country = country.id \
          INNER JOIN status ON users.status = status.id \
          ORDER BY users.id"
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    
    users = []
    for x in myresult:
      users.append(x)
      
    num_fields = len(mycursor.description)
    print(num_fields)
    field_names = [i[0] for i in mycursor.description]
    df_users = pd.DataFrame(users,columns=field_names)
    df_users.to_csv( "Users.csv", index=False, encoding='utf-8-sig')
    return df_users

def build_app(tecaj):
    users = pd.read_csv('Users.csv')
    transactions = pd.read_csv('Transactions.csv')
    st.title('Streamlit aplikacija')
    st.markdown("&nbsp;&nbsp;&nbsp;&nbspIspod ovog teksta su prikazane tablica korisnika i tablica transakcija.\
    Tablicu korisnika sam kreirao pomoću inner joina tablica iz dump-a MySQL baze,\
    a tablicu transakcija sam kreirao dohvaćanjem podataka putem API-ja.", unsafe_allow_html=True)
    st.subheader('Korisnici')
    st.dataframe(users)
    st.subheader('Transakcije')
    st.dataframe(transactions)
    st.write(""" 
    &nbsp;&nbsp;&nbsp;&nbspPrethodne dvije tablice su kreirane pomoću programskog jezika
    Python jer je on bolji za pripremu, čišćenje i transformaciju podataka, te
    podržava noSQL strukturu. Programski jezik PHP podržava relacijski SQL
    model baze podataka i OOP.
            Sljedeći korak bi bio kreiranje dva modela: Korisnik i Transakcija,
    njihovo povezivanje 1:M vezom, te migracija i seed tablica u Laravelu.
    Tada bi se projekt mogao realizirati vrlo lako i klikom na željenog
    korisnika otvorio bi se modal sa svim podacima o njemu skupa s popisom 
    njegovih transakcija. 
            S obzirom da sam otkrio novu zanimljivu biblioteku streamlit 
    vratit ću se na rješavanje zadatka putem Pythona. Kako bih dobio željene
    rezultate iz zadatka bit će potrebna još jedna denormalizacija. Spajanjem
    tablice korisnika s tablicom transakcija dobivamo završni skup podataka.
    """)
    final_df = pd.merge(users, transactions, left_on='id', right_on='User_ID')
    df = final_df[['id','username','email','code','city','dob','name.1',
                  'Amount','Currency','Type','Date','Processed','Details']]
    df.rename(columns={'name.1': 'Status', 'id': 'UID', 'dob': 'Age'}, inplace=True)
    
    st.subheader('Transakcije po korisniku')
    option = st.selectbox('Odaberi korisnika', list(set(df['username'].tolist())))
    st.dataframe(df.loc[df['username'] == option])
    
    st.subheader('Transakcije po tipu kartice')
    card = st.radio("Odaberi vrstu kartice",('Visa', 'Maestro', 'MasterCard'))
    if card == 'Visa':
        st.dataframe(df.loc[df['Type'] == 'Visa'])
    if card == 'Maestro':
        st.dataframe(df.loc[df['Type'] == 'Maestro'])
    if card == 'MasterCard':
        st.dataframe(df.loc[df['Type'] == 'Mastercard'])
    
    df.loc[df['Currency'] == 'usd', 'Amount'] = df['Amount'] * float(tecaj)
    df['Currency'] = 'eur'
    drzave = df.groupby(['code'])['Amount'].agg('sum')
    # drzave['Country'] = drzave.index
    drzave1 = pd.DataFrame({'Country': drzave.index, 'Amount': drzave.values})
        
    st.sidebar.subheader('Preuzimanje podataka')
    st.sidebar.write("&nbsp;&nbsp;&nbsp;&nbsp1USD = " + tecaj + "EUR")
    if st.sidebar.button('Transakcije po državi'):
        st.sidebar.dataframe(drzave)
        csv1 = drzave1.to_csv(index=False)
        b64 = base64.b64encode(csv1.encode()).decode()
        link1= f'<a href="data:file/csv;base64,{b64}" download="Drzava.csv">Download csv :floppy_disk: </a>'
        st.sidebar.markdown(link1, unsafe_allow_html=True)
        # # st.sidebar.write('&nbsp;&nbsp;&nbsp;&nbspPodaci spremljeni :floppy_disk:')
        
    korisnici = df.groupby(df['username']).agg({'Amount': ['sum','count']})
    korisnici1 = korisnici.reset_index()
        
    if st.sidebar.button('Transakcije po korisniku'):
        # st.sidebar.write('&nbsp;&nbsp;&nbsp;&nbspPodaci spremljeni :floppy_disk:')
        st.sidebar.dataframe(korisnici)
        csv2 = korisnici1.to_csv(index=False)
        b64 = base64.b64encode(csv2.encode()).decode()
        link1= f'<a href="data:file/csv;base64,{b64}" download="Korisnici.csv">Download csv :floppy_disk: </a>'
        st.sidebar.markdown(link1, unsafe_allow_html=True)
         
    # st.route('myendpoint', mycallback)    
        
    # if st.server.request.foo == 'korisnici':
    #     st.server.respond({'data': korisnici})
    # if st.server.request.foo == 'drzave':
    #     st.server.respond({'data': drzave})
    
    
def combine():
    users = pd.read_csv('Users.csv')
    transactions = pd.read_csv('Transactions.csv')
    final_df = pd.merge(users, transactions, left_on='id', right_on='User_ID')
    df = final_df[['id','username','email','code','city','dob','name.1',
                  'Amount','Currency','Type','Date','Processed','Details']]
    # df.loc[df.Currency == 'usd', 'Amount'] = df * 1.22
    # df.loc[df.Currency == 'usd', 'Currency'] = 'usd'
    # for row in df.loc[df.Currency == 'usd', 'Amount']:
    #     print(row)
    df.loc[df['Currency'] == 'usd', 'Amount'] = df['Amount'] * 0.82
    return df

def usd_eur():
    url = "https://valuta.exchange/hr/usd-to-eur?amount=1"
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")
    stranice = soup.find('div', class_="Converter__ToContainer-ah6o35-3 iVEPxa")
    konverzija = stranice.find('input', class_="CurrencyInput-sc-46bpxp-0 loGueB")['value']
    return konverzija

if __name__ == '__main__':
    # get_transactions()
    # transactions = transactions_to_csv()
    # users = get_users()
    # final_df = combine()
    tecaj = usd_eur()
    build_app(tecaj)
    