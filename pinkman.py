# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 15:18:11 2021

@author: ivan.severovic
"""

import mysql.connector
import requests
import pickle
import pandas as pd
import streamlit as st

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

def build_app():
    users = pd.read_csv('Users.csv')
    transactions = pd.read_csv('Transactions.csv')
    st.write(""" # Pinkman zadatak
    /tIspod su prikazane tablica korisnika i tablica transakcija.
    Tablicu korisnika sam kreirao pomoću inner joina tablica iz dump-a baze,
    a tablicu transakcija sam kreirao dohvaćanjem vrijednosti iz API-ja.
    """)
    st.dataframe(users)
    st.dataframe(transactions)
    st.write(""" 
             /tPrethodne dvije tablice su kreirane pomoću programskog jezika
    Python jer je on bolji za pripremu, čišćenje i transformaciju podataka, te
    podržava noSQL strukturu. Programski jezik PHP podržava relacijski SQL
    model baze podataka i OOP.
            /tSljedeći korak bi bio kreiranje dva modela: Korisnik i Transakcija,
    njihovo povezivanje 1:M vezom, te migracija i seed tablica u Laravelu.
    Tada bi se prvi zadatak mogao riješiti vrlo lako i klikom na željenog
    korisnika otvorio bi se modal sa svim podacima o njemu skupa s popisom 
    njegovih transakcija. 
            /tS obzirom na to je u zadatku navedeno da se za njegovo rješavanje ne
    smije koristiti framework, a PHP znam koristiti sa uz Laravel framework, 
    vratit ću se na rješavanje zadatka putem Pythona. Kako bih dobio željene
    rezultate iz zadatka bit će potrebna još jedna denormalizacija. Spajanjem
    tablice korisnika s tablicom transakcija dobivamo sljedeći rezultat.
    """)
    final_df = pd.merge(users, transactions, left_on='id', right_on='User_ID')
    df = final_df[['id','username','email','code','city','dob','name.1',
                  'Amount','Currency','Type','Date','Processed','Details']]
    st.dataframe(df)
    
def combine():
    users = pd.read_csv('Users.csv')
    transactions = pd.read_csv('Transactions.csv')
    final_df = pd.merge(users, transactions, left_on='id', right_on='User_ID')
    df = final_df[['id','username','email','code','city','dob','name.1',
                  'Amount','Currency','Type','Date','Processed','Details']]
    return df

if __name__ == '__main__':
    # get_transactions()
    # transactions = transactions_to_csv()
    # users = get_users()
    # final_df = combine()
    build_app()
    