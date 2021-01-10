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

def combine():
    users = pd.read_csv('Users.csv')
    transactions = pd.read_csv('Transactions.csv')
    st.write("""
    # Pinkman zadatak
    Tablica korisnika i transakcija 
    """)
    st.dataframe(users)
    st.dataframe(transactions)

if __name__ == '__main__':
    # get_transactions()
    # transactions = transactions_to_csv()
    # users = get_users()
    combine()
    