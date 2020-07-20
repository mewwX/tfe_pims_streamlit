# -*- coding: utf-8 -*-
#
# PING16_2020
#!/usr/bin/env python
#
#   /**
#   * Tests StreamLit
#   * @author mc
#   * @version 0.1
#   **/

# In[1]:

from datetime import datetime
now1 = datetime.now()
#print("Début du script =", now1)
# import warnings
# warnings.filterwarnings('ignore')

import os
import sys
import datetime as dt
import streamlit as st
import pandas as pd
import numpy as np

import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(['srv-cassandra'], auth_provider=auth_provider)
session = cluster.connect()
insertinto = session.prepare("""
                             INSERT INTO ods.tags_values (name, year, min_timestamp, max_timestamp, value_float, value_int, value_string, value_binary, value, qualite, value_type, atelier, machine, section, detail, type, description) 
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                             """)
# bounded = insertinto.bind(listCass)                
# session.execute(bounded)

#Connexion base SQL
import pyodbc
import pandas as pd

server = 'srv-cassandra'
database = ''
username = 'tag_visu'
password = 'tag_visu'

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

#Récupération de la liste de tags + création d'une liste
sql_query = pd.read_sql_query("SELECT Name FROM Aether.dbo.timeseriestransfers where Name > 'V3.'",cnxn)
taglist = sql_query.Name.values.tolist()

###########################################################################


def main() :
    
    #------------Sidebar-----------#
    
    st.sidebar.markdown("# Product filters :")

    st.sidebar.markdown("""> ## <center>(1) ID Produit direct</center>
    """, unsafe_allow_html = True)
    id_produit1 = st.sidebar.text_input("Id produit :")
    
    st.sidebar.markdown("""<h2 style="color:Cyan; background-color:Navy;"><center> Ou cherchez à partir d'un rouleau </center></h2>
    """, unsafe_allow_html = True)
    #st.sidebar.warning("OU cherchez un rouleau")
    st.sidebar.markdown("""> ## <center>(2.1) Fourchette de production</center>
    """, unsafe_allow_html = True)
    
    start_date = st.sidebar.date_input("Date de début")    
    end_date = st.sidebar.date_input("Date de fin")
    rlx_ligne = st.sidebar.selectbox("Selection",['V1','V2','V3','T4','FT1'])
    
    rows_rouleaux = session.execute("""SELECT id_rouleaux from ods.rlx_rouleaux
                                       WHERE fin_fab > $$"""+str(start_date)+"""$$
                                       AND fin_fab <= $$"""+str(end_date)+"""$$
                                       allow filtering;
                                       """)
    list_rlx = []
    for row in rows_rouleaux :
        list_rlx.append(row.id_rouleaux)
            
            
    st.sidebar.markdown("""> ## <center>(2.2) Id </center>
    """, unsafe_allow_html = True)
    id_produit2 = st.sidebar.selectbox("Selection",list_rlx)
    
    if len(id_produit1) > 0 :
        id_produit = id_produit1
    else : 
        id_produit = id_produit2
    #----------------------------#   
    
    choix_principal = st.selectbox("Type de produit",['Nothing','Matière première','Rouleaux','Bobines','Fardeaux','Commandes'])

    st.markdown("* * *")
    st.write(list_rlx)
    #------------Product History-----------#
    st.title("Product")
    
    df_family_final = pd.DataFrame()
    df_gen_final = pd.DataFrame()
   
    rows_history = session.execute("""SELECT id_rouleau, genealogie FROM ods.genealogie
                                      WHERE id_produit = $$"""+id_produit+"""$$;
                                      """)
    list_history = []
    for row in rows_history :
        
        for i in range (len(row)) :
            list_history.append(row[i])
            
    if len(list_history) > 0 :
        rows_history = session.execute("""SELECT * FROM ods.genealogie
                                           WHERE id_rouleau = $$"""+list_history[0]+"""$$;
                                           """)
        for row in rows_history :
            list_history2 = []
            for i in range (len(row)) :
                list_history2.append(row[i])

            df_history = pd.DataFrame([list_history2], columns = ['id_produit','id_fardeau','date_depart_client','genealogie','id_rouleau','machine_prod','niveau','num_bl','num_cmd_sap','type_produit'])
            df_family_final = df_family_final.append(df_history) 

        rows_history2 = session.execute("""SELECT * FROM ods.genealogie
                                           WHERE genealogie = $$"""+list_history[1]+"""$$;
                                           """)
        for row in rows_history2 :
            list_gen = []
            for i in range (len(row)) :
                list_gen.append(row[i])

            df_gen = pd.DataFrame([list_gen], columns = ['id_produit','id_fardeau','date_depart_client','genealogie','id_rouleau','machine_prod','niveau','num_bl','num_cmd_sap','type_produit'])

            df_gen_final = df_gen_final.append(df_gen)  

        df_family_final = df_family_final.sort_values('niveau')
        df_family_final = df_family_final.drop(['genealogie','id_fardeau','num_bl','num_cmd_sap','id_rouleau', 'date_depart_client'],axis=1)
        df_family_final = df_family_final.drop_duplicates()
        
    else :
        pass
    
    # Affichage df(s)
    st.markdown("### Product history")
    st.dataframe(df_gen_final)
    st.markdown("### Product's family ()")
    st.dataframe(df_family_final)
    
    st.info("More to come")
    #----------------------------#
    st.markdown("* * *")


    if choix_principal == 'Matière première' :
        #------------Raw Material-----------#    
        st.markdown("""# Matière première / _Raw Material_ """)

        st.info("Coming soon")
        
    elif choix_principal == 'Rouleaux' :         
        #------------Rouleaux-----------#
        st.title("""Rouleaux / _Rolls_ """)

        df_rlx_final = pd.DataFrame()
        rows_rlx = session.execute("""SELECT * FROM ods.rlx_rouleaux                                      
                                      WHERE id_rouleaux = $$"""+id_produit+"""$$;
                                   """)

        for row in rows_rlx :   
            list_rlx = []
            for i in range(len(row)) :            
                list_rlx.append(row[i])    

            df_rlx = pd.DataFrame([list_rlx], columns = ['id_rouleaux','atelier','campagne','code_defaut_labo','code_defaut_prod','code_prod','debut_fab','diametre_init','enrouleuse','fin_fab','largeur','lib_code_prod','longueur','poids','pqp','statut_qualite'])
            df_rlx_final = df_rlx_final.append(df_rlx)

        st.write(df_rlx_final) 

        #boucle de test - affichage seulement si quelque chose à afficher
        if len(df_rlx_final) == 0 :
            st.warning("Pas d'informations")
        else :
            infos_g = pd.DataFrame([[list_rlx[3], list_rlx[9], list_rlx[1]]], columns = ['Campagne', 'Enrouleuse', 'Référence'])
            carac_rlx = pd.DataFrame([[list_rlx[8], list_rlx[11], list_rlx[12], list_rlx[13]]], columns = ['Diamètre(mm)', 'Largeur(mm)', 'Longueur(m)', 'Poids(kg)'])


            st.write('< ID :', list_rlx[2], '> &nbsp;&nbsp;&nbsp;&nbsp < Début de fabrication :', list_rlx[7], '>')
            st.write('< Statut :', float(list_rlx[15]), '> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp < Fin de fabrication :', list_rlx[10], '>')
            st.write('< Atelier :', list_rlx[0],  '> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; < Temps de fabrication :', list_rlx[10]-list_rlx[7], '>')

            radio_rlx = st.radio('Quelles infos afficher ?', ['Tout','Infos générales','Caracteristiques'])
            if radio_rlx == 'Tout' :
                st.dataframe(df_rlx_final)
            elif radio_rlx == 'Infos générales' :
                st.table(infos_g)
            elif radio_rlx == 'Caracteristiques' :
                st.table(carac_rlx)

            st.info("More to come")

        #----------------------------#
    
    elif choix_principal == 'Bobines' :
        #------------Bobines-----------#
        st.title("""Bobines / _?_ """)
        
        st.info("Coming soon")

        #----------------------------#

    elif choix_principal == 'Fardeaux' :  
        #------------Fardeaux-----------#
        st.title("""Fardeaux / _?_ """)
        
        st.info("Coming soon")
    
        #----------------------------#
        
        
        
    elif choix_principal == 'Commandes' : 
        #------------Commandes-----------#    
        st.title("""Commandes / _Commands_ """)
        
        st.info("Coming soon")

        #----------------------------#
        
    else : 
        st.warning("Bug dans la matrice, veuillez selectionner un produit pour le rapport !")
        

        
if __name__ == "__main__":
    
    st.sidebar.markdown("""
    > ### `Product Dashboard`
    > Avec cette webApp, vous serrez en mesure de récupérer toutes les informations relatives à un produit ainsi que tout son cycle de vie dans l'usine.
    
    > -
    
    > _This webApp is able to provide you with all informations relative to a TFE product; from raw materials to customers' commands._
    > * * *
    """)
    
    logo = "Logo TFE Product dashboard.png"
    st.image(logo, width = 700)
    st.markdown("* * *")
    password = st.text_input("Password", key="pwd", type="password")
    st.markdown("* * *")
    if password == "password" :
        main()
    
