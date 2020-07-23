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
from datetime import datetime
import random
import streamlit as st
import pandas as pd
import numpy as np
import time

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

rows = session.execute("""SELECT name FROM ods.tags_def WHERE line = 'V3_rv'""")
taglist2 = []
for row in rows :
    taglist2.append(row.name)

###########################################################################

def getDuration(then, now, interval = "default"):

    # Returns a duration as specified by variable interval
    # Functions, except totalDuration, returns [quotient, remainder]

    duration = now - then # For build-in functions
    duration_in_s = duration.total_seconds() 

    def years():
        return divmod(duration_in_s, 31536000) # Seconds in a year=31536000.
    def days(seconds = None):
        return divmod(seconds if seconds != None else duration_in_s, 86400) # Seconds in a day = 86400
    def hours(seconds = None):
        return divmod(seconds if seconds != None else duration_in_s, 3600) # Seconds in an hour = 3600
    def minutes(seconds = None):
        return divmod(seconds if seconds != None else duration_in_s, 60) # Seconds in a minute = 60

    def seconds(seconds = None):
        if seconds != None:
            return divmod(seconds, 1)   
        return duration_in_s

    def totalDuration():
        y = years()
        d = days(y[1]) # Use remainder to calculate next variable
        h = hours(d[1])
        m = minutes(h[1])
        s = seconds(m[1])

        return "Période de  {} an(s), {} jour(s), {} heure(s), {} minutes".format(int(y[0]), int(d[0]), int(h[0]), int(m[0]))

    return {
        'years': int(years()[0]),
        'days': int(days()[0]),
        'hours': int(hours()[0]),
        'minutes': int(minutes()[0]),
        'seconds': int(seconds()),
        'default': totalDuration()
    }[interval]

###########################################################################

def main_rv() :
    
    #------------------------------------Sidebar------------------------------------#
    
    st.sidebar.markdown("# Rouge vert :")
    
    
    
    #-------------------------------------------------------------------------------#
    #------------------------------------Filtres------------------------------------#
    
    st.info("Bienvenue sur l'outil Big Data __Rouge_Vert__")
    st.success("Commençons par filtrer")
    st.title("Filtrage")
    
    machine_prod = st.selectbox("Selection de la ligne de production",['Faire un choix','V1','V2','V3','T4','FT1'])
    
    if machine_prod == 'Faire un choix' :
        st.warning("Selectionnez une ligne de production")
    elif machine_prod == 'V3' : 
        reference_prod = st.selectbox("Selection de la référence prod",
                                      ['Faire un choix','Pas de choix','4,5F53 -V3','4,5F531 -V3','4F53 -V3'])
        if reference_prod == 'Faire un choix' :
            st.warning("Selectionnez une référence à étudier")
        else :
            my_bar1 = st.progress(2)
            st.success("Choisissez maintenant la bonne periode à inspecter")
                        
            st.title("Choisissez la période comparée :")
            
            st.info("A venir ; comparaison avec valeurs actuelles (presque en direct)")
            
            debut_periode_comp = st.date_input("Période comparée : Début", 
                                               min_value = dt.datetime(2017,4,1),
                                               max_value = datetime.now(), 
                                               value=dt.datetime(2017, 4, 1))
            debut_heure_comp = st.time_input("Heure de début", value=dt.datetime(1,1,1,5,0))
            comparisonhours1 = (debut_heure_comp.hour)*60 + (debut_heure_comp.minute)
            
            fin_periode_comp = st.date_input("Période comparée : Fin",
                                             min_value = debut_periode_comp, 
                                             max_value = datetime.now(), 
                                             value=datetime.now())
            fin_heure_comp = st.time_input("Heure de fin", value=dt.datetime(1,1,1,5,0))
            comparisonhours2 = (fin_heure_comp.hour)*60 + (fin_heure_comp.minute)
            
            if (debut_periode_comp == fin_periode_comp) and (int(comparisonhours1-comparisonhours2) > 0) : 
                st.warning("Problème dans les heures")
            elif debut_periode_comp > fin_periode_comp :
                st.warning("Les dates sont mauvaises")
            elif (str(debut_periode_comp) == "2017-04-01") :
                st.warning("Choisir une date de début postérieur au 1er Avril 2017")
            elif (str(debut_periode_comp) != "2017-04-01") and (debut_periode_comp <= fin_periode_comp) :
                
                datedebut = dt.datetime.combine(debut_periode_comp, debut_heure_comp)
                datefin = dt.datetime.combine(fin_periode_comp, fin_heure_comp)
                
                st.write(getDuration(datedebut, datefin))
                
                my_bar2 = st.progress(0)
                my_bar1.progress(25)
                my_bar2.progress(25)
                st.success("Maintenant, les bonnes périodes pour comparer")
                                
                st.title("Bonnes périodes :")                
                list_periode = []
                
                bonneperiodedebut1 = st.text_input("Bonne période 1 : Date et heure début",
                                                   value = "Format YYYY-MM-JJ HH:mm")
                bonneperiodefin1 = st.text_input("Bonne période 1 : Date et heure fin",
                                                 value = "Format YYYY-MM-JJ HH:mm")
                validation1 = st.checkbox("Validation bonne période 1")
                if validation1 :
                    list_periode.append(bonneperiodedebut1)
                    list_periode.append(bonneperiodefin1)
                
                st.markdown("* * *")
                bonneperiodedebut2 = st.text_input("Bonne période 2 : Date et heure début",
                                                   value = "--")
                bonneperiodefin2 = st.text_input("Bonne période 2 : Date et heure fin",
                                                 value = "--")
                validation2 = st.checkbox("Validation bonne période 2")
                if validation2 :
                    list_periode.append(bonneperiodedebut2)
                    list_periode.append(bonneperiodefin2)
                
                st.markdown("* * *")
                bonneperiodedebut3 = st.text_input("Bonne période 3 : Date et heure début",
                                                   value = "--")
                bonneperiodefin3 = st.text_input("Bonne période 3 : Date et heure fin",
                                                 value = "--")
                validation3 = st.checkbox("Validation bonne période 3")
                if validation3 :
                    list_periode.append(bonneperiodedebut3)
                    list_periode.append(bonneperiodefin3)
                
                st.markdown("* * *")                
                
                if validation1 or validation2 or validation3 :
                    my_bar3 = st.progress(50)
                    my_bar1.progress(50)
                    my_bar2.progress(50)
                    
                    list_periode_fix = list_periode
                    
                try :
                    st.write(list_periode_fix)
                except :
                    st.info("Validez au moins 1 bonne période ou vérifiez les infos")


#                 st.write(getDuration(then, now))

                dataz = {'name': [], 'timestamp': [], 'value': []}
                df_bonne_periode_final = pd.DataFrame(dataz)
                my_bar_bonne_periode = st.progress(0)
#                 for i in range (len(taglist2)): #len(taglist)

#                     #requête CQL. Tri sur la date
#                     prog = 100*i/(len(taglist2)-1)
#                     my_bar_bonne_periode.progress(int(prog))
#                     rows_bonne_periode = session.execute("""SELECT name, timestamp, value
#                                                 FROM aethertimeseries.datapointsyear 
#                                                 WHERE name = $$"""+taglist[i]+"""$$ 
#                                                 AND year = """+str(debut_periode_comp.year)+"""

#                                                 AND timestamp >= $$"""+str(datedebut)+"""$$
#                                                 AND timestamp <= $$"""+str(datefin)+"""$$

#                                                 ORDER BY timestamp;
                                                
#                                            """)
#                     k=0
#                     # Cassandra
#                     for row in rows_bonne_periode :
#                         list_bonne_periode=[row.name, row.timestamp, row.value]
                        
#                         data_bonne_periode = {'name': [list_bonne_periode[0]], 
#                                               'timestamp': [list_bonne_periode[1]], 
#                                               'value': [list_bonne_periode[2]]}
#                         df_sub_tag = pd.DataFrame(data_bonne_periode)
#                         df_bonne_periode_final = pd.concat([df_bonne_periode_final, df_sub_tag],
#                                                            axis = 0,
#                                                            ignore_index=True)
                        
                st.dataframe(df_bonne_periode_final)

        
    else :
        st.info("Veuilliez selectionner V3 - seul actif pour le moment")
    
    #-------------------------------------------------------------------------------#

def main() :
    
    #------------------------------------Sidebar------------------------------------#
    
    st.sidebar.markdown("# Product filters :")

    st.sidebar.markdown("""> ## <center>(1) ID Produit direct</center>
    """, unsafe_allow_html = True)
    id_produit1 = st.sidebar.text_input("Id produit :")
    
    st.sidebar.markdown("""<h2 style="color:white; background-color:Navy;"><center> Ou cherchez à partir d'un rouleau </center></h2>
    """, unsafe_allow_html = True)
    #st.sidebar.warning("OU cherchez un rouleau")
    st.sidebar.markdown("""> ## <center>(2.1) Fourchette de production</center>
    """, unsafe_allow_html = True)
    
    start_date = st.sidebar.date_input("Date de début")    
    end_date = st.sidebar.date_input("Date de fin")
    rlx_ligne = st.sidebar.selectbox("Selection",['V1','V2','V3','T4','FT1'])
    
    rows_rouleaux = session.execute("""SELECT id_rouleaux from ods.rlx_rouleaux
                                       WHERE fin_fab >= $$"""+str(start_date)+"""$$
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
    #-------------------------------------------------------------------------------#

    #------------Product History-----------#
    st.title("Product")
    
    df_family_final = pd.DataFrame()
    df_gen_final = pd.DataFrame()
   
    rows_history = session.execute("""SELECT id_rouleau, genealogie FROM ods.genealogie
                                      WHERE id_produit = $$"""+id_produit+"""$$;
                                      """)
    list_history = []
    for row in rows_history :
        list_history = []    
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

            df_history = pd.DataFrame([list_history2],
                                      columns = ['id_produit','id_fardeau','date_depart_client',
                                                 'genealogie','id_rouleau','machine_prod',
                                                 'niveau','num_bl','num_cmd_sap','type_produit'])
            df_family_final = df_family_final.append(df_history) 

        rows_history2 = session.execute("""SELECT * FROM ods.genealogie
                                           WHERE genealogie = $$"""+list_history[1]+"""$$;
                                           """)
        for row in rows_history2 :
            list_gen = []
            for i in range (len(row)) :
                list_gen.append(row[i])

            df_gen = pd.DataFrame([list_gen],
                                  columns = ['id_produit','id_fardeau','date_depart_client',
                                             'genealogie','id_rouleau','machine_prod',
                                             'niveau','num_bl','num_cmd_sap','type_produit'])

            df_gen_final = df_gen_final.append(df_gen)  

        df_family_final = df_family_final.sort_values(['niveau', 'genealogie'])        
        df_family_final = df_family_final.drop(['genealogie','id_fardeau','num_bl',
                                                'num_cmd_sap','id_rouleau', 'date_depart_client'],axis=1)
        df_family_final = df_family_final.drop_duplicates()
        
    else :
        pass

    # Affichage df(s)
    st.markdown("### Product history")
    st.dataframe(df_gen_final)
    st.markdown("### Product's family (Everything from the same roll)")
    st.dataframe(df_family_final)
    
    st.info("More to come")
    #----------------------------#
    st.markdown("* * *")

    choix_principal = st.selectbox("Type de produit",
                                   ['Nothing','Matière première','Rouleaux','Bobines','Fardeaux','Commandes'])

    st.markdown("* * *")
    
        #---------------------------------------------------------------------------------------------------#
        
    if choix_principal == 'Matière première' :
        #------------Raw Material-----------#    
        st.markdown("""# Matière première / _Raw Material_ """)

        st.info("Coming soon")
        
        #----------------------------------------------------------------------------------------------------#
        
    elif choix_principal == 'Rouleaux' :         
        #------------Rouleaux-----------#
        st.title("""Rouleaux / _Rolls_ """)

        df_rlx_final = pd.DataFrame()
        df_mesure_final = pd.DataFrame()
        
        rows_rlx = session.execute("""SELECT * FROM ods.rlx_rouleaux                                      
                                      WHERE id_rouleaux = $$"""+id_produit+"""$$;
                                   """)

        for row in rows_rlx :   
            list_rlx = []
            for i in range(len(row)) :            
                list_rlx.append(row[i])    

            df_rlx = pd.DataFrame([list_rlx], columns = ['machine_prod','reference_prod','id_rouleaux',
                                                         'campagne','code_defaut_labo','code_defaut_prod',
                                                         'code_prod','debut_fab','diametre','enrouleuse',
                                                         'fin_fab','largeur','longueur','poids',
                                                         'pqp','statut_qualite'])
            df_rlx_final = df_rlx_final.append(df_rlx)

        st.write(df_rlx_final) 

        #boucle de test - affichage seulement si quelque chose à afficher
        if len(df_rlx_final) == 0 :
            st.warning("Pas d'informations")
        else :
            infos_g = pd.DataFrame([[list_rlx[3], list_rlx[9], list_rlx[1]]],
                                   columns = ['Campagne', 'Enrouleuse', 'Référence'])
            carac_rlx = pd.DataFrame([[list_rlx[8], list_rlx[11], list_rlx[12], list_rlx[13]]],
                                     columns = ['Diamètre(mm)', 'Largeur(mm)', 'Longueur(m)', 'Poids(kg)'])


            st.write('< ID :', list_rlx[2], '> &nbsp;&nbsp;&nbsp;&nbsp < Début de fabrication :', list_rlx[7], '>')
            st.write('< Statut :', float(list_rlx[15]),
                     '> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp < Fin de fabrication :', list_rlx[10], '>')
            st.write('< Atelier :', list_rlx[0],
                     '> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; < Temps de fabrication :',
                     list_rlx[10]-list_rlx[7], '>')

            radio_rlx = st.radio('Quelles infos afficher ?', ['Tout','Infos générales','Caracteristiques'])
            if radio_rlx == 'Tout' :
                st.dataframe(df_rlx_final)
            elif radio_rlx == 'Infos générales' :
                st.table(infos_g)
            elif radio_rlx == 'Caracteristiques' :
                st.table(carac_rlx)

            
            rows_mesure = session.execute("""SELECT * FROM ods.mesures 
                                             WHERE id_rouleaux = $$"""+id_produit+"""$$;
                                             """)
            
            for row in rows_mesure :
                list_mesure = []
                for i in range(len(row)) :            
                    list_mesure.append(row[i])
                df_mesure = pd.DataFrame([list_mesure], columns = ['machine_prod','reference_prod','date_mesure',
                                                         'lib_mesure','pos_mesure','valeur_mesure',
                                                         'code_mesure','code_prod','enrouleuse','hors_spec',
                                                         'id_rouleaux','pqp','provenance','spec_max',
                                                         'spec_min'])
                df_mesure_final = df_mesure_final.append(df_mesure)
                
            df_mesure_final_horsspec = pd.DataFrame()
            tempo = (df_mesure_final['hors_spec'] == '1')
            df_mesure_final_horsspec = df_mesure_final[tempo]
            
            df_mesure_final = df_mesure_final.sort_values(['date_mesure', 'lib_mesure'])        
            df_mesure_final = df_mesure_final.drop(['machine_prod','reference_prod','code_mesure',
                                                    'code_prod','hors_spec','id_rouleaux','provenance'],axis=1)
            
            df_mesure_final_horsspec = df_mesure_final_horsspec.sort_values(['date_mesure', 'lib_mesure'])        
            df_mesure_final_horsspec = df_mesure_final_horsspec.drop(['machine_prod','reference_prod','code_mesure',
                                                                      'code_prod','hors_spec',
                                                                      'id_rouleaux','provenance'],axis=1)
                
            st.markdown("### Mesures faites sur ce rouleau")       
            radio_mesure = st.radio('Quelles infos afficher ?', ['Tout','Hors Spec'])
            if radio_mesure == 'Tout' :
                st.dataframe(df_mesure_final)
            elif radio_mesure == 'Hors Spec' :
                st.dataframe(df_mesure_final_horsspec)
            
            
            st.markdown("### Rapport OCS du rouleau (V3)")
            
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
    
    if password == "rouge_vert" :
        main_rv()
