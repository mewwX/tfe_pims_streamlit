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

#                                              #-- IMPORTS: --#

from datetime import datetime
from dateutil.relativedelta import *

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

#Connexion base SQL
import pyodbc
import pandas as pd

server = 'srv-cassandra'
database = ''
username = 'tag_visu'
password = 'tag_visu'

# cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
# cursor = cnxn.cursor()

#Récupération de la liste de tags + création d'une liste
# sql_query = pd.read_sql_query("SELECT Name FROM Aether.dbo.timeseriestransfers where Name > 'V3.'",cnxn)
# taglist = sql_query.Name.values.tolist()

# Récupération de la liste de tag Rouge_Vert
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

def vitesse_ligne_actuelle(machine_prod, datedebut, datefin,yyear) :            
    if machine_prod == 'V1' :
        tag_vitesse = 'V1.LF.enr1.SI3880.pv'
    elif machine_prod == 'V2' :
        tag_vitesse = 'V2.LF.ENR.vitesse_chariot.pv'
    elif machine_prod == 'V3' :
        tag_vitesse = 'V3.LF.LV3.SI0611.pv'
    elif machine_prod == 'T4' :
        tag_vitesse = 'T4.LF.LF.vitM6.pv'
    elif machine_prod == 'FT1' :
        st.info("FT1 pas encore géré")
    else :
        st.warning("Veuillez choisir une ligne correcte")
        
    try :
        rows_vitesse = session.execute("""SELECT value
                                    FROM aethertimeseries.datapointsyear 
                                    WHERE name = $$"""+tag_vitesse+"""$$ 
                                    AND year = """+yyear+"""

                                    AND timestamp >= $$"""+str(datedebut)+"""$$
                                    AND timestamp <= $$"""+str(datefin)+"""$$

                                    ORDER BY timestamp DESC
                                    LIMIT 1;
                               """)
        
        for row in rows_vitesse :
            list_vitesse = [row.value]

        vitesse = round(float(list_vitesse[0]),1)
    except :
        st.warning("Il y a eu un problème - sûrement lors de l'import du tag")
        vitesse = ("ERROR")
    return vitesse

def reference_ligne_actuelle(machine_prod, datedebut, datefin,yyear) :            
    if machine_prod == 'V1' :
        st.info("Tag référence de v1 inconnu pour le moment")
    elif machine_prod == 'V2' :
        tag_reference = 'V2.LF.LV2.recette'
    elif machine_prod == 'V3' :
        tag_reference = 'V3.LF.V32.Recette.pv'
    elif machine_prod == 'T4' :
        tag_reference = 'T4.LF.LF.vitM6.pv'
    elif machine_prod == 'FT1' :
        st.info("FT1 pas encore géré")
    else :
        st.warning("Veuillez choisir une ligne correcte")
        
    try :
        rows_reference = session.execute("""SELECT value
                                    FROM aethertimeseries.datapointsyear 
                                    WHERE name = $$"""+tag_reference+"""$$ 
                                    AND year = """+yyear+"""

                                    AND timestamp >= $$"""+str(datedebut)+"""$$
                                    AND timestamp <= $$"""+str(datefin)+"""$$

                                    ORDER BY timestamp DESC
                                    LIMIT 1;
                               """)
        
        for row in rows_reference :
            list_reference = [row.value]

        reference = round(float(list_reference[0]),1)
    except :
        st.warning("Il y a eu un problème - sûrement lors de l'import du tag")
        reference = ("ERROR")
    return reference

def creation_dataframes(list_bonne_marche, path_part_1) :
    list_return = []
    try :
        path_final = path_part_1 + list_bonne_marche[0]
        df1 = pd.read_csv(path_final, encoding = 'latin1', sep= ";")
        list_return.append(df1)
    except :
        pass
    try :
        path_final = path_part_1 + list_bonne_marche[1]
        df2 = pd.read_csv(path_final, encoding = 'latin1', sep= ";")
        list_return.append(df2)
    except :
        pass
    try :
        path_final = path_part_1 + list_bonne_marche[2]
        df3 = pd.read_csv(path_final, encoding = 'latin1', sep= ";")
        list_return.append(df3)
    except :
        pass
    return list_return

def fusion_dataframes(list_of_dataframe) :
    combien = len(list_of_dataframe)
    
    if combien == 1 :
        df_tmp_1 = list_of_dataframe[0]
        return df_tmp_1
    
    elif combien == 2 :
        df_tmp_1 = list_of_dataframe[0]
        df_tmp_2 = list_of_dataframe[1]
        
        list_tags = df_tmp_1.pop('tag_name')
        list_tags = df_tmp_2.pop('tag_name')
        
        df_added = (df_tmp_1.add(df_tmp_2)) / 2
        df_added['tag_name'] = list_tags
        return df_added
    
    elif combien == 3 :
        df_tmp_1 = list_of_dataframe[0]
        df_tmp_2 = list_of_dataframe[1]
        df_tmp_3 = list_of_dataframe[2]
        
        list_tags = df_tmp_1.pop('tag_name')
        list_tags = df_tmp_2.pop('tag_name')
        list_tags = df_tmp_3.pop('tag_name')
        
        df_added = (df_tmp_1.add(df_tmp_2).add(df_tmp_3)) / 3
        df_added['tag_name'] = list_tags
        return df_added
    
###########################################################################

                                              #-- Rouge Vert: --#
def main_rv() :
    
    now1 = datetime.now() - relativedelta(minutes = +3)
    
    #------------------------------------Sidebar------------------------------------#
    
    st.sidebar.title("Rouge vert :")
    st.sidebar.markdown("* * *")
    try :
        if reference_prod == '4F53 -V3' :
            st.sidebar.markdown("## Bonnes marches du 4F53 -V3")
    except :
        st.sidebar.info("Selectionnez une ligne de production et une référence")
    
    #-------------------------------------------------------------------------------#
    #------------------------------------Filtres------------------------------------#
    
    st.info("Bienvenue sur l'outil Big Data __Rouge_Vert__")
    st.success("Commençons par filtrer")
    st.title("Filtrage")
    my_bar1 = st.progress(2)
    
    choix1 = ["Faire un choix"]
    temp = os.listdir("/share-srvcassandra/Rouge_Vert_Data/")
    for i in range (len(temp)) :
        choix1.append(temp[i])
        
    machine_prod = st.selectbox("Selection de la ligne de production",choix1)
    
    if machine_prod == 'Faire un choix' :
        st.warning("Selectionnez une ligne de production")
    else : 
        for j in range (0,25):
            my_bar1.progress(j)
            time.sleep(0.01)
        my_bar1.progress(25)
        choix2 = ["Faire un choix"]
        path = "/share-srvcassandra/Rouge_Vert_Data/" + str(machine_prod)
        temp = os.listdir(path)
        for i in range (len(temp)) :
            choix2.append(temp[i])
            
        reference_prod = st.selectbox("Selection de la référence prod", choix2)
        
        if reference_prod == 'Faire un choix' :
            st.warning("Selectionnez une référence à étudier")
        else :
             
            st.success("GO ?")
            
            datedebut = (now1 - relativedelta(minutes = +21)).strftime("%Y-%m-%d %H:%M")
            datefin = now1.strftime("%Y-%m-%d %H:%M")
            yyear = str(now1.year)

            for j in range (25,50):
                my_bar1.progress(j)
                time.sleep(0.05)
            my_bar1.progress(50)
            
            st.title("Go :")

            # Checkpoint
            etapesuivante = 0
            if st.button("go") :
                etapesuivante = 1              
            if etapesuivante == 1 :
                my_bar1.progress(100)

                dataz = {'name': [], 'timestamp': [], 'value': []}
                df_bonne_periode_final = pd.DataFrame(dataz)
                my_bar_bonne_periode = st.progress(0)
                for i in range (len(taglist2)): #len(taglist)

                    # Progression de la barre
                    prog = 100*i/(len(taglist2)-1)
                    my_bar_bonne_periode.progress(int(prog))

                    #requête CQL. Tri sur la date
                    rows_bonne_periode = session.execute("""SELECT name, timestamp, value
                                                FROM aethertimeseries.datapointsyear 
                                                WHERE name = $$"""+taglist2[i]+"""$$ 
                                                AND year = """+yyear+"""

                                                AND timestamp >= $$"""+str(datedebut)+"""$$
                                                AND timestamp <= $$"""+str(datefin)+"""$$

                                                ORDER BY timestamp DESC
                                                LIMIT 1;

                                           """)
                    k=0
                    # Cassandra
                    for row in rows_bonne_periode :
                        list_bonne_periode=[row.name, row.timestamp, row.value]

                        data_bonne_periode = {'name': [list_bonne_periode[0]], 
                                              'timestamp': [list_bonne_periode[1]], 
                                              'value': [list_bonne_periode[2]]}
                        df_sub_tag = pd.DataFrame(data_bonne_periode)
                        df_bonne_periode_final = pd.concat([df_bonne_periode_final, df_sub_tag],
                                                           axis = 0,
                                                           ignore_index=True)
                    
                st.write("La vitesse actuelle de ", machine_prod, " est",
                         vitesse_ligne_actuelle(machine_prod, datedebut, datefin,yyear))
                
                path_part_1 = "/share-srvcassandra/Rouge_Vert_Data/" + str(machine_prod) + "/" + str(reference_prod) + "/"

                st.dataframe(df_bonne_periode_final)
                

    
    #-------------------------------------------------------------------------------#
    
###########################################################################

                                              #-- Généalogie: --#    

def main_gene() :
    
    #------------------------------------Sidebar------------------------------------#
    
    st.sidebar.markdown("# Product filters :")

    st.sidebar.markdown("""> ## <center>(1) ID Produit direct</center>
    """, unsafe_allow_html = True)
    id_produit1 = st.sidebar.text_input("Id produit :", value = '00000000')
    
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
   
    rows_history = session.execute("""SELECT id_rouleau, genealogie, type_produit FROM ods.genealogie
                                      WHERE id_produit = $$"""+id_produit+"""$$;
                                      """)
    list_history = []
    for row in rows_history :
        for i in range (3) :
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

        df_family_final = df_family_final.sort_values(['genealogie', 'niveau'])        
        df_family_final = df_family_final.drop(['genealogie','id_fardeau','num_bl',
                                                'num_cmd_sap','id_rouleau', 'date_depart_client'],axis=1)
        df_family_final = df_family_final.drop_duplicates(subset = ['id_produit'])
        
    else :
        pass

    # Affichage df(s)
    if list_history[2] == 'Bobine' or list_history[2] == 'Fardeau' :

        st.markdown("### Product history")
        st.dataframe(df_gen_final)
    
    st.write(list_history)
    st.markdown("### Product's family (Everything from the same roll)")
    st.dataframe(df_family_final)
    
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
                st.dataframe(infos_g)
            elif radio_rlx == 'Caracteristiques' :
                st.dataframe(carac_rlx)

            
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
            try :

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
                st.markdown("* * *")
                st.markdown("### Mesures faites sur ce rouleau")
                radio_mesure = st.radio('Quelles infos afficher ?', ['Tout','Hors Spec'])
                if radio_mesure == 'Tout' :
                    st.dataframe(df_mesure_final)
                elif radio_mesure == 'Hors Spec' :
                    st.dataframe(df_mesure_final_horsspec)
                    
            except :
                
                st.markdown("* * *")
                st.markdown("### Mesures faites sur ce rouleau")                
                st.info("Pas de mesures pour ce rouleaux")
                
            st.markdown("* * *")
            st.markdown("### Rapport OCS du rouleau (V3)")
            df_ocs_final = pd.DataFrame()
            
            rows_ocs = session.execute("""SELECT * FROM ods.ocs 
                                          WHERE id_rouleaux = $$"""+id_produit+"""$$;
                                          """)
            for row in rows_ocs :
                list_ocs = []
                for i in range(len(row)) :            
                    list_ocs.append(row[i])
                    
                df_ocs = pd.DataFrame([list_ocs], columns = ['machine_prod','reference_prod','horodate_fin',
                                                                'id_rouleaux','chemin_data','enrouleuse',
                                                                'grd_chromosome_abs','grd_chromosome_ppm',
                                                                'petit_chromosome_abs','petit_chromosome_ppm',
                                                                'pqp','statut_blocage','surface'])
                df_ocs_final = df_ocs_final.append(df_ocs)
                st.dataframe(df_ocs_final)
                
                st.write('< Nombre de grand chromosome :', df_ocs_final['grd_chromosome_abs'][0], '> &nbsp;&nbsp;&nbsp;&nbsp < Nombre de petit chromosome :', df_ocs_final['petit_chromosome_abs'][0], '>')
            
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
        
        df_fdx_final = pd.DataFrame()
        
        rows_fdx = session.execute("""SELECT * FROM ods.fdx_fardeaux
                                      WHERE id_fardeaux = $$"""+id_produit+"""$$;
                                   """)

        for row in rows_fdx :   
            list_fdx = []
            for i in range(len(row)) :            
                list_fdx.append(row[i])    

            df_fdx = pd.DataFrame([list_fdx], columns = ['id_fardeau','code_prod','date_creation',
                                                         'hauteur','largeur','lieux_stockage',
                                                         'login','poids_brut','poids_net','pqp',
                                                         'statut_commercial','unit_origine'])
            df_fdx_final = df_fdx_final.append(df_fdx)

        st.write("__Date entre la création du fardeaux et son départ chez le client : __")
        st.write(getDuration(list_fdx[2], df_gen_final.iloc[0][2]))
        
        st.write("__Infos générales__")
        st.write(df_fdx_final) 
        
        st.info("More coming")
    
        #----------------------------#
        
        
        
    elif choix_principal == 'Commandes' : 
        #------------Commandes-----------#    
        st.title("""Commandes / _Commands_ """)
        
        st.info("Coming soon")

        #----------------------------#
        
    else : 
        st.warning("Bug dans la matrice, veuillez selectionner un produit pour le rapport !")
        
    #-------------------------------------------------------------------------------#
    
###########################################################################

                                              #-- Dérogations : --#    

def main_derogations() :
    
    my_bar1 = st.progress(1)
    
    testest = pd.ExcelFile("/share-srvcassandra/Divers/Derogations 3.xlsx")
    testestdf = testest.parse('Feuil1', skiprows=2, index_col=None, na_values=['NA'],
                              usecols=(5,6,8,9,10,11,12,13,14,15,16,17,18,19,20,21))
    my_bar1.progress(25)
    
    testestdf2 = testest.parse('Feuil1', skiprows=2, index_col=None, na_values=['NA'], usecols=(5,6))
    testestdf2 = testestdf2.dropna()
    my_bar1.progress(50)
    df1=pd.merge(testestdf,testestdf2, how='inner', right_index=True, left_index=True)
    df1 = df1.drop(['Code Prod_x','Mois','Date de la décision','Année','Code Prod_y','Ref prod_y'],axis=1)
    df1 = df1.reset_index(drop=True)
    
    st.write("__Voilà la lsite de toutes les dérogations__ ; Il y en a ", len(df1))
    
    my_bar1.progress(75)
    st.dataframe(df1)
    my_bar1.progress(100)
    #------------------------------------Sidebar------------------------------------#
    
    st.sidebar.title("Dérogations :")
    st.sidebar.markdown("* * *")
    st.sidebar.markdown("""
    
    Retrouvez vos dérrogations préférés en __4 étapes__ !
    
    > 1. Choix de la ligne
    > 2. Choix de la référence
    > 3. Choix du type de déro
    > 4. Choix du motif
    
    """)
    
    my_sidebar2 = st.sidebar.progress(1)
    my_sidebar3 = st.sidebar.progress(1)
    my_sidebar4 = st.sidebar.progress(1)
    my_sidebar5 = st.sidebar.progress(1)
    
    #-------------------------------------------------------------------------------#
    
    
    
    dftemp = df1['Ligne']
    dftemp = dftemp.drop_duplicates()
    dftemp = dftemp.sort_values()
    dftemp = dftemp.reset_index(drop=True)
    choix1 = ['Faire un choix']
    for i in range (len(dftemp)) :
        choix1.append(dftemp[i])
        
    machine_prod = st.selectbox("Selection de la ligne de production",choix1)
    
    if machine_prod == 'Faire un choix' :
        st.warning("Veuillez selectionner une ligne de production")
    else :
        my_bar2 = st.progress(25)
        my_sidebar2.progress(25)
        
        temp = (df1['Ligne'] == str(machine_prod))
        df2 = df1[temp]
        df2 = df2.drop(['Cloturé'],axis=1)
        time.sleep(0.33)
        my_bar2.progress(50)
        my_sidebar2.progress(50)

        st.write(machine_prod, "__possède__ ", len(df2), "__dérogations__. Continuez pour affiner la recherche.")
        
        time.sleep(0.33)
        my_bar2.progress(75)
        my_sidebar2.progress(75)
        
        time.sleep(0.33)
        my_bar2.progress(100)
        my_sidebar2.progress(100)
        
        st.dataframe(df2)
        
        dftemp = df2['Ref prod_x']
        dftemp = dftemp.drop_duplicates()
        dftemp = dftemp.sort_values()
        dftemp = dftemp.reset_index(drop=True)
        
        choix2 = ['Faire un choix']
        for i in range (len(dftemp)) :
            choix2.append(dftemp[i])

        reference_prod = st.selectbox("Selection de la référence de production", choix2)

        if reference_prod == 'Faire un choix' :
            st.warning("Veuillez choisir une référence")

        else :
            
            my_bar3 = st.progress(25)
            my_sidebar3.progress(25)
            
            temp = (df2['Ref prod_x'] == str(reference_prod))
            
            time.sleep(0.33)
            my_bar3.progress(50)
            my_sidebar3.progress(50)
            
            df3 = df2[temp]

            time.sleep(0.33)
            my_bar3.progress(75)
            my_sidebar3.progress(75)

            time.sleep(0.33)
            my_bar3.progress(100)
            my_sidebar3.progress(100)
            
            pourcent = round((len(df3) / len(df2)),3)*100
            st.write("La référence \'", reference_prod, "\' possède ", len(df3), "dérogations.")
            st.write("C'est ", pourcent, "% des dérogations de ", machine_prod, ".")
            st.dataframe(df3)

            dftemp = df3['Famille']
            dftemp = dftemp.drop_duplicates()
            dftemp = dftemp.reset_index(drop=True)
            choix3 = ['Faire un choix']
            for i in range (len(dftemp)) :
                choix3.append(dftemp[i])

            famille_dero = st.selectbox("Selectionnez la famille de dérogation :", choix3)

            if famille_dero == 'Faire un choix' :
                st.warning("Veuillez choisir une famille de dérogation")

            else :
                
                my_bar4 = st.progress(25)
                my_sidebar4.progress(25)
                
                temp = (df3['Famille'] == str(famille_dero))
                
                time.sleep(0.33)
                my_bar4.progress(50)
                my_sidebar4.progress(50)
        
                df4 = df3[temp]
                
                time.sleep(0.33)
                my_bar4.progress(75)
                my_sidebar4.progress(75)

                time.sleep(0.33)
                my_bar4.progress(100)
                my_sidebar4.progress(100)
                
                st.write("Il reste ", len(df4), "dérogations dans votre recherche. Encore une étape !")
                df4 = df4.drop(['Ref prod_x','Ligne','PQP init.'],axis=1)
                st.dataframe(df4)

                dftemp = df4['Motif']
                dftemp = dftemp.drop_duplicates()
                dftemp = dftemp.reset_index(drop=True)
                choix4 = ['Faire un choix']
                for i in range (len(dftemp)) :
                    choix4.append(dftemp[i])

                motif_dero = st.selectbox("Selectionnez le motif de la dérogation :", choix4)

                if motif_dero == 'Faire un choix' :
                    st.warning("Veuillez choisir un motif")

                else :
                    my_bar5 = st.progress(25)
                    my_sidebar5.progress(25)
                    
                    temp = (df4['Motif'] == str(motif_dero))
                    
                    time.sleep(0.33)
                    my_bar5.progress(50)
                    my_sidebar5.progress(50)
                    
                    df5 = df4[temp]
                    
                    time.sleep(0.33)
                    my_bar5.progress(75)
                    my_sidebar5.progress(75)

                    time.sleep(0.33)
                    my_bar5.progress(100)
                    my_sidebar5.progress(100)
                    st.table(df5)
                    
                    st.sidebar.success("Terminé !")
        
    
    
###########################################################################    
        
if __name__ == "__main__":
    
    st.sidebar.markdown("""
    > ### `Product Dashboards`
    > L'information rapide et facile d'accès !
    
    
    > _Fast and easy to access informations !_
    > * * *
    """)
    
    logo = "Logo TFE Product dashboard.png"
    st.image(logo, width = 700)
    st.markdown("* * *")
    password = st.text_input("Password", key="pwd", type="password")
    st.markdown("* * *")
    if password == "genealogie" :
        main_gene()
    
    if password == "rouge_vert" :
        main_rv()
        
    if password == "derogations" :
        main_derogations()
