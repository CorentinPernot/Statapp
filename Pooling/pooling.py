def Oracle(df1,df2, df_merge):
    "Pré pooling"
    
    L1 = list(df1['tx_id'].values) 
    L2 = list(df2['tx_id'].values)
    
    Liste = L1 + L2 #on concatène les transactions des 2 df 
    nombre_doublons_pre = len(Liste.unique())
    
    
    df_intermediaire = pd.DataFrame(np.isin(L1,L2).astype(int)*np.array(L1), columns=['Doublons'])
    Liste_doublons_pre=  df_intermediaire[df_intermediaire.Doublons >0]
    
    
    "Post pooling"
    
    tx_1_nb=len(df1['tx_id'].unique()) #nombre de transactions uniques dans le df1
    tx_2_nb=len(df2['tx_id'].unique()) #nombre de transactions uniques dans le df2 
    tx_merge_nb=len(df_merge['tx_id'].unique())  #nombre de transactions uniques dans le df_merge
    
    nombre_doublons_post = tx_1_nb + tx_2_nb - tx_merge_nb # nombre de doublons post pooling 
    
    #Liste_doublons_post = df_merge['tx_id'].
    
    print(" Nombre de doublons Pré pooling : " + str(nombre_doublons_pre))
    print(" Nombre de doublons Post pooling : " + str(nombre_doublons_post))
    
    " On va essayer d'identifier les transactions pour lesquelles le pooling n'a pas fonctionné" 
    
    
    return()

def levenshtein(seq1, seq2):
    size_x = len(seq1) + 1
    size_y = len(seq2) + 1
    matrix = np.zeros ((size_x, size_y))
    for x in range(size_x):
        matrix [x, 0] = x
    for y in range(size_y):
        matrix [0, y] = y

    for x in range(1, size_x):
        for y in range(1, size_y):
            if seq1[x-1] == seq2[y-1]:
                matrix [x,y] = min(
                    matrix[x-1, y] + 1,
                    matrix[x-1, y-1],
                    matrix[x, y-1] + 1
                )
            else:
                matrix [x,y] = min(
                    matrix[x-1,y] + 1,
                    matrix[x-1,y-1] + 1,
                    matrix[x,y-1] + 1
                )
    return (matrix[size_x - 1, size_y - 1])

def pivot(piv,df_A,df_B):
    L_df=[]
    L_A=df_A[piv].drop_duplicates()
    L_B=df_B[piv].drop_duplicates()
    L_piv=list(set(L_A) & set(L_B))


    for x in  (L_piv):
        df_A_piv=df_A[df_A[piv]==x]
        df_B_piv=df_B[df_B[piv]==x]
        L_df.append((df_A_piv,df_B_piv))
    
    return (L_df)

def get_string(dfA,dfB,pivot):
    dfA_bis=dfA.drop(pivot,axis=1)
    dfB_bis=dfB.drop(pivot,axis=1)
    n=len(dfA_bis.columns)
    dfA_bis["s"]=dfA_bis[dfA_bis.columns[0]]
    dfB_bis["s"]=dfB_bis[dfB_bis.columns[0]]

    for k in range  (1,n) :
        dfA_bis["s"]=dfA_bis["s"]+dfA_bis[dfA_bis.columns[k]]
        dfB_bis["s"]=dfB_bis["s"]+dfB_bis[dfB_bis.columns[k]]
    
    return(dfA_bis,dfB_bis)

#dfA et dfB sub dataframes qui ont la colonne s
def get_distance_levenstein(dfA,dfB,rowA,rowB):
    sA=dfA.at["s",rowA]
    sB=dfB.at["s",rowB]
    return(levenshtein(sA,sB))
#
