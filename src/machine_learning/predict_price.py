import pandas as pd
import numpy as np
import seaborn as sns
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
import warnings
warnings.filterwarnings('ignore')


#%%
df = pd.read_parquet('italy_housing_price_rent_clean.parquet.gzip')
df['posti auto'].value_counts()
df['posti auto'] = df['posti auto'].apply(lambda x: 0 if x == None else 1)
df['efficienza energetica'] = df['efficienza energetica'].str.replace('[^0-9\.]', '', regex=True)
df = df.loc[df['efficienza energetica'] != '']
df['efficienza energetica'] = df['efficienza energetica'].astype('float64')

#%%
descriptives = [['citta', 'quartiere']]
df = df[['prezzo', 'm2', 'stanze', 'bagni', 'riferimento e data annuncio',
         'posti auto', 'regione', 'efficienza energetica', 'accesso disabili', 'ascensore']]
df.isna().sum()
df = df.dropna()

#%%
X = df.drop(columns=['riferimento e data annuncio', 'prezzo'])
y = df['prezzo']

numerical_vars = ['m2', 'stanze', 'efficienza energetica']
categorical_vars = ['bagni', 'posti auto', 'regione', 'accesso disabili', 'ascensore']

for col in numerical_vars:
    X[col] = X[col].astype('float64')

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=12)

#%%
cat_steps = [('OHE', OneHotEncoder(drop='first', handle_unknown='ignore'))]
pipe_cat = Pipeline(cat_steps)

num_steps = [('scale', StandardScaler()) ]
pipe_num = Pipeline(num_steps)

#%%
ohe = OneHotEncoder(drop='first', handle_unknown='ignore')
transformed = ohe.fit_transform(X_train[['ascensore', 'bagni']].values.reshape(-1,1))

print(ohe.categories_)
z = transformed.toarray()


#%%
one_pipe = ColumnTransformer(transformers=[
    ('numbers', pipe_num, numerical_vars),
    ('categories', pipe_cat, categorical_vars)])

one_pipe.fit_transform(X_train)

x = one_pipe.fit_transform(X_train)

#%%
modeling = Pipeline([
    ('preprocessing', one_pipe),
    'model', LinearRegression()
])

modeling.fit(X_train, y_train)


#%%
steps = [('scale', StandardScaler()), ('model', LinearRegression())]
pipe = Pipeline(steps)
pipe.fit(X_train[['m2', 'efficienza energetica']], y_train)


#%%
X_train.dtypes






#%% input missing values
imp_mean = SimpleImputer(missing_values=np.nan, strategy='mean')
imp_mean.fit(df)
df = imp_mean.transform(df)

#%%
X['m2'] = X['m2'].astype('float64')


#%%
df['m2'].str.replace('', np.nan)
#%%
df.loc[df['m2'] == '']
#%%
