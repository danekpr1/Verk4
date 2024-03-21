from sqlalchemy import create_engine
import pandas as pd
from mlxtend.frequent_patterns import apriori, association_rules

pd.options.mode.chained_assignment = None

# Upplýsingar um tengingu við gagnagrunn
server = 'neu-dev-rudb2024-01-rg.database.windows.net'
database = 'vgbi2024nem'
username = 'vgbiH9'
password = 'CDWWoKJ2fLYhQFb9YSbPxDSLjDyl7Y4j'
connection_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

# Tengjast gagnagrunni
engine = create_engine(connection_str)

# SQL fyrirspurnir til að sækja gögn úr gagnagrun
sales_query = """
SELECT [id], [rowKey], [date], [idStore], [idProduct], [unitsSold], [receipt]
FROM [h9].[factSales]
ORDER BY [date]
"""
products_query = """
SELECT [id], [rowKey], [name], [category]
FROM [h9].[dimProduct]
"""

# Nota pandas til að hlaða niðurstöðum fyrirspurna í DataFrame
sales_df = pd.read_sql_query(sales_query, engine)
products_df = pd.read_sql_query(products_query, engine)

# Loka tengslum við gagnagrunninn
engine.dispose()

# Tryggja að dálkar 'idProduct' í 'sales_df' og 'rowKey' í 'products_df' séu báðir af gerðinni strengur
sales_df['idProduct'] = sales_df['idProduct'].astype(str)
products_df['rowKey'] = products_df['rowKey'].astype(str)

# Sameina sölugögn með upplýsingum um vörur eftir 'idProduct' og 'rowKey'
combined_df = pd.merge(sales_df, products_df, left_on='idProduct', right_on='rowKey')

# Undirbúa gögn fyrir körfugreiningu með því að snúa töflunni: raðir eru kvittanir, dálkar eru vörunöfn
basket = (combined_df.groupby(['receipt', 'name'])['unitsSold']
          .sum().unstack().reset_index().fillna(0)
          .set_index('receipt'))

# Breytum seldum einingum yfir á boolean -> satt ef selt, annars rangt
basket_sets = basket > 0

# Notum Apriori algrímið og veljum lægstan stuðning sem við viljum birta
frequent_itemsets = apriori(basket_sets, min_support=0.005, use_colnames=True)

# Setjum á reglur
rules = association_rules(frequent_itemsets, metric="lift", min_threshold=1)

# Birta niðurstöður
print(rules[['antecedents', 'consequents', 'support', 'confidence']])
