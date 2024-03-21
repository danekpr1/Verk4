from sqlalchemy import create_engine
import pandas as pd
from prophet import Prophet
from matplotlib import pyplot as plt
import warnings

warnings.filterwarnings('ignore', category=FutureWarning)

# Upplýsingar um tengingu við gagnagrunn
server = 'neu-dev-rudb2024-01-rg.database.windows.net'
database = 'vgbi2024nem'
username = 'vgbiH9'
password = 'CDWWoKJ2fLYhQFb9YSbPxDSLjDyl7Y4j'
connection_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

# Búa til tengsl við gagnagrunninn
engine = create_engine(connection_str)

#  SQL fyrirspurnir til að sækja gögn
query = """
SELECT [date], SUM([unitsSold]) AS unitsSold
FROM [vgbi2024nem].[h9].[factSales]
GROUP BY [date]
ORDER BY [date]
"""

# Nota pandas til að hlaða niðurstöðum fyrirspurna í DataFrame
df = pd.read_sql_query(query, engine)

# Loka tengslum við gagnagrunninn
engine.dispose()

# Athuga tímabil gagnanna
print("Date range:", df['date'].min(), "to", df['date'].max())

# Breyta dálkaheitum fyrir samhæfni með Prophet
df.columns = ['ds', 'y']

# Upphafsstilla og þjálfa Prophet líkanið
model = Prophet(daily_seasonality=True)
model.fit(df)

# Búa til DataFrame fyrir spár um framtíðina (fyrir næstu 360 daga)
future = model.make_future_dataframe(periods=360)

# nota líkanið í að gera spár
forecast = model.predict(future)

# Setja á graf
fig = model.plot(forecast)
plt.title('Söluspá')
plt.xlabel('Dagsetning')
plt.ylabel('Fjöldi seldra vara')
plt.show()


