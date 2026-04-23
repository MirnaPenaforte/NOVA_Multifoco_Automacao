import pandas as pd
df_custo = pd.DataFrame({"Preço Custo": ["14.253700", "6.075275"]})
# This was my first fix:
df_custo['Preço Custo'] = (
    df_custo['Preço Custo']
    .astype(str)
    .str.strip()
    .apply(lambda x: x.replace('.', '').replace(',', '.') if ',' in x else x)
)
df_custo['Preço Custo'] = pd.to_numeric(df_custo['Preço Custo'], errors='coerce').fillna(0.0)
df_custo['Preço Custo'] = df_custo['Preço Custo'].apply(
    lambda x: f"{x:.2f}".replace('.', ',') if x > 0 else '0,001'
)
print(df_custo)

# Wait, what if pandas read_csv actually converts it to something else?
# Let's test with df_estoque_bruto
