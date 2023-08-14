

# 1. Preparação.----------------------------------------------------------------------

# Primeiro instalar o pacote do cran basedosdados.

# Carregando os pacotes necessários.
library(basedosdados)
library(dplyr)
library(dbplyr)
library(tidyr)
library(writexl)


# Defina o seu projeto no Google Big Query.
set_billing_id("cnpj-base-de-dados")


# 2. Empresas, estabelecimentos, simples, sócios e dicionário.-----------------------

# Cada um dos nomes dará origem a um df.
emp <- paste0("br_me_cnpj.","empresas")
estab <- paste0("br_me_cnpj.","estabelecimentos")
simp <- paste0("br_me_cnpj.","simples")
soc <- paste0("br_me_cnpj.","socios")
dic <- paste0("br_me_cnpj.","dicionario")

# Exemplo 1.
df_simp <- bdplyr(simp) |> 
  bd_collect()

saveRDS(df_simp, "rds/simples.rds")


# Exemplo 2.
df_dic <- bdplyr(dic) |> 
  bd_collect()

saveRDS(df_dic, "rds/dicionario.rds")


# Exemplo 3.
df_estab <- bdplyr(estab) |> 
  select(cnpj, cnpj_basico, nome_fantasia, situacao_cadastral,
         sigla_uf, id_municipio_rf, situacao_especial) |> 
  filter(sigla_uf == "RJ", situacao_cadastral == "2") |> 
  bd_collect()

saveRDS(df_estab, "rds/estabelecimentos.rds")


# 3. Criando o df dos CNPJ que não são do Simples. ---------------------------------------------

cnpj_nao_simples <- simples |> 
  select(cnpj_basico, opcao_simples) |> 
  filter(opcao_simples == 0) |> 
  select(cnpj_basico)

saveRDS(cnpj_nao_simples, "rds/cnpj_nao_simples.rds")
write_xlsx(cnpj_nao_simples, "excel/cnpj_nao_simples.xlsx")
 

# 4. Fazendo o inner join, faxinando e salvando. --------------------------

# Relacionando municípios do RJ.
df_municip <- bdplyr("br_bd_diretorios_brasil.municipio") |>
  filter(sigla_uf == "RJ") |> 
  select(id_municipio_rf, nome) |> 
  bd_collect()

# Juntando tudo e salvando.
estab_rj_nao_simples <- estabelecimentos |> 
  inner_join(cnpj_nao_simples, by = "cnpj_basico") |> 
  select(cnpj_basico, nome_fantasia, id_municipio_rf) |> 
  inner_join(df_municip, by = "id_municipio_rf")

saveRDS(estab_rj_nao_simples, "rds/estab_rj_nao_simples.rds")


# 5. Finalmente calculando a quantidade de cnpj por município. ------------------------------------------------------------

quant_cnpj_por_municip <- estab_rj_nao_simples |> 
  distinct() |> 
  count(nome)

saveRDS(quant_cnpj_por_municip, "data-raw/rds/quant_cnpj_por_municip.rds")
write_xlsx(quant_cnpj_por_municip, "data-raw/excel/quant_cnpj_por_municip.xlsx")  
  
  

# 6. Fazendo a mesma coisa só que na base de dados. -----------------------

df_estab_02 <- bdplyr("br_me_cnpj.estabelecimentos") |> 
  select(cnpj, cnpj_basico, nome_fantasia, situacao_cadastral,
         sigla_uf, id_municipio_rf, situacao_especial) |> 
  filter(sigla_uf == "RJ", situacao_cadastral == "2") 

cnpj_nao_simples_02 <- bdplyr("br_me_cnpj.simples") |> 
  select(cnpj_basico, opcao_simples) |> 
  filter(opcao_simples == 0) |> 
  select(cnpj_basico)

df_municip_02 <- bdplyr("br_bd_diretorios_brasil.municipio") |>
  filter(sigla_uf == "RJ") |> 
  select(id_municipio_rf, nome)

estab_rj_nao_simples_02 <- df_estab_02 |> 
  inner_join(cnpj_nao_simples_02, by = "cnpj_basico") |> 
  select(cnpj_basico, nome_fantasia, id_municipio_rf) |> 
  inner_join(df_municip_02, by = "id_municipio_rf") |> 
  bd_collect()

saveRDS(estab_rj_nao_simples_02, "rds/estab_rj_nao_simples_02.rds")

# Será igual àquele que fiz fase por fase? 

# setequal(estab_rj_nao_simples, estab_rj_nao_simples_02)

# R. TRUE. Yes, yes, yes!!!
# Isto significa que posso consultar a base da dados do CNPJ da Receita Federal
# com enorme facilidade!!!
