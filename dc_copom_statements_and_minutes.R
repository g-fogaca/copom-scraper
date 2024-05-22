rm(list=ls())
# Importing packages
library(tidyverse)
library(jsonlite)
library(pdftools)
library(rvest)

t0 <- Sys.time()

# Data importing --------------------------------------------------------------

# Set how many minutes/statements to import
n_max <- 1000

# Import statements list
api_statements <- paste0(
  "https://www.bcb.gov.br/api/servico/sitebcb/copom/comunicados?quantidade=",
  n_max
  )

statements <- fromJSON(api_statements) %>% 
  list_rbind() %>% 
  arrange(nro_reuniao)

# Import statements details

statements_details <- list()

for (meeting in statements$nro_reuniao) {
  
  print(paste0(meeting, "/", max(statements$nro_reuniao)))
  
  url <- paste0(
    "https://www.bcb.gov.br/api/servico/sitebcb/copom/comunicados_detalhes?nro_reuniao=",
    meeting
  )
  
  details <- fromJSON(url) %>% 
    list_rbind()
  
  statements_details[[meeting]] <- details
  
}

statements_details <- statements_details %>% 
  list_rbind()

# Import minutes list
api_minutes <- paste0(
  "https://www.bcb.gov.br/api/servico/sitebcb/copom/atas?quantidade=",
  n_max
  )

minutes <- fromJSON(api_minutes) %>% 
  list_rbind() %>% 
  arrange(nroReuniao)

# Import minutes details

minutes_details <- list()

for (meeting in minutes$nroReuniao) {
  
  print(paste0(meeting, "/", max(minutes$nroReuniao)))
  
  url <- paste0(
    "https://www.bcb.gov.br/api/servico/sitebcb/copom/atas_detalhes?nro_reuniao=",
    meeting
  )
  
  details <- fromJSON(url) %>% 
    list_rbind()
  
  minutes_details[[meeting]] <- details
  
}

minutes_details <- minutes_details %>% 
  list_rbind()

t1 <- Sys.time()

print("Web Scraping:")
print(t1-t0)

# Join
df_raw <- bind_rows(
  minutes_details %>%
    rename(text = textoAta) %>% 
    mutate(type = "Minute"),
  statements_details %>%
    rename(
      text = textoComunicado,
      nroReuniao = nro_reuniao
      ) %>% 
    mutate(type = "Statement")
) %>% 
  relocate(type, nroReuniao, dataReferencia, dataPublicacao, titulo, text, urlPdfAta)

# Data preprocessing -------------------------------------------------------------

df_clean <- df_raw %>% 
  rename(meeting = nroReuniao) %>% 
  mutate(
    period = case_when(
      type == "Statement" ~ dataReferencia,
      type == "Minute" ~ dataPublicacao
    )
  ) %>% 
  select(-dataReferencia, -dataPublicacao)

# Solving NAs
read_pdf_url <- function(url) {
  tryCatch({
    text <- pdf_text(url)
    text <- paste(text, collapse = " ") 
    return(text)
  }, error = function(e) {
    return(NA)
  })
}

df_nas <- df_clean %>% 
  filter(is.na(text)) %>% 
  mutate(text = map_chr(urlPdfAta, read_pdf_url)) %>% 
  select(-urlPdfAta)

# Final data wrangling
converter_html_text <- function(chr) {
  tryCatch({
    text <- read_html(chr) %>%
      html_text(trim = TRUE)
    return(text)
  }, error = function(e) {
    return(NA)
  })
}

copom_str_replace <- function(text){
  text <- str_replace_all(text, "\\r?\\n", " ")
  text <- str_to_lower(text)
  text <- str_replace_all(text, "[[:punct:]]", " ")
  text <- str_replace_all(text, "[[:digit:]]", " ")
  text <- str_trim(text)
  text <- str_replace_all(text, "\\s+", " ")
  
  if(startsWith(text, "sumário")){
    text <- sub(".*?(a diretoria colegiada analisou)", "\\1", text, ignore.case = TRUE)
  } else {
    text <- text
  }
  
  return(text)
}

df_final <- df_clean %>% 
  drop_na(text) %>% 
  select(-urlPdfAta) %>% 
  mutate(text = map_chr(text, converter_html_text)) %>% 
  bind_rows(df_nas) %>% 
  mutate(text = map_chr(text, copom_str_replace)) %>% 
  select(-titulo) %>% 
  arrange(period) %>% 
  relocate(type, meeting, period, text)

t2 <- Sys.time()
print("Data preprocessing:")
print(t2-t1)

# Saving
write.table(df_final, "data/db_copom.txt", sep = "|", row.names = F, quote = F)
