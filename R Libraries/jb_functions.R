#=========================
# to format dataframe columns however you choose
# arguments include the dataframe you want to format along with a character vectore specifying the format type of each column
#=========================
jb_format <- function(df, vec) {
  
  new_df <- c()
  formats <- c('nothing' = function(x){x},
               'number0' = function(x){formattable::accounting(x, digits = 0)},
               'number1' = function(x){formattable::accounting(x, digits = 1)},
               'number2' = function(x){formattable::accounting(x, digits = 2)},
               'percent0' = function(x){formattable::percent(x, digits = 0)},
               'percent1' = function(x){formattable::percent(x, digits = 1)},
               'percent2' = function(x){formattable::percent(x, digits = 2)},
               'money' = function(x){formattable::currency(x)})
  
  for (i in 1:ncol(df)) {
    col <- names(df)[i]
    fmt = formats[[vec[i]]]
    new_df[[col]] <- lapply(df[col], fmt)
  }

  new_df <- data.frame(new_df, stringsAsFactors = FALSE)
  rownames(new_df) <- rownames(df)
  
  return(new_df)
  
}

#=========================
# to make pretty tables for RPUbs
#=========================
options(knitr.table.format = "html")
library(knitr)
library(kableExtra)

jb_pretty_df <- function(df) {
  new_df <- df %>%
    kable() %>%
    kable_styling(bootstrap_options = c("striped", "hover", "condensed", "responsive")) %>%
    scroll_box(width = '100%')
  return(new_df)
}