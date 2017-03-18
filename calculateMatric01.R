library(RODBC)
library(NLP)
library(openNLP)
library("stringr")

findADI <- function(seq="{a}", word="a") {
  x <- strsplit(seq,"}")
  cur <- 0
  sum <- 0
  
  for(i in 1:length(x[[1]])) {
    
    if (length(grep(word, x[[1]][i]))>0) {
      cur <- cur + 1
    } else {
      sum <- sum + cur^2
      cur <- 0
    }
    if(i == length(x[[1]])) sum <- sum + cur^2
  }
  
  return(sum)
}

#-------------------------------------------

dbhandle <- odbcDriverConnect('driver={SQL Server};server=localhost;database=Enron;trusted_connection=true')

SqlCommand <- "select did, name from dbo.document2 order by did"
des <- sqlQuery(dbhandle, SqlCommand)

odbcClose(dbhandle)

fileName <- "C:/tmp/list_all_doc_flow.txt"
conn <- file(fileName,open="r")
linn <-readLines(conn)

num_thread <- length(linn)
num_word <- nrow(des)

ADI <- matrix(0,nrow=num_thread,ncol=num_word)

S <- c(rep(1,num_thread))
abs_X <- num_thread

for (i in 1:num_thread){
  tid <- i
  seq <- linn[i]
  print(sprintf("ADI @ %d out of %d", i, num_thread))
  #print(seq)
  
  if(seq == "{}") next
  
  S[i] <- str_count(seq,"\\}")
  
  words <- gsub("}", "", gsub("\\{", "", seq))
  words <- unique(as.numeric(strsplit(words, ",")[[1]]))
  
  for(j in 1:length(words)) {
    #print(sprintf("tid = %s. did = %s. ADI = %s", tid, words[j], findADI(seq, words[j])))
    ADI[tid,words[j]] <- findADI(seq, words[j])
  }
}

#--------------------------------------------

RDI <- matrix(0,nrow=num_thread,ncol=num_word)

for(i in 1:num_word) {
  print(sprintf("RDI @ %d out of %d", i, num_word))
  RDI[,i] <- ADI[,i]*2^((length(ADI[,i])-length(ADI[,i][ADI[,i]==0]))/abs_X)
}

#--------------------------------------------

TDI <- c(rep(0,num_word))

for(i in 1:num_word) {
  print(sprintf("TDI @ %d out of %d", i, num_word))
  sum <- 0
  for(j in 1:num_thread) {
    #print(sprintf("%d %f", j, ((RDI[j,i]^0.5)/(2*S[j]))))
    sum <- sum + (RDI[j,i]^0.5)/(2*S[j])
  }
  TDI[i] <- sum/abs_X
}

TDI2 <- data.frame(des$name, TDI)

close(conn)

#--------------------------------------------

write.csv(ADI, "C:/tmp/ADI.csv")
write.csv(RDI, "C:/tmp/RDI.csv")
write.csv(TDI2, "C:/tmp/TDI.csv")









