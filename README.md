# DIA

The Datasets can be downloaded from here - https://www.imdb.com/interfaces/

Create a folder for the project called 'imdb' and upload all seven files downloaded from IMBD

After download, please upload the files in TSV to your local hadoop and execute the following code in your terminal

~ /bin/bash <your local hdfs file path>/compile.sh imdb imdb/titleratings.tsv imdb/titlebasics.tsv imdb/titlecrew.tsv imdb/titleoutput imdb/combinedoutput imdb/namebasics.tsv imdb/getdirectorsnameoutput ~

The file output gotten from the hadoop process is fed into Jupyter by converting it to a csv file. A copy can be found within this zip. 
