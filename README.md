# DIA

The Datasets can be downloaded from here - https://www.imdb.com/interfaces/ . There are 7 TSV files on this page to download. Please see link for all dataset description

Clone the repository from github using https://github.com/ayo-nci/DIA.git or download the zip format of the repository from the same URL

Running Steps
1. Create a folder for the project on your local hdfs called 'imdb' and upload all seven files downloaded from IMBD
2. Extract the contents of the cloned git/zip file to a folder on your local file system called 'imdb'
3. Run this command inside the folder mentioned in (2) above to carry out the hadoop job. - */bin/bash /imdb/compile.sh imdb imdb/titleratings.tsv imdb/titlebasics.tsv imdb/titlecrew.tsv imdb/titleoutput imdb/combinedoutput imdb/namebasics.tsv imdb/getdirectorsnameoutput*

The file output gotten from the hadoop process is fed into Jupyter by converting it to a csv file. A copy can be found within this zip. 
