
import csv

csvfile = open('repaired_csv.csv', 'r')
myFile = open('fixed.csv', 'w')
# spamreader = csv.reader(csvfile)

ll = csvfile.read().splitlines()
for l in ll:
    if l != ',,,,,,,,,,,,,':
        myFile.write(l + "\n")
